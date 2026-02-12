import asyncio
import contextlib
import json
import logging
import os
import ssl
import uuid

import httpx
import socketio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from prometheus_client import Counter, Gauge, Histogram
from shared.metrics import install_metrics

from app.sockets.manager import RoomManager

SERVICE_NAME = "socket-hub"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")
GLOBAL_ROOM_PASSWORD = os.getenv("ROOM_PASSWORD", "")
DISPATCHER_URL = os.getenv("DISPATCHER_URL", "http://127.0.0.1:8001")
DISPATCH_TIMEOUT_SECONDS = float(os.getenv("DISPATCH_TIMEOUT_SECONDS", "180"))
DISPATCH_TIMEOUT_MIN_SECONDS = float(os.getenv("DISPATCH_TIMEOUT_MIN_SECONDS", "420"))
DISPATCH_CONNECT_TIMEOUT_SECONDS = float(
    os.getenv("DISPATCH_CONNECT_TIMEOUT_SECONDS", "10")
)
DISPATCH_WRITE_TIMEOUT_SECONDS = float(
    os.getenv("DISPATCH_WRITE_TIMEOUT_SECONDS", "30")
)
DISPATCH_POOL_TIMEOUT_SECONDS = float(os.getenv("DISPATCH_POOL_TIMEOUT_SECONDS", "30"))
DISPATCH_READ_TIMEOUT_SECONDS = max(
    DISPATCH_TIMEOUT_SECONDS, DISPATCH_TIMEOUT_MIN_SECONDS
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
USE_REDIS_ROOMS = os.getenv("SOCKETHUB_USE_REDIS", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
USE_KAFKA_PUBLISH = os.getenv(
    "SOCKETHUB_USE_KAFKA", os.getenv("ENABLE_KAFKA", "true")
).strip().lower() in {"1", "true", "yes", "on"}
USE_KAFKA_RESULTS_CONSUMER = os.getenv(
    "SOCKETHUB_USE_KAFKA_RESULTS", os.getenv("SOCKETHUB_USE_KAFKA", "true")
).strip().lower() in {"1", "true", "yes", "on"}
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "90000"))
KAFKA_RETRY_BACKOFF_MS = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "1000"))
KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", "8"))
KAFKA_CONNECTIONS_MAX_IDLE_MS = int(
    os.getenv("KAFKA_CONNECTIONS_MAX_IDLE_MS", "180000")
)
POSTS_TOPIC = os.getenv("POSTS_TOPIC", "posts.inbound")
RESULTS_TOPIC = os.getenv("RESULTS_TOPIC", "jobs.results")
KAFKA_RESULTS_GROUP = os.getenv("SOCKETHUB_RESULTS_GROUP", "socket-hub-results")
SOCKETHUB_RESULT_CALLBACK_TOKEN = os.getenv(
    "SOCKETHUB_RESULT_CALLBACK_TOKEN", ""
).strip()

logger = logging.getLogger(__name__)

socket_connections_current = Gauge(
    "socket_connections_current",
    "Current active websocket connections",
    multiprocess_mode="livesum",
)
socket_posts_received_total = Counter(
    "socket_posts_received_total",
    "Total posts received via Socket.IO",
)
socket_posts_completed_total = Counter(
    "socket_posts_completed_total",
    "Total posts completed via dispatcher/worker flow",
)
socket_posts_failed_total = Counter(
    "socket_posts_failed_total",
    "Total post processing failures",
)
socket_auth_errors_total = Counter(
    "socket_auth_errors_total",
    "Total room auth errors",
)
socket_dispatch_duration_seconds = Histogram(
    "socket_dispatch_duration_seconds",
    "Socket-hub dispatch roundtrip latency",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

app = FastAPI(title="Luxia Socket Hub", version=SERVICE_VERSION)
install_metrics(
    app, service_name=SERVICE_NAME, version=SERVICE_VERSION, env=SERVICE_ENV
)
logger.info(
    "[SocketHub] timeouts read=%.1fs(min=%.1fs) connect=%.1fs write=%.1fs pool=%.1fs",
    DISPATCH_READ_TIMEOUT_SECONDS,
    DISPATCH_TIMEOUT_MIN_SECONDS,
    DISPATCH_CONNECT_TIMEOUT_SECONDS,
    DISPATCH_WRITE_TIMEOUT_SECONDS,
    DISPATCH_POOL_TIMEOUT_SECONDS,
)
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path="socket.io")
_background_tasks: set[asyncio.Task] = set()
_room_manager: RoomManager | None = None
_kafka_producer: AIOKafkaProducer | None = None
_kafka_consumer: AIOKafkaConsumer | None = None
_kafka_results_task: asyncio.Task | None = None


def _kafka_producer_kwargs() -> dict:
    cfg: dict = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "request_timeout_ms": KAFKA_REQUEST_TIMEOUT_MS,
        "retry_backoff_ms": KAFKA_RETRY_BACKOFF_MS,
        "connections_max_idle_ms": KAFKA_CONNECTIONS_MAX_IDLE_MS,
    }
    if KAFKA_SECURITY_PROTOCOL.upper() == "SASL_SSL":
        cfg.update(
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_SASL_USERNAME,
                "sasl_plain_password": KAFKA_SASL_PASSWORD,
                "ssl_context": ssl.create_default_context(),
            }
        )
    return cfg


def _kafka_consumer_kwargs() -> dict:
    cfg = _kafka_producer_kwargs()
    cfg.update(
        {
            "group_id": KAFKA_RESULTS_GROUP,
            "enable_auto_commit": True,
            "auto_offset_reset": "latest",
        }
    )
    return cfg


def _is_dispatcher_callback_allowed(token: str | None) -> bool:
    if not SOCKETHUB_RESULT_CALLBACK_TOKEN:
        return True
    return bool(token) and token == SOCKETHUB_RESULT_CALLBACK_TOKEN


def _log_kafka_runtime_config() -> None:
    using_event_hubs = "servicebus.windows.net" in KAFKA_BOOTSTRAP.lower()
    logger.info(
        "[SocketHub][KafkaConfig] bootstrap=%s protocol=%s request_timeout_ms=%d retries=%d retry_backoff_ms=%d",
        KAFKA_BOOTSTRAP,
        KAFKA_SECURITY_PROTOCOL,
        KAFKA_REQUEST_TIMEOUT_MS,
        KAFKA_RETRIES,
        KAFKA_RETRY_BACKOFF_MS,
    )
    if using_event_hubs and KAFKA_SECURITY_PROTOCOL.upper() != "SASL_SSL":
        logger.warning(
            "[SocketHub][KafkaConfig] Event Hubs bootstrap detected but protocol is not SASL_SSL"
        )
    if using_event_hubs and ":9093" not in KAFKA_BOOTSTRAP:
        logger.warning(
            "[SocketHub][KafkaConfig] Event Hubs bootstrap should usually include :9093"
        )


def _track_background_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def _is_room_auth_valid(password: str) -> bool:
    if GLOBAL_ROOM_PASSWORD:
        return password == GLOBAL_ROOM_PASSWORD
    return True


async def _dispatch_and_emit_result(room_id: str, job_id: str, claim: str) -> None:
    try:
        with socket_dispatch_duration_seconds.time():
            timeout = httpx.Timeout(
                connect=DISPATCH_CONNECT_TIMEOUT_SECONDS,
                read=DISPATCH_READ_TIMEOUT_SECONDS,
                write=DISPATCH_WRITE_TIMEOUT_SECONDS,
                pool=DISPATCH_POOL_TIMEOUT_SECONDS,
            )
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    f"{DISPATCHER_URL}/dispatch/submit",
                    json={
                        "job_id": job_id,
                        "claim": claim,
                        "room_id": room_id,
                        "source": SERVICE_NAME,
                    },
                )
                response.raise_for_status()
                payload = response.json()

        result = payload.get("result", payload)
        final_status = str(result.get("status", "completed"))
        if _kafka_producer is not None:
            try:
                await _kafka_producer.send_and_wait(
                    RESULTS_TOPIC,
                    json.dumps(
                        {"job_id": job_id, "room_id": room_id, "status": final_status}
                    ).encode("utf-8"),
                )
            except Exception as kafka_exc:
                logger.warning("[SocketHub] Kafka result publish failed: %s", kafka_exc)
        if final_status == "completed":
            socket_posts_completed_total.inc()
        else:
            socket_posts_failed_total.inc()
        await sio.emit("worker_update", result, room=room_id)
    except Exception as exc:
        socket_posts_failed_total.inc()
        error_type = type(exc).__name__
        error_repr = repr(exc)
        await sio.emit(
            "worker_update",
            {
                "status": "error",
                "job_id": job_id,
                "claim": claim,
                "message": f"Dispatch pipeline failed ({error_type}): {error_repr}",
            },
            room=room_id,
        )


async def _consume_results_loop() -> None:
    if _kafka_consumer is None:
        logger.warning(
            "[SocketHub] Kafka results consumer loop started without consumer instance"
        )
        return
    logger.info(
        "[SocketHub] Kafka results consumer started topic=%s group=%s",
        RESULTS_TOPIC,
        KAFKA_RESULTS_GROUP,
    )
    async for msg in _kafka_consumer:
        try:
            payload = json.loads(msg.value.decode("utf-8"))
            room_id = str(payload.get("room_id") or "").strip()
            if not room_id:
                continue
            logger.info(
                "[SocketHub][Kafka] consumed ok topic=%s room_id=%s job_id=%s status=%s",
                RESULTS_TOPIC,
                room_id,
                str(payload.get("job_id") or ""),
                str(payload.get("status") or ""),
            )
            await sio.emit("worker_update", payload, room=room_id)
            logger.info(
                "[SocketHub][Kafka] result emitted ok room_id=%s job_id=%s status=%s",
                room_id,
                str(payload.get("job_id") or ""),
                str(payload.get("status") or ""),
            )
            status = str(payload.get("status", "")).lower()
            if status == "completed":
                socket_posts_completed_total.inc()
            elif status in {"error", "failed"}:
                socket_posts_failed_total.inc()
        except Exception as exc:
            logger.warning("[SocketHub] Kafka result consume failed: %s", exc)


@sio.event
async def connect(_sid, _environ, _auth=None):
    socket_connections_current.inc()


@sio.event
async def disconnect(_sid):
    socket_connections_current.dec()


@sio.event
async def join_room(sid, data):
    if not isinstance(data, dict):
        socket_auth_errors_total.inc()
        await sio.emit("auth_error", {"message": "Invalid join payload."}, room=sid)
        return

    room_id = str(data.get("room_id") or "").strip()
    password = str(data.get("password") or "")
    if not room_id:
        socket_auth_errors_total.inc()
        await sio.emit("auth_error", {"message": "room_id is required."}, room=sid)
        return
    if not _is_room_auth_valid(password):
        socket_auth_errors_total.inc()
        await sio.emit("auth_error", {"message": "Invalid room credentials."}, room=sid)
        return

    await sio.save_session(sid, {"room_id": room_id, "room_authenticated": True})
    await sio.enter_room(sid, room_id)
    if _room_manager is not None:
        try:
            await _room_manager.create_room(room_id)
        except Exception as redis_exc:
            logger.warning(
                "[SocketHub] Redis room create failed for %s: %s", room_id, redis_exc
            )
    await sio.emit("join_room_success", {"room_id": room_id}, room=sid)


@sio.event
async def post_message(sid, data):
    if not isinstance(data, dict):
        await sio.emit(
            "worker_update",
            {"status": "error", "message": "Invalid post payload."},
            room=sid,
        )
        return

    session = await sio.get_session(sid)
    room_id = str(data.get("room_id") or session.get("room_id") or "").strip()
    content = str(data.get("content") or "").strip()

    if not session.get("room_authenticated") or not room_id:
        socket_auth_errors_total.inc()
        await sio.emit(
            "auth_error", {"message": "You must join a room first."}, room=sid
        )
        return
    if not content:
        await sio.emit(
            "worker_update",
            {"status": "error", "message": "Claim content is required."},
            room=sid,
        )
        return

    socket_posts_received_total.inc()
    job_id = str(uuid.uuid4())
    if _room_manager is not None:
        try:
            await _room_manager.enqueue_post(
                room_id, {"job_id": job_id, "content": content}
            )
        except Exception as redis_exc:
            logger.warning(
                "[SocketHub] Redis enqueue failed for %s: %s", room_id, redis_exc
            )
    published_to_kafka = False
    if _kafka_producer is not None:
        try:
            await _kafka_producer.send_and_wait(
                POSTS_TOPIC,
                json.dumps(
                    {
                        "job_id": job_id,
                        "room_id": room_id,
                        "claim": content,
                        "source": SERVICE_NAME,
                    }
                ).encode("utf-8"),
            )
            published_to_kafka = True
            logger.info(
                "[SocketHub][Kafka] publish ok topic=%s room_id=%s job_id=%s",
                POSTS_TOPIC,
                room_id,
                job_id,
            )
        except Exception as kafka_exc:
            logger.warning("[SocketHub] Kafka post publish failed: %s", kafka_exc)
    await sio.emit(
        "worker_update",
        {"status": "processing", "job_id": job_id, "claim": content},
        room=room_id,
    )
    if not published_to_kafka:
        task = asyncio.create_task(
            _dispatch_and_emit_result(room_id=room_id, job_id=job_id, claim=content)
        )
        _track_background_task(task)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready", "service": SERVICE_NAME}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    socket_connections_current.inc()
    try:
        while True:
            message = await websocket.receive_text()
            await websocket.send_text(f"echo:{message}")
    except WebSocketDisconnect:
        pass
    finally:
        socket_connections_current.dec()


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "running"}


@app.post("/internal/dispatch-result")
async def internal_dispatch_result(
    payload: dict,
    x_dispatcher_token: str | None = Header(default=None),
) -> dict[str, str]:
    if not _is_dispatcher_callback_allowed(x_dispatcher_token):
        raise HTTPException(status_code=403, detail="forbidden")

    room_id = str(payload.get("room_id") or "").strip()
    if not room_id:
        return {"status": "ignored"}

    await sio.emit("worker_update", payload, room=room_id)
    logger.info(
        "[SocketHub][Fallback] result emitted ok room_id=%s job_id=%s status=%s",
        room_id,
        str(payload.get("job_id") or ""),
        str(payload.get("status") or ""),
    )
    status = str(payload.get("status", "")).lower()
    if status == "completed":
        socket_posts_completed_total.inc()
    elif status in {"error", "failed"}:
        socket_posts_failed_total.inc()
    return {"status": "ok"}


@app.on_event("startup")
async def startup_resources() -> None:
    global _room_manager, _kafka_producer, _kafka_consumer, _kafka_results_task
    _log_kafka_runtime_config()
    if USE_REDIS_ROOMS:
        try:
            _room_manager = RoomManager(redis_url=REDIS_URL)
            await _room_manager.connect()
            logger.info("[SocketHub] Redis room manager enabled")
        except Exception as exc:
            _room_manager = None
            logger.warning(
                "[SocketHub] Redis unavailable, continuing without room queue: %s", exc
            )
    if USE_KAFKA_PUBLISH:
        try:
            _kafka_producer = AIOKafkaProducer(**_kafka_producer_kwargs())
            await _kafka_producer.start()
            logger.info(
                "[SocketHub] Kafka producer enabled bootstrap=%s protocol=%s",
                KAFKA_BOOTSTRAP,
                KAFKA_SECURITY_PROTOCOL,
            )
        except Exception as exc:
            _kafka_producer = None
            logger.warning(
                "[SocketHub] Kafka unavailable, continuing without publish mirror: %s",
                exc,
            )
    if USE_KAFKA_RESULTS_CONSUMER:
        try:
            _kafka_consumer = AIOKafkaConsumer(
                RESULTS_TOPIC, **_kafka_consumer_kwargs()
            )
            await _kafka_consumer.start()
            _kafka_results_task = asyncio.create_task(_consume_results_loop())
        except Exception as exc:
            _kafka_consumer = None
            _kafka_results_task = None
            logger.warning("[SocketHub] Kafka results consumer unavailable: %s", exc)


@app.on_event("shutdown")
async def shutdown_resources() -> None:
    global _room_manager, _kafka_producer, _kafka_consumer, _kafka_results_task
    if _kafka_results_task is not None:
        _kafka_results_task.cancel()
        with contextlib.suppress(Exception):
            await _kafka_results_task
        _kafka_results_task = None
    if _kafka_consumer is not None:
        with contextlib.suppress(Exception):
            await _kafka_consumer.stop()
        _kafka_consumer = None
    if _kafka_producer is not None:
        with contextlib.suppress(Exception):
            await _kafka_producer.stop()
        _kafka_producer = None
    if _room_manager is not None:
        with contextlib.suppress(Exception):
            await _room_manager.disconnect()
        _room_manager = None

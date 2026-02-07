# socket-hub/app/main.py
import asyncio
import json
import os
import ssl
import uuid
from contextlib import asynccontextmanager

import socketio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore
from aiokafka.errors import KafkaConnectionError, KafkaError  # type: ignore
from fastapi import FastAPI

from app.api.routes import router
from app.sockets.manager import RoomManager


# Helper to mask sensitive info in Redis URLs
def mask_url(url: str) -> str:
    # Only mask if URL contains credentials
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" in rest:
        userinfo, host = rest.split("@", 1)
        # Mask password if present
        if ":" in userinfo:
            user, _ = userinfo.split(":", 1)
            masked = f"{user}:***"
        else:
            masked = "***"
        return f"{scheme}://{masked}@{host}"
    return url


# Kafka Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

POSTS_TOPIC = "posts.inbound"
RESULTS_TOPIC = "jobs.results"
KAFKA_AVAILABLE = False
REDIS_AVAILABLE = False

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
room_manager = RoomManager(redis_url=REDIS_URL)


def _get_kafka_config() -> dict:
    """Build Kafka client configuration with optional SASL/SSL for Azure Event Hubs."""
    config = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
    }

    # Azure Event Hubs requires SASL_SSL
    if KAFKA_SECURITY_PROTOCOL == "SASL_SSL":
        ssl_context = ssl.create_default_context()
        config.update(
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_SASL_USERNAME,
                "sasl_plain_password": KAFKA_SASL_PASSWORD,
                "ssl_context": ssl_context,
            }
        )

    return config


# Socket.IO server
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

producer: AIOKafkaProducer = None
consumer: AIOKafkaConsumer = None
redis_log_client = None  # For subscribing to worker logs via Redis pub/sub
_completed_forward_guard: dict[str, float] = {}
_COMPLETED_GUARD_TTL_SECONDS = 3600.0


def _gc_completed_forward_guard(now: float) -> None:
    expired = [
        job
        for job, ts in _completed_forward_guard.items()
        if now - ts > _COMPLETED_GUARD_TTL_SECONDS
    ]
    for job in expired:
        _completed_forward_guard.pop(job, None)


async def _init_kafka() -> tuple[AIOKafkaProducer | None, AIOKafkaConsumer | None]:
    try:
        kafka_config = _get_kafka_config()

        kafka_producer = AIOKafkaProducer(
            **kafka_config,
            linger_ms=2,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await kafka_producer.start()
        print(
            f"[Kafka] Producer started (bootstrap={KAFKA_BOOTSTRAP}, protocol={KAFKA_SECURITY_PROTOCOL})"
        )

        kafka_consumer = AIOKafkaConsumer(
            RESULTS_TOPIC,
            group_id="socket-hub-result-consumers",
            **kafka_config,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await kafka_consumer.start()
        print(f"[Kafka] Consumer started (topic={RESULTS_TOPIC})")

        return kafka_producer, kafka_consumer

    except Exception as e:
        print(f"[Kafka] Disabled due to startup failure: {e}")
        # best-effort cleanup if partially started
        try:
            if "kafka_producer" in locals() and kafka_producer is not None:
                await kafka_producer.stop()
        except Exception as e:
            print(f"[Kafka] Producer cleanup failed: {e}")
        try:
            if "kafka_consumer" in locals() and kafka_consumer is not None:
                await kafka_consumer.stop()
        except Exception as e:
            print(f"[Kafka] Consumer cleanup failed: {e}")
        return None, None


async def _shutdown_kafka(
    kafka_producer: AIOKafkaProducer | None, kafka_consumer: AIOKafkaConsumer | None
) -> None:
    """Shutdown Kafka producer and consumer gracefully."""
    try:
        if kafka_consumer is not None:
            await kafka_consumer.stop()
    except Exception as e:
        print(f"[Kafka] Consumer stop failed: {e}")

    try:
        if kafka_producer is not None:
            await kafka_producer.stop()
    except Exception as e:
        print(f"[Kafka] Producer stop failed: {e}")


# LIFESPAN EVENT: START/STOP
@asynccontextmanager
async def lifespan(_app: FastAPI):
    global producer, consumer, KAFKA_AVAILABLE, REDIS_AVAILABLE

    enable_kafka = os.getenv("ENABLE_KAFKA", "true").lower() == "true"
    task_results: asyncio.Task | None = None
    task_logs: asyncio.Task | None = None

    # ---- Redis connect (never crash app) ----
    try:
        await room_manager.connect()
        REDIS_AVAILABLE = True
        print(f"[Redis] Connected to {mask_url(REDIS_URL)}")
    except Exception as e:
        REDIS_AVAILABLE = False
        print(f"[Redis] Connection failed: {e}")

    # ---- Kafka init (best-effort) ----
    producer = None
    consumer = None
    KAFKA_AVAILABLE = False

    if enable_kafka:
        try:
            producer, consumer = await _init_kafka()
            KAFKA_AVAILABLE = producer is not None and consumer is not None

            if consumer is not None:
                task_results = asyncio.create_task(worker_results_listener())
        except Exception as e:
            # absolutely never crash startup due to Kafka
            KAFKA_AVAILABLE = False
            producer = None
            consumer = None
            print(f"[Kafka] Startup failed; continuing without Kafka: {e}")
    else:
        print("[Kafka] Disabled (ENABLE_KAFKA=false)")

    # ---- Redis log listener (best-effort) ----
    task_logs = asyncio.create_task(worker_logs_listener())

    try:
        yield
    finally:
        # cancel tasks
        for t in (task_results, task_logs):
            if t:
                t.cancel()

        # stop kafka gracefully
        await _shutdown_kafka(producer, consumer)
        KAFKA_AVAILABLE = False

        # disconnect redis
        try:
            await room_manager.disconnect()
        except Exception as e:
            print(f"[Redis] Disconnect failed: {e}")
        REDIS_AVAILABLE = False


# FastAPI app
app = FastAPI(title="Socket Hub Service", lifespan=lifespan)
app.include_router(router, prefix="/api")
asgi_app = socketio.ASGIApp(sio, app)


@app.get("/")
async def root():
    return {"message": "Socket Hub running"}


# SOCKET EVENTS
@sio.event
async def connect(sid, _):
    print(f"Client connected: {sid}")


@sio.event
async def disconnect(sid):
    print(f"Client disconnected: {sid}")


@sio.event
async def join_room(sid, data):
    room_id = data.get("room_id")
    await sio.save_session(sid, {"room_id": room_id})
    await sio.enter_room(sid, room_id)

    # Redis room tracking is optional - pipeline works via Kafka without it
    if REDIS_AVAILABLE:
        try:
            await room_manager.create_room(room_id)
        except Exception as e:
            print(f"[Redis] Room creation failed (non-fatal): {e}")

    print(f"[Socket] {sid} joined room {room_id}")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {
        "status": "ready",
        "redis": REDIS_AVAILABLE,
        "kafka": KAFKA_AVAILABLE,
        "kafka_bootstrap": KAFKA_BOOTSTRAP if KAFKA_AVAILABLE else None,
    }


# HTTP Fallback URL for direct worker calls (when Kafka is unavailable)
WORKER_HTTP_URL = os.getenv("WORKER_HTTP_URL", "http://localhost:8002")


async def _call_worker_http(payload: dict, room_id: str) -> None:
    """Call worker directly via HTTP and emit result to socket room."""
    import httpx

    try:
        print(f"[HTTP Fallback] Calling worker directly for post {payload['post_id']}")

        # Emit processing status
        await sio.emit(
            "worker_update",
            {
                "job_id": payload["post_id"],
                "post_id": payload["post_id"],
                "room_id": room_id,
                "status": "processing",
                "message": "Starting RAG pipeline (HTTP fallback)...",
            },
            room=room_id,
        )

        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            response = await client.post(
                f"{WORKER_HTTP_URL}/worker/verify",
                json={
                    "claim": payload["text"],
                    "post_id": payload["post_id"],
                    "room_id": room_id,
                    "domain": "general",
                },
            )

            if response.status_code == 200:
                result = response.json()
                # Emit the result to the socket room
                await sio.emit("worker_update", result, room=room_id)
                print(f"[HTTP Fallback] Result emitted to room {room_id}")
            else:
                print(f"[HTTP Fallback] Worker returned error: {response.status_code}")
                await sio.emit(
                    "worker_update",
                    {
                        "job_id": payload["post_id"],
                        "post_id": payload["post_id"],
                        "room_id": room_id,
                        "status": "error",
                        "error": f"Worker HTTP error: {response.status_code}",
                    },
                    room=room_id,
                )

    except Exception as e:
        print(f"[HTTP Fallback] Error: {e}")
        await sio.emit(
            "worker_update",
            {
                "job_id": payload["post_id"],
                "post_id": payload["post_id"],
                "room_id": room_id,
                "status": "error",
                "error": f"HTTP fallback failed: {e}",
            },
            room=room_id,
        )


# POST MESSAGE EVENT -> PUBLISH TO KAFKA -> DISPATCHER
@sio.event
async def post_message(sid, data):
    session = await sio.get_session(sid)
    room_id = data.get("room_id") or session.get("room_id")

    post_id = str(uuid.uuid4())
    payload = {
        "post_id": post_id,
        "room_id": room_id,
        "text": data.get("content"),
        "timestamp": data.get("timestamp"),
        "user_socket_id": sid,
    }

    # push into Redis queue (optional backup - pipeline works via Kafka)
    if REDIS_AVAILABLE:
        try:
            await room_manager.enqueue_post(room_id, payload)
        except Exception as e:
            print(f"[Redis] Enqueue failed (non-fatal): {e}")

    # publish into Kafka for dispatcher (this is the main pipeline)
    kafka_success = False
    if producer is not None:
        try:
            await producer.send_and_wait(POSTS_TOPIC, payload)
            print(f"[Kafka] Published post {post_id} to {POSTS_TOPIC}")
            kafka_success = True
        except (KafkaConnectionError, KafkaError, OSError) as e:
            print(f"[Kafka] Publish failed: {e}")
    else:
        print("[Kafka] Skipped publish (Kafka disabled)")

    # HTTP fallback: call worker directly if Kafka failed
    if not kafka_success and payload.get("text"):
        print(f"[Socket] Using HTTP fallback for post {post_id}")
        # Fire-and-forget HTTP call to worker (runs in background)
        _http_task = asyncio.create_task(_call_worker_http(payload, room_id))
        _http_task.add_done_callback(
            lambda t: t.exception() if t.done() and not t.cancelled() else None
        )

    print(f"[Socket] Post queued: {post_id}")


# KAFKA WORKER RESULTS -> EMIT TO SOCKET ROOMS
async def worker_results_listener():
    """
    This listens to worker outputs in Kafka and forwards them
    to the correct socket.io room.
    """

    if consumer is None:
        print("[SocketHub] Kafka consumer not available; results listener not started.")
        return

    print("[SocketHub] Worker results listener started...")

    async for msg in consumer:
        result = msg.value
        job_id = result.get("job_id")
        post_id = result.get("post_id")
        room_id = result.get("room_id")
        event_type = result.get("event_type")

        if not job_id:
            continue

        # Emit to the room the client originally joined
        target_room = room_id or post_id
        if target_room:
            now = asyncio.get_event_loop().time()
            _gc_completed_forward_guard(now)
            if event_type == "completed":
                if job_id in _completed_forward_guard:
                    print(
                        f"[SocketHub] Dropped duplicate completed event for job {job_id}"
                    )
                    continue
                _completed_forward_guard[job_id] = now
            await sio.emit("worker_update", result, room=target_room)
            print(
                f"[SocketHub] Emitted {event_type or 'legacy'} event for job {job_id} to room {target_room}"
            )
        else:
            print(f"[SocketHub] No room_id or post_id for job {job_id}, skipping emit")


# REDIS LOG SUBSCRIPTION -> EMIT TO SOCKET ROOMS
async def worker_logs_listener():
    """
    This subscribes to worker logs via Redis pub/sub and forwards them
    to the correct socket.io room.
    """
    global redis_log_client

    print(f"[SocketHub] Worker logs listener starting... (redis={mask_url(REDIS_URL)})")

    try:
        import redis.asyncio as aioredis

        # Azure Redis requires SSL - detect and configure accordingly
        redis_url = REDIS_URL
        ssl_enabled = redis_url.startswith("rediss://") or ":6380" in redis_url

        if ssl_enabled:
            # Azure Redis with SSL (port 6380)
            redis_log_client = aioredis.from_url(
                redis_url,
                ssl_cert_reqs="none",  # String "none" to disable cert verification
                socket_timeout=60.0,
                socket_connect_timeout=60.0,
                retry_on_timeout=True,
            )
        else:
            redis_log_client = aioredis.from_url(redis_url)

        # Test connection
        await redis_log_client.ping()

        pubsub = redis_log_client.pubsub()

        # Subscribe to the worker logs channel (published by worker's LogManager)
        await pubsub.subscribe("logs:all")

        print("[SocketHub] Worker logs listener ready, listening on 'logs:all' channel")

        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    log_data = json.loads(message["data"])

                    # Extract room_id or post_id from context
                    request_id = log_data.get("request_id")

                    # Emit to all connected clients (broadcast)
                    # or to specific room if request_id is available
                    event_data = {
                        "level": log_data.get("level", "INFO"),
                        "message": log_data.get("message", ""),
                        "timestamp": log_data.get("timestamp", ""),
                        "module": log_data.get("module", ""),
                        "request_id": request_id,
                    }

                    if request_id:
                        # Emit to specific room (post_id == request_id in most cases)
                        await sio.emit("worker_log", event_data, room=request_id)
                    else:
                        # Broadcast to all connected clients
                        await sio.emit("worker_log", event_data)

                    msg_preview = log_data.get("message", "")[:60]
                    print(
                        f"[SocketHub] Streamed log: [{log_data.get('level')}] {msg_preview}"
                    )

                except json.JSONDecodeError:
                    pass  # Skip malformed messages
                except Exception as e:
                    print(f"[SocketHub] Error processing log: {e}")

    except Exception as e:
        print(f"[SocketHub] Worker logs listener error: {e}")
    finally:
        if redis_log_client:
            await redis_log_client.close()

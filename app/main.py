import asyncio
import os
import uuid

import httpx
import socketio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from prometheus_client import Counter, Gauge, Histogram
from shared.metrics import install_metrics

SERVICE_NAME = "socket-hub"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")
GLOBAL_ROOM_PASSWORD = os.getenv("ROOM_PASSWORD", "")
DISPATCHER_URL = os.getenv("DISPATCHER_URL", "http://127.0.0.1:8001")
DISPATCH_TIMEOUT_SECONDS = float(os.getenv("DISPATCH_TIMEOUT_SECONDS", "90"))

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
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path="socket.io")
_background_tasks: set[asyncio.Task] = set()


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
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DISPATCH_TIMEOUT_SECONDS)
            ) as client:
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
        if final_status == "completed":
            socket_posts_completed_total.inc()
        else:
            socket_posts_failed_total.inc()
        await sio.emit("worker_update", result, room=room_id)
    except Exception as exc:
        socket_posts_failed_total.inc()
        await sio.emit(
            "worker_update",
            {
                "status": "error",
                "job_id": job_id,
                "claim": claim,
                "message": f"Dispatch pipeline failed: {exc}",
            },
            room=room_id,
        )


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
    await sio.emit(
        "worker_update",
        {"status": "processing", "job_id": job_id, "claim": content},
        room=room_id,
    )
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

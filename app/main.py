import asyncio
import os
import uuid

import socketio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from prometheus_client import Gauge
from shared.metrics import install_metrics

SERVICE_NAME = "socket-hub"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")
GLOBAL_ROOM_PASSWORD = os.getenv("ROOM_PASSWORD", "")

socket_connections_current = Gauge(
    "socket_connections_current",
    "Current active websocket connections",
    multiprocess_mode="livesum",
)

app = FastAPI(title="Luxia Socket Hub", version=SERVICE_VERSION)
install_metrics(
    app, service_name=SERVICE_NAME, version=SERVICE_VERSION, env=SERVICE_ENV
)
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app, socketio_path="socket.io")
_background_tasks: set[asyncio.Task] = set()


def _is_room_auth_valid(password: str) -> bool:
    if GLOBAL_ROOM_PASSWORD:
        return password == GLOBAL_ROOM_PASSWORD
    return True


async def _emit_fake_verdict(room_id: str, job_id: str, claim: str) -> None:
    await asyncio.sleep(1.2)
    await sio.emit(
        "worker_update",
        {
            "status": "completed",
            "job_id": job_id,
            "claim": claim,
            "verdict": "UNVERIFIABLE",
            "verdict_confidence": 0.62,
            "truthfulness_percent": 50.0,
            "verdict_rationale": "Socket.IO compatibility mode active. Pipeline integration can be reattached.",
            "evidence": [],
            "evidence_count": 0,
            "top_ranking_score": 0.0,
            "avg_ranking_score": 0.0,
            "trust_threshold": 0.7,
            "trust_threshold_met": False,
            "used_web_search": False,
            "data_source": "cache",
        },
        room=room_id,
    )


def _track_background_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@sio.event
async def connect(_sid, _environ, _auth=None):
    socket_connections_current.inc()


@sio.event
async def disconnect(_sid):
    socket_connections_current.dec()


@sio.event
async def join_room(sid, data):
    if not isinstance(data, dict):
        await sio.emit("auth_error", {"message": "Invalid join payload."}, room=sid)
        return

    room_id = str(data.get("room_id") or "").strip()
    password = str(data.get("password") or "")
    if not room_id:
        await sio.emit("auth_error", {"message": "room_id is required."}, room=sid)
        return
    if not _is_room_auth_valid(password):
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

    job_id = str(uuid.uuid4())
    await sio.emit(
        "worker_update",
        {"status": "processing", "job_id": job_id, "claim": content},
        room=room_id,
    )
    task = asyncio.create_task(
        _emit_fake_verdict(room_id=room_id, job_id=job_id, claim=content),
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

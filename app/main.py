import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from prometheus_client import Gauge
from shared.metrics import install_metrics

SERVICE_NAME = "socket-hub"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")

socket_connections_current = Gauge(
    "socket_connections_current",
    "Current active websocket connections",
)

app = FastAPI(title="Luxia Socket Hub", version=SERVICE_VERSION)
install_metrics(
    app, service_name=SERVICE_NAME, version=SERVICE_VERSION, env=SERVICE_ENV
)


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

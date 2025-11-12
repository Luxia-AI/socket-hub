from fastapi import FastAPI
import socketio

from app.api.routes import router as api_router

# Socket.IO server
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*"
)

# FastAPI app
app = FastAPI(title="Socket Hub Service")
app.include_router(api_router, prefix="/api")

# Mount socket.io
asgi_app = socketio.ASGIApp(sio, app)

@app.get("/")
async def root():
    return {"message": "Socket Hub running"}

# Example socket event
@sio.event
async def connect(sid, environ):
    print(f"Client connected: {sid}")

@sio.event
async def disconnect(sid):
    print(f"Client disconnected: {sid}")
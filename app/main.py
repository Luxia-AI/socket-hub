from contextlib import asynccontextmanager

from fastapi import FastAPI
import socketio

# Import custom modules
from app.api.routes import router
from app.sockets.manager import RoomManager

# Initialize RoomManager (connection happens in startup event)
room_manager = RoomManager()

# Socket.IO server
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await room_manager.connect()
    yield
    # Shutdown
    await room_manager.disconnect()


# FastAPI app
app = FastAPI(title="Socket Hub Service", lifespan=lifespan)
app.include_router(router, prefix="/api")

# Mount socket.io - this is the ASGI app that should be run
asgi_app = socketio.ASGIApp(sio, app)


@app.get("/")
async def root():
    return {"message": "Socket Hub running"}


# Socket event
@sio.event
async def connect(sid, environ):
    print(f"Client connected: {sid}")


@sio.event
async def disconnect(sid):
    print(f"Client disconnected: {sid}")


# Join room event
@sio.event
async def join_room(sid, data):
    """Client joins a room"""
    room_id = data.get("room_id")
    await sio.save_session(sid, {"room_id": room_id})
    sio.enter_room(sid, room_id)
    await room_manager.create_room(room_id)
    print(f"[Socket] {sid} joined room {room_id}")


# Post message event
@sio.event
async def post_message(sid, data):
    """Client posts a message to the room"""
    session = await sio.get_session(sid)
    room_id = session.get("room_id")
    post = {
        "sid": sid,
        "room_id": room_id,
        "content": data.get("content"),
        "timestamp": data.get("timestamp"),
    }
    if room_id and post:
        await room_manager.enqueue_post(room_id, post)
        await sio.emit("post_queued", {"room_id": room_id}, room=room_id)
        print(f"[Socket] {sid} posted message to room {room_id}")

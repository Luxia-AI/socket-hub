# socket-hub/app/main.py
import asyncio
import json
import uuid
from contextlib import asynccontextmanager

import socketio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore
from fastapi import FastAPI

from app.api.routes import router
from app.sockets.manager import RoomManager

KAFKA_BOOTSTRAP = "kafka:9092"
POSTS_TOPIC = "posts.inbound"
RESULTS_TOPIC = "jobs.results"

# Initialize RoomManager (Redis)
room_manager = RoomManager()

# Socket.IO server
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

producer: AIOKafkaProducer = None
consumer: AIOKafkaConsumer = None


# LIFESPAN EVENT: START/STOP
@asynccontextmanager
async def lifespan(_app: FastAPI):
    global producer, consumer

    # Redis connect
    await room_manager.connect()

    # Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        linger_ms=2,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    # Kafka consumer for worker results
    consumer = AIOKafkaConsumer(
        RESULTS_TOPIC,
        group_id="socket-hub-result-consumers",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()

    # async background listener
    task = asyncio.create_task(worker_results_listener())

    yield

    # shutdown
    task.cancel()
    await consumer.stop()
    await producer.stop()
    await room_manager.disconnect()


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
    sio.enter_room(sid, room_id)
    await room_manager.create_room(room_id)
    print(f"[Socket] {sid} joined room {room_id}")


# POST MESSAGE EVENT -> PUBLISH TO KAFKA -> DISPATCHER
@sio.event
async def post_message(sid, data):
    session = await sio.get_session(sid)
    room_id = session.get("room_id")

    post_id = str(uuid.uuid4())
    payload = {
        "post_id": post_id,
        "room_id": room_id,
        "text": data.get("content"),
        "timestamp": data.get("timestamp"),
        "user_socket_id": sid,
    }

    # push into Redis queue (optional for internal pipeline)
    await room_manager.enqueue_post(room_id, payload)

    # publish into Kafka for dispatcher
    await producer.send_and_wait(POSTS_TOPIC, payload)

    await sio.emit("post_queued", {"post_id": post_id}, room=room_id)
    print(f"[Socket] Published post {post_id} to Kafka")


# KAFKA WORKER RESULTS -> EMIT TO SOCKET ROOMS
async def worker_results_listener():
    """
    This listens to worker outputs in Kafka and forwards them
    to the correct socket.io room.
    """
    print("[SocketHub] Worker results listener started...")

    async for msg in consumer:
        result = msg.value
        post_id = result.get("post_id")

        if not post_id:
            continue

        # emit to socket room (room_id == post_id)
        await sio.emit("worker_update", result, room=post_id)

        print(f"[SocketHub] Emitted worker result for post {post_id}")

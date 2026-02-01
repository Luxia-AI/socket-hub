# socket-hub/app/main.py
import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

import socketio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore
from aiokafka.errors import KafkaConnectionError, KafkaError  # type: ignore
from fastapi import FastAPI

from app.api.routes import router
from app.sockets.manager import RoomManager

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTS_TOPIC = "posts.inbound"
RESULTS_TOPIC = "jobs.results"
KAFKA_AVAILABLE = False
REDIS_AVAILABLE = False

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
room_manager = RoomManager(redis_url=REDIS_URL)

# Socket.IO server
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

producer: AIOKafkaProducer = None
consumer: AIOKafkaConsumer = None
redis_log_client = None  # For subscribing to worker logs via Redis pub/sub


async def _init_kafka() -> tuple[AIOKafkaProducer | None, AIOKafkaConsumer | None]:
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            linger_ms=2,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await kafka_producer.start()
        print("[Kafka] Producer started")

        kafka_consumer = AIOKafkaConsumer(
            RESULTS_TOPIC,
            group_id="socket-hub-result-consumers",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await kafka_consumer.start()
        print("[Kafka] Consumer started")

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
        print("[Redis] Connected")
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
    await room_manager.create_room(room_id)
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

    # publish into Kafka for dispatcher (best-effort)
    if producer is not None:
        try:
            await producer.send_and_wait(POSTS_TOPIC, payload)
            print(f"[Kafka] Published post {post_id} to {POSTS_TOPIC}")
        except (KafkaConnectionError, KafkaError, OSError) as e:
            # degrade gracefully: payload still in Redis queue
            print(f"[Kafka] Publish failed; queued in Redis only: {e}")
    else:
        print("[Kafka] Skipped publish (Kafka disabled)")

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
        post_id = result.get("post_id")

        if not post_id:
            continue

        # emit to socket room (room_id == post_id)
        await sio.emit("worker_update", result, room=post_id)

        print(f"[SocketHub] Emitted worker result for post {post_id}")


# REDIS LOG SUBSCRIPTION -> EMIT TO SOCKET ROOMS
async def worker_logs_listener():
    """
    This subscribes to worker logs via Redis pub/sub and forwards them
    to the correct socket.io room.
    """
    global redis_log_client

    print("[SocketHub] Worker logs listener starting...")

    try:
        import redis.asyncio as aioredis

        redis_log_client = await aioredis.from_url(REDIS_URL)
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

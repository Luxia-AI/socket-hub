import json
from redis.asyncio import Redis, from_url
from typing import Any, Optional


class RoomManager:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        prefix: str = "room",
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self.redis: Optional[Redis[str]] = None

    async def connect(self):
        """Establish async connection to Redis"""
        self.redis = await from_url(self.redis_url, decode_responses=True)
        print("[Redis] Connected")

    async def disconnect(self):
        """Close connection to Redis"""
        if self.redis:
            await self.redis.close()
            print("[Redis] Disconnected")

    async def create_room(self, room_id: str) -> str:
        """Initialize a room queue"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        exists = await self.redis.exists(key)
        if not exists:
            await self.redis.lpush(key, json.dumps({"init": True}))
            print(f"[Redis] Room {room_id} initialized")
        return key

    async def enqueue_post(self, room_id: str, post: dict[str, Any]) -> None:
        """Add a post to the room queue"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        await self.redis.rpush(key, json.dumps(post))
        length = await self.redis.llen(key)
        print(f"[Redis] Added post to room {room_id} (queue length: {length})")

    async def get_next_post(self, room_id: str) -> Optional[dict[str, Any]]:
        """Retrieve (and remove) next post"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        data = await self.redis.lpop(key)
        return json.loads(data) if data else None

    async def get_queue_size(self, room_id: str) -> int:
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        return await self.redis.llen(key)

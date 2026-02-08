import json
import os
from typing import Any

from redis.asyncio import Redis, from_url


def _decode_queue_item(raw: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


class RoomManager:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        prefix: str = "room",
        max_queue_size: int | None = None,
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        if max_queue_size is None:
            max_queue_size = int(os.getenv("ROOM_QUEUE_MAXLEN", "100"))
        self.max_queue_size = max_queue_size
        self.redis: Redis | None = None

    async def connect(self) -> None:
        """Establish async connection to Redis"""
        # Azure Redis requires SSL (rediss:// or port 6380)
        ssl_enabled = (
            self.redis_url.startswith("rediss://") or ":6380" in self.redis_url
        )

        if ssl_enabled:
            # Azure Redis with SSL - use ssl_cert_reqs="none" (string) for rediss:// URLs
            # The rediss:// scheme automatically enables SSL
            self.redis = from_url(
                self.redis_url,
                decode_responses=True,
                ssl_cert_reqs="none",  # String "none" to disable cert verification
                socket_timeout=60.0,
                socket_connect_timeout=60.0,
                retry_on_timeout=True,
            )
        else:
            self.redis = from_url(self.redis_url, decode_responses=True)

        # Actually test the connection (from_url doesn't connect, it just creates client)
        await self.redis.ping()
        print(f"[Redis] Connected and verified (ssl={ssl_enabled})")

    async def disconnect(self) -> None:
        """Close connection to Redis"""
        if self.redis:
            await self.redis.close()
            print("[Redis] Disconnected")

    async def create_room(self, room_id: str) -> str:
        """Initialize a room queue"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        exists: int = await self.redis.exists(key)  # type: ignore[misc]
        if not exists:
            await self.redis.lpush(key, json.dumps({"init": True}))  # type: ignore[misc]
            print(f"[Redis] Room {room_id} initialized")
        return key

    async def enqueue_post(self, room_id: str, post: dict[str, Any]) -> None:
        """Add a post to the room queue"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        await self.redis.rpush(key, json.dumps(post))  # type: ignore[misc]
        if self.max_queue_size and self.max_queue_size > 0:
            await self.redis.ltrim(key, -self.max_queue_size, -1)  # type: ignore[misc]
        length: int = await self.redis.llen(key)  # type: ignore[misc]
        print(f"[Redis] Added post to room {room_id} (queue length: {length})")

    async def remove_post(self, room_id: str, post_id: str) -> bool:
        """Remove a post from the room queue by post_id."""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        items: list[str] = await self.redis.lrange(key, 0, -1)  # type: ignore[misc]

        for raw in items:
            parsed = _decode_queue_item(raw)
            if parsed and parsed.get("post_id") == post_id:
                removed: int = await self.redis.lrem(key, 1, raw)  # type: ignore[misc]
                return removed > 0

        return False

    async def get_next_post(self, room_id: str) -> dict[str, Any] | None:
        """Retrieve (and remove) next post"""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        data: str | None = await self.redis.lpop(key)  # type: ignore[misc]
        return json.loads(data) if data else None

    async def get_queue_size(self, room_id: str) -> int:
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.prefix}:{room_id}"
        return await self.redis.llen(key)  # type: ignore[misc]

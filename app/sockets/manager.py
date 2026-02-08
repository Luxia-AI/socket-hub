import json
import os
import secrets
from hashlib import pbkdf2_hmac
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
        self.auth_prefix = f"{prefix}_auth"
        if max_queue_size is None:
            max_queue_size = int(os.getenv("ROOM_QUEUE_MAXLEN", "100"))
        self.max_queue_size = max_queue_size
        self.password_hash_iterations = int(
            os.getenv("ROOM_PASSWORD_PBKDF2_ITERATIONS", "120000")
        )
        self.redis: Redis | None = None

    def _hash_password(self, password: str, salt_hex: str) -> str:
        password_bytes = password.encode("utf-8")
        salt = bytes.fromhex(salt_hex)
        digest = pbkdf2_hmac(
            "sha256", password_bytes, salt, self.password_hash_iterations
        )
        return digest.hex()

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

    async def register_room_credentials(
        self, room_id: str, password: str, overwrite: bool = False
    ) -> bool:
        """Register room credentials in Redis. Returns True when written."""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.auth_prefix}:{room_id}"
        exists: int = await self.redis.exists(key)  # type: ignore[misc]
        if exists and not overwrite:
            return False

        salt_hex = secrets.token_hex(16)
        hash_hex = self._hash_password(password, salt_hex)
        payload = json.dumps(
            {
                "room_id": room_id,
                "salt": salt_hex,
                "hash": hash_hex,
                "iterations": self.password_hash_iterations,
            }
        )
        await self.redis.set(key, payload)  # type: ignore[misc]
        return True

    async def verify_room_password(self, room_id: str, password: str) -> bool:
        """Validate room password against stored hash."""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.auth_prefix}:{room_id}"
        raw: str | None = await self.redis.get(key)  # type: ignore[misc]
        if not raw:
            return False
        parsed = _decode_queue_item(raw)
        if not parsed:
            return False
        salt_hex = str(parsed.get("salt", ""))
        expected_hash = str(parsed.get("hash", ""))
        if not salt_hex or not expected_hash:
            return False
        candidate_hash = self._hash_password(password, salt_hex)
        return secrets.compare_digest(expected_hash, candidate_hash)

    async def room_credentials_exist(self, room_id: str) -> bool:
        """Check if room credentials are provisioned."""
        assert self.redis is not None, "Redis not connected"  # nosec
        key = f"{self.auth_prefix}:{room_id}"
        exists: int = await self.redis.exists(key)  # type: ignore[misc]
        return bool(exists)

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

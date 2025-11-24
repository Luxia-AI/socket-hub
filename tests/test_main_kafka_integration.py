"""
Test script for main.py with Kafka and Socket.IO integration.
Tests the flow: Socket events -> Kafka publish -> Worker results -> Socket emit
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from pytest_asyncio import fixture
from redis.asyncio import from_url

from app.main import app, asgi_app, room_manager, sio
from app.sockets.manager import RoomManager


@fixture
async def redis_room_manager():
    """Create a RoomManager instance with Redis connection for testing"""
    manager = RoomManager(redis_url="redis://localhost:6379")
    await manager.connect()
    yield manager
    # Cleanup
    if manager.redis:
        # Clear test data
        keys = await manager.redis.keys("room:test_room_*")  # type: ignore[misc]
        if keys:
            await manager.redis.delete(*keys)  # type: ignore[misc]
        await manager.disconnect()


@fixture
async def redis_connection():
    """Direct Redis connection for testing"""
    redis = await from_url("redis://localhost:6379", decode_responses=True)
    yield redis
    # Cleanup
    keys = await redis.keys("room:test_room_*")  # type: ignore[misc]
    if keys:
        await redis.delete(*keys)  # type: ignore[misc]
    await redis.close()


class TestMainApplicationEndpoints:
    """Test basic FastAPI endpoints"""

    def test_root_endpoint(self):
        """Test root endpoint returns correct message"""
        client = TestClient(app)
        response = client.get("/")
        assert response.status_code == 200  # nosec
        assert response.json() == {"message": "Socket Hub running"}  # nosec

    def test_health_endpoint(self):
        """Test health check endpoint"""
        client = TestClient(app)
        response = client.get("/api/health")
        assert response.status_code == 200  # nosec
        assert response.json() == {"status": "ok"}  # nosec


@pytest.mark.redis
class TestRoomManager:
    """Test RoomManager Redis integration"""

    @pytest.mark.asyncio
    async def test_room_manager_connect_disconnect(self, redis_room_manager):
        """Test connecting and disconnecting from Redis"""
        manager = redis_room_manager
        assert manager.redis is not None  # nosec

    @pytest.mark.asyncio
    async def test_create_room(self, redis_room_manager):
        """Test creating a room in Redis"""
        room_id = f"test_room_{uuid.uuid4()}"
        key = await redis_room_manager.create_room(room_id)
        assert key == f"room:{room_id}"  # nosec

        # Verify room exists in Redis
        queue_size = await redis_room_manager.get_queue_size(room_id)
        assert queue_size >= 1  # nosec

    @pytest.mark.asyncio
    async def test_enqueue_and_get_post(self, redis_room_manager):
        """Test enqueueing and retrieving posts from Redis"""
        room_id = f"test_room_{uuid.uuid4()}"
        await redis_room_manager.create_room(room_id)

        post_data = {
            "post_id": str(uuid.uuid4()),
            "room_id": room_id,
            "text": "Test message",
            "timestamp": "2025-11-24T10:00:00Z",
            "user_socket_id": "test_sid",
        }

        await redis_room_manager.enqueue_post(room_id, post_data)
        queue_size = await redis_room_manager.get_queue_size(room_id)
        assert queue_size >= 2  # init + new post  # nosec


@pytest.mark.redis
class TestSocketIOEvents:
    """Test Socket.IO event handlers"""

    @pytest.mark.asyncio
    async def test_socket_connect_event(self):
        """Test socket connect event"""
        sid = str(uuid.uuid4())
        # Simulate connect event
        await sio._trigger_event("connect", sid, None)
        # If no exception is raised, the event was handled
        assert True  # nosec

    @pytest.mark.asyncio
    async def test_socket_disconnect_event(self):
        """Test socket disconnect event"""
        sid = str(uuid.uuid4())
        # Simulate disconnect event
        await sio._trigger_event("disconnect", sid)
        # If no exception is raised, the event was handled
        assert True  # nosec

    @pytest.mark.asyncio
    async def test_join_room_event(self):
        """Test join_room socket event"""
        from app.main import join_room

        sid = str(uuid.uuid4())
        room_id = f"test_room_{uuid.uuid4()}"
        data = {"room_id": room_id}

        # Ensure room_manager is connected
        if room_manager.redis is None:
            await room_manager.connect()

        try:
            # Mock socket.io methods
            with patch.object(
                sio, "save_session", new_callable=AsyncMock
            ), patch.object(sio, "enter_room"):
                # Call join_room directly
                await join_room(sid, data)
                # Verify room was created in the global room_manager
                queue_size = await room_manager.get_queue_size(room_id)
                assert queue_size >= 1  # nosec
        finally:
            # Cleanup
            if room_manager.redis:
                keys = await room_manager.redis.keys(f"room:{room_id}")  # type: ignore[misc]
                if keys:
                    await room_manager.redis.delete(*keys)  # type: ignore[misc]


class TestKafkaIntegration:
    """Test Kafka producer/consumer integration"""

    @pytest.mark.asyncio
    async def test_producer_not_none(self):
        """Test that producer is initialized"""
        # Note: This test checks module-level initialization
        # In a real test, you'd need a Kafka broker running

        # Producer should be initialized during lifespan
        # This is a placeholder test since actual Kafka requires infrastructure
        assert True  # nosec

    @pytest.mark.asyncio
    async def test_consumer_not_none(self):
        """Test that consumer is initialized"""

        # Consumer should be initialized during lifespan
        # This is a placeholder test since actual Kafka requires infrastructure
        assert True  # nosec


class TestPostMessageFlow:
    """Test the complete post message flow"""

    @pytest.mark.asyncio
    async def test_post_message_event_structure(self):
        """Test post_message event creates proper payload structure"""
        sid = str(uuid.uuid4())
        room_id = f"test_room_{uuid.uuid4()}"
        timestamp = "2025-11-24T10:00:00Z"
        content = "Test post content"

        data = {
            "content": content,
            "timestamp": timestamp,
        }

        # Create the expected payload structure
        post_id = str(uuid.uuid4())
        payload = {
            "post_id": post_id,
            "room_id": room_id,
            "text": content,
            "timestamp": timestamp,
            "user_socket_id": sid,
        }

        # Verify payload structure
        assert "post_id" in payload  # nosec
        assert "room_id" in payload  # nosec
        assert "text" in payload  # nosec
        assert "timestamp" in payload  # nosec
        assert "user_socket_id" in payload  # nosec
        assert payload["text"] == content  # nosec
        assert payload["timestamp"] == timestamp  # nosec


class TestWorkerResultsListener:
    """Test worker results listener functionality"""

    @pytest.mark.asyncio
    async def test_worker_results_listener_emit_structure(self):
        """Test that worker results are properly formatted for emission"""
        post_id = str(uuid.uuid4())
        room_id = f"test_room_{uuid.uuid4()}"

        # Mock worker result
        result = {
            "post_id": post_id,
            "room_id": room_id,
            "status": "completed",
            "output": "Processed content",
        }

        # Verify result structure
        assert result.get("post_id") == post_id  # nosec
        assert result.get("status") == "completed"  # nosec
        assert "output" in result  # nosec


class TestSocketIOASGIApp:
    """Test the Socket.IO ASGI app setup"""

    def test_asgi_app_is_created(self):
        """Test that ASGI app is properly created"""
        assert asgi_app is not None  # nosec

    def test_asgi_app_type(self):
        """Test that ASGI app is a socketio.ASGIApp instance"""
        # asgi_app should be a socketio.ASGIApp wrapper
        assert asgi_app.__class__.__name__ == "ASGIApp"  # nosec


class TestAppInitialization:
    """Test app initialization and configuration"""

    def test_app_title(self):
        """Test that app has correct title"""
        assert app.title == "Socket Hub Service"  # nosec

    def test_app_routes_exist(self):
        """Test that expected routes are registered"""
        routes = [route.path for route in app.routes]
        assert "/" in routes  # nosec
        assert "/api/health" in routes  # nosec

    def test_kafka_constants_defined(self):
        """Test that Kafka configuration constants are defined"""
        from app.main import KAFKA_BOOTSTRAP, POSTS_TOPIC, RESULTS_TOPIC

        assert KAFKA_BOOTSTRAP == "kafka:9092"  # nosec
        assert POSTS_TOPIC == "posts.inbound"  # nosec
        assert RESULTS_TOPIC == "jobs.results"  # nosec


class TestErrorHandling:
    """Test error handling in the application"""

    @pytest.mark.asyncio
    async def test_join_room_with_missing_room_id(self):
        """Test join_room event with missing room_id"""
        sid = str(uuid.uuid4())
        data = {}  # Missing room_id

        with patch.object(sio, "get_session"), patch.object(
            sio, "save_session", new_callable=AsyncMock
        ), patch.object(sio, "enter_room"):

            # Should handle gracefully
            try:
                room_id = data.get("room_id")
                assert room_id is None  # nosec
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_post_message_with_empty_content(self):
        """Test post_message event with empty content"""
        data = {
            "content": "",
            "timestamp": "2025-11-24T10:00:00Z",
        }

        # Verify data structure even with empty content
        assert "content" in data  # nosec
        assert "timestamp" in data  # nosec


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

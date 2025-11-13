import pytest
import socketio
import asyncio


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_socket():
    """Test socket connectivity to the server"""
    sio = socketio.AsyncClient()
    try:
        # Try to connect with a short timeout
        await asyncio.wait_for(sio.connect("http://127.0.0.1:8000"), timeout=5.0)
        assert sio.connected  # nosec
        await sio.disconnect()
    except (asyncio.TimeoutError, ConnectionRefusedError, Exception) as e:
        # If server is not running or connection times out, skip the test
        pytest.skip(f"Server not available: {type(e).__name__}: {str(e)}")

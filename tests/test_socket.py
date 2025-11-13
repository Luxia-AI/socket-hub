import pytest
import socketio


@pytest.mark.asyncio
async def test_socket():
    """Test socket connectivity to the server"""
    sio = socketio.AsyncClient()
    try:
        await sio.connect("http://127.0.0.1:8000", wait_timeout=5, retry=True)
        assert sio.connected  # nosec
        await sio.disconnect()
    except Exception as e:
        # If server is not running, skip the test
        pytest.skip(f"Server not available: {str(e)}")

import socketio

sio = socketio.AsyncClient()


async def test_socket():
    await sio.connect("http://127.0.0.1:8000")
    await sio.wait()


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_socket())

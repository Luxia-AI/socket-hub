from unittest.mock import AsyncMock, patch

import pytest

from app import main as main_module


@pytest.mark.asyncio
async def test_join_room_requires_password() -> None:
    with patch.object(
        main_module, "_verify_room_secret", new_callable=AsyncMock
    ) as verify_mock, patch.object(
        main_module, "_authorize_socket_action", new_callable=AsyncMock
    ) as authorize_mock, patch.object(
        main_module.sio, "emit", new_callable=AsyncMock
    ) as emit_mock:
        await main_module.join_room(
            "sid-1",
            {
                "room_id": "room-demo",
                "client_id": "client_demo",
                "access_token": "client-operator-token",
            },
        )

    verify_mock.assert_not_awaited()
    authorize_mock.assert_not_awaited()
    emit_mock.assert_awaited_once()
    args, kwargs = emit_mock.await_args
    assert args[0] == "auth_error"
    assert args[1]["code"] == "invalid_room_password"
    assert args[1]["message"] == "password is required."
    assert kwargs["room"] == "sid-1"


@pytest.mark.asyncio
async def test_post_message_rejected_before_successful_join() -> None:
    with patch.object(
        main_module.sio, "get_session", new_callable=AsyncMock, return_value={}
    ), patch.object(main_module.sio, "emit", new_callable=AsyncMock) as emit_mock:
        await main_module.post_message(
            "sid-1",
            {
                "room_id": "room-demo",
                "client_id": "client_demo",
                "claim": "test claim",
            },
        )

    emit_mock.assert_awaited_once()
    args, kwargs = emit_mock.await_args
    assert args[0] == "auth_error"
    assert args[1]["message"] == "You must join a room first."
    assert kwargs["room"] == "sid-1"

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app import main as main_module


class _RaisingClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        raise httpx.ReadTimeout("worker timed out")


@pytest.mark.asyncio
async def test_dispatch_error_message_contains_exception_type_and_repr():
    with patch(
        "app.main.httpx.AsyncClient", return_value=_RaisingClient()
    ), patch.object(
        main_module.sio,
        "emit",
        new_callable=AsyncMock,
    ) as emit_mock:
        await main_module._dispatch_and_emit_result(
            room_id="room-x",
            job_id="job-x",
            claim="Vaccines do not cause autism.",
        )

    args, _kwargs = emit_mock.await_args
    event_name = args[0]
    payload = args[1]

    assert event_name == "worker_update"
    assert payload["status"] == "error"
    assert "Dispatch pipeline failed (ReadTimeout)" in payload["message"]
    assert "ReadTimeout('worker timed out')" in payload["message"]

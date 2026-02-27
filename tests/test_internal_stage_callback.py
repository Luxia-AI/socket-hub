from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app


def test_internal_dispatch_stage_emits_worker_stage() -> None:
    payload = {
        "job_id": "job-1",
        "room_id": "room-a",
        "client_id": "client_demo",
        "client_claim_id": "claim-1",
        "stage": "search_done",
        "stage_payload": {"queries_total": 3},
    }

    with patch("app.main.sio.emit", new_callable=AsyncMock) as emit_mock, TestClient(
        app
    ) as client:
        response = client.post("/internal/dispatch-stage", json=payload)

    assert response.status_code == 200  # nosec
    assert response.json() == {"status": "ok"}  # nosec
    emit_mock.assert_awaited_once()
    args, kwargs = emit_mock.await_args
    assert args[0] == "worker_stage"
    assert args[1]["room_id"] == "room-a"
    assert args[1]["stage"] == "search_done"
    assert kwargs["room"] == "room-a"

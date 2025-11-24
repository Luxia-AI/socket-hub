from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root_endpoint():
    """Test the root endpoint returns correct response"""
    response = client.get("/")
    assert response.status_code == 200  # nosec
    assert response.json() == {"message": "Socket Hub running"}  # nosec


def test_health():
    response = client.get("/api/health")
    assert response.status_code == 200  # nosec
    assert response.json() == {"status": "ok"}  # nosec

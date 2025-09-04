from fastapi.testclient import TestClient
from api.app import app  # adjust if needed


def test_health_ok():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200

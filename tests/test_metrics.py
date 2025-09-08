from fastapi.testclient import TestClient
from api.app import app


def test_metrics_ok():
    c = TestClient(app)
    r = c.get("/metrics")
    assert r.status_code == 200

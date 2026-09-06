import pytest
from fastapi.testclient import TestClient
from main import app, state

@pytest.fixture
def client():
    # Disable lifespan background tasks during TestClient calls
    with TestClient(app) as c:
        yield c

def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "ok"
    assert "mode" in data
    assert "running" in data

def test_api_latest(client):
    response = client.get("/api/latest")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

def test_api_logs(client):
    response = client.get("/api/logs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_api_settings_get_and_post(client):
    get_res = client.get("/api/settings")
    assert get_res.status_code == 200
    cfg = get_res.json()
    assert "mode" in cfg
    assert "trading" in cfg

    post_res = client.post("/api/settings", json={"trading": {"risk_value": 10}})
    assert post_res.status_code == 200
    assert post_res.json().get("status") == "ok"

def test_start_and_stop_trading(client):
    start_res = client.post("/api/start")
    assert start_res.status_code == 200
    assert start_res.json().get("running") is True
    assert state["running"] is True

    stop_res = client.post("/api/stop")
    assert stop_res.status_code == 200
    assert stop_res.json().get("running") is False
    assert state["running"] is False

def test_history_and_files(client):
    hist_res = client.get("/history")
    assert hist_res.status_code == 200
    assert isinstance(hist_res.json(), list)

    files_res = client.get("/api/files")
    assert files_res.status_code == 200
    assert isinstance(files_res.json(), list)

def test_test_connection_endpoint(client):
    conn_res = client.post("/api/test-connection")
    assert conn_res.status_code == 200
    assert isinstance(conn_res.json(), dict)

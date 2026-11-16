import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from conftest import FakeClock, FakeTransport, availability, telemetry
from fastapi.testclient import TestClient

from smartlight.api import create_app
from smartlight.config import Settings
from smartlight.models import PREFIX
from smartlight.service import Service


@pytest.fixture
def api(tmp_path):
    transport, clock = FakeTransport(), FakeClock()
    app = create_app(Settings(database=tmp_path / "api.db"), transport, clock)
    with TestClient(app) as client:
        yield client, transport, clock, app


def feed(transport, kind, value):
    assert transport.sink(f"{PREFIX}/{value['node_id']}/{kind}", json.dumps(value).encode(), transport.session)


def activate(client, transport):
    feed(transport, "availability", availability())
    feed(transport, "telemetry", telemetry(uptime=2**63 + 1, seq=2**63 + 2))
    return client.get("/api/nodes").json()[0]


def test_endpoints_openapi_and_http_bigints(api):
    client, transport, _, _ = api
    assert client.get("/api/health").status_code == 200
    assert len(client.get("/api/nodes").json()) == 3
    node = activate(client, transport)
    assert node["connection"] == "ONLINE" and node["can_command"]
    assert node["telemetry"]["uptime_ms"] == str(2**63 + 1)
    assert node["last_seen"].endswith("Z")
    history = client.get("/api/nodes/node_01/history").json()
    assert history["total"] == 1 and history["items"][0]["telemetry"]["seq"] == str(2**63 + 2)
    assert client.get("/api/nodes/node_01/history?limit=1001").status_code == 422
    assert client.get("/api/nodes/unknown/history").status_code == 404
    assert client.get("/api/commands/unknown").status_code == 404
    assert client.get("/api/experiments/999/export.csv").status_code == 404
    assert client.get("/api/nodes/node_01/history?from=2023-11-14T22:00:00").status_code == 422
    assert client.get("/api/stats?node_id=unknown").status_code == 404
    assert client.get("/api/stats?from=1700000000").status_code == 422
    assert client.get("/docs").status_code == 200
    assert len(client.get("/openapi.json").json()["paths"]) == 11


def test_command_http_contract_and_concurrent_pending(api):
    client, transport, _, _ = api
    assert client.post("/api/nodes/node_01/commands", json={"mode": "AUTO"}).status_code == 409
    activate(client, transport)
    for body in [
        {"mode": "AUTO", "brightness": 50},
        {"mode": "MANUAL", "brightness": True},
        {"mode": "AUTO", "other": 1},
    ]:
        assert client.post("/api/nodes/node_01/commands", json=body).status_code == 422
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(
            pool.map(
                lambda _: client.post("/api/nodes/node_01/commands", json={"mode": "MANUAL", "brightness": 80}),
                range(2),
            )
        )
    assert sorted(r.status_code for r in responses) == [202, 409]
    accepted = next(r.json() for r in responses if r.status_code == 202)
    assert accepted["result"] == "PENDING" and type(accepted["payload"]["deadline_uptime_ms"]) is str
    assert len(transport.published) == 1
    packet = transport.published[0][1]
    assert type(packet["deadline_uptime_ms"]) is int
    feed(
        transport,
        "ack",
        dict(
            schema_version=1,
            node_id="node_01",
            boot_id="b7e28a10",
            command_id=packet["command_id"],
            result="APPLIED",
            reason=None,
        ),
    )
    assert client.get(f"/api/commands/{packet['command_id']}").json()["result"] == "APPLIED"


def test_experiments_stats_csv_cors_and_validation(api):
    client, transport, clock, _ = api
    result = client.post("/api/experiments", json={"name": "synthetic fixture", "configuration": {"source": "test"}})
    assert result.status_code == 201
    experiment = result.json()
    assert client.post("/api/experiments", json={"name": "second"}).status_code == 409
    feed(transport, "availability", availability())
    feed(transport, "telemetry", telemetry())
    clock.advance(2)
    feed(transport, "telemetry", telemetry(1, 2000))
    stopped = client.post(f"/api/experiments/{experiment['id']}/stop")
    assert stopped.status_code == 200
    assert client.post(f"/api/experiments/{experiment['id']}/stop").json() == stopped.json()
    stats = client.get(f"/api/stats?experiment_id={experiment['id']}&node_id=node_01").json()
    assert stats["total_energy_wh"] > 0
    exported = client.get(f"/api/experiments/{experiment['id']}/export.csv")
    assert exported.status_code == 200 and "excluded_reasons" in exported.text
    assert "text/csv" in exported.headers["content-type"]
    assert client.get("/api/alerts").status_code == 200
    assert client.post("/api/experiments", json={"name": " "}).status_code == 422
    allowed = client.get("/api/nodes", headers={"Origin": "http://localhost:5173"})
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert (
        "access-control-allow-origin"
        not in client.get("/api/nodes", headers={"Origin": "https://evil.example"}).headers
    )
    start = datetime.fromtimestamp(clock.utc(), timezone.utc).isoformat().replace("+00:00", "Z")
    assert client.get("/api/stats", params={"from": start, "to": start}).status_code == 422


def test_db_read_only_health_and_command_safety(api):
    client, transport, _, app = api
    activate(client, transport)
    # Change only the test connection, on its owning worker.
    asyncio.run(app.state.service.call("db.execute", "PRAGMA query_only=ON"))
    assert client.get("/api/health").status_code == 503
    assert client.post("/api/nodes/node_01/commands", json={"mode": "AUTO"}).status_code == 503
    assert not transport.published
    asyncio.run(app.state.service.call("db.execute", "PRAGMA query_only=OFF"))
    assert client.get("/api/health").status_code == 200


def test_broker_loss_and_restart_keep_history(tmp_path):
    transport, clock = FakeTransport(), FakeClock()
    settings = Settings(database=tmp_path / "restart.db")
    app = create_app(settings, transport, clock)
    with TestClient(app) as client:
        activate(client, transport)
        transport.connected = False
        assert client.get("/api/health").status_code == 503
        assert client.get("/api/nodes").json()[0]["freshness"] == "STALE"
    again = FakeTransport()
    with TestClient(create_app(settings, again, clock)) as client:
        assert client.get("/api/nodes").json()[0]["freshness"] == "STALE"
        assert client.get("/api/nodes/node_01/history").json()["total"] == 1
        assert client.post("/api/nodes/node_01/commands", json={"mode": "AUTO"}).status_code == 409


def test_callback_queue_is_bounded_without_database_access(tmp_path):
    service = Service(Settings(database=tmp_path / "never-opened.db", queue_size=1), FakeTransport(), FakeClock())
    assert service.ingest("test", b"{}", "s")
    assert not service.ingest("test", b"{}", "s")
    assert service.dropped == 1 and service.generation == 1
    assert not (tmp_path / "never-opened.db").exists()


def test_closed_database_returns_503_without_killing_worker(api):
    client, _transport, _clock, app = api
    asyncio.run(app.state.service.call("db.close"))
    assert client.get("/api/health").status_code == 503
    assert client.get("/api/nodes").status_code == 503
    assert app.state.service.thread.is_alive()


def test_experiment_list_detail_paging_and_continuity(api):
    client, transport, _, _ = api
    assert client.get("/api/experiments").json() == dict(items=[], total=0, limit=100, offset=0)
    first = client.post("/api/experiments", json={"name": "first"}).json()
    client.post(f"/api/experiments/{first['id']}/stop")
    second = client.post("/api/experiments", json={"name": "second"}).json()
    page = client.get("/api/experiments?limit=1").json()
    assert page["total"] == 2 and page["items"][0] == second
    assert client.get("/api/experiments?limit=1&offset=1").json()["items"] == [
        client.get(f"/api/experiments/{first['id']}").json()
    ]
    assert client.get("/api/experiments/9999").status_code == 404
    assert client.get("/api/experiments?limit=1001").status_code == 422
    assert client.get("/api/experiments?offset=-1").status_code == 422
    activate(client, transport)
    row = client.get("/api/nodes/node_01/history").json()["items"][0]
    assert isinstance(row["continuity"], str) and row["continuity"]


def test_live_server_cors_keeps_existing_origins(api):
    client, _, _, _ = api
    for origin in ("http://localhost:5500", "http://127.0.0.1:5500", "http://localhost:5173"):
        response = client.options(
            "/api/nodes/node_01/commands",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == origin
    assert (
        "access-control-allow-origin"
        not in client.get("/api/nodes", headers={"Origin": "https://evil.example"}).headers
    )

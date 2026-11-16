import json
from uuid import uuid4

import pytest
from conftest import availability, ingest, ready, telemetry
from pydantic import ValidationError

from smartlight.engine import Engine
from smartlight.models import PREFIX, CommandInput, parse_wire


@pytest.mark.parametrize(
    "field,value",
    [
        ("seq", True),
        ("seq", -1),
        ("seq", 2**64),
        ("uptime_ms", 1.5),
        ("schema_version", True),
        ("brightness_applied", 101),
        ("current_adc", -1),
        ("ldr_adc", "12"),
        ("distance_cm", 0),
        ("distance_cm", float("nan")),
        ("power_sim_w", float("inf")),
        ("faults", ["OPEN_LOAD"] * 2),
    ],
)
def test_invalid_fields(rig, field, value):
    engine, _, _ = rig
    data = telemetry(**{field: value})
    assert ingest(engine, "telemetry", data) == "INVALID"
    assert engine.db.execute("SELECT count(*) FROM telemetry").fetchone()[0] == 0
    assert engine.db.execute("SELECT payload FROM ingest_errors").fetchone()[0]


@pytest.mark.parametrize("payload", [b"{", b"[]", b"{}{}", b"\xff", b'{"seq":1,"seq":2}', b"x" * 2049])
def test_malformed_payload(rig, payload):
    engine, _, _ = rig
    assert engine.ingest(f"{PREFIX}/node_01/telemetry", payload) == "INVALID"


def test_topic_and_unknown_fields(rig):
    engine, _, _ = rig
    assert engine.ingest(f"{PREFIX}/node_02/telemetry", json.dumps(telemetry()).encode()) == "INVALID"
    assert ingest(engine, "telemetry", telemetry(extra=True)) == "INVALID"
    with pytest.raises(ValueError):
        parse_wire(f"{PREFIX}/node_01/diagnostics/heartbeat", b"{}")


def test_retained_online_does_not_enable_node(rig):
    engine, _, _ = rig
    ingest(engine, "availability", availability())
    assert engine.node_views()[0]["connection"] == "UNKNOWN"
    assert not engine.node_views()[0]["can_command"]
    ready(engine)
    assert engine.node_views()[0]["can_command"]


def test_unconfirmed_boot_dedup_order_and_uint64(rig):
    engine, clock, _ = rig
    assert ingest(engine, "telemetry", telemetry()) == "UNCONFIRMED_BOOT"
    ingest(engine, "availability", availability())
    assert ingest(engine, "telemetry", telemetry(3, 2**63 + 5)) == "ACCEPTED"
    assert ingest(engine, "telemetry", telemetry(3, 2**63 + 5)) == "DUPLICATE"
    clock.advance(1)
    assert ingest(engine, "telemetry", telemetry(2, 2)) == "OUT_OF_ORDER"


def test_boot_retirement_and_restart(rig, tmp_path):
    engine, clock, transport = rig
    ready(engine, telemetry(10, 10000, faults=["OPEN_LOAD"]))
    ingest(engine, "availability", availability("12345678"))
    assert not engine.node_views()[0]["can_command"]
    assert ingest(engine, "telemetry", telemetry(0, 0, boot_id="12345678")) == "ACCEPTED"
    assert ingest(engine, "telemetry", telemetry(11, 11000)) == "RETIRED_BOOT"
    ingest(engine, "availability", availability())
    assert engine.nodes["node_01"].candidate == "12345678"
    engine.close()
    again = Engine(tmp_path / "test.db", transport, clock)
    try:
        assert again.node_views()[0]["freshness"] == "STALE"
        again.connection(True, "session-1")
        ingest(again, "availability", availability("12345678"))
        assert ingest(again, "telemetry", telemetry(0, 0, boot_id="12345678")) == "DUPLICATE"
        assert not again.node_views()[0]["can_command"]
        assert ingest(again, "telemetry", telemetry(1, 2000, boot_id="12345678")) == "ACCEPTED"
        assert again.node_views()[0]["can_command"]
    finally:
        again.close()


def test_fault_offline_recovery_alerts(rig):
    engine, clock, _ = rig
    ready(engine, telemetry(faults=["OPEN_LOAD"]))
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(1, 2000, faults=["OPEN_LOAD"]))
    assert len(engine.reporting.alerts()["items"]) == 1
    clock.advance(10)
    engine.tick()
    assert engine.node_views()[0]["freshness"] == "STALE"
    alerts = engine.reporting.alerts()["items"]
    assert {a["kind"] for a in alerts} == {"OPEN_LOAD", "OFFLINE"}
    assert all(a["ended_at"] is None for a in alerts)
    ingest(engine, "telemetry", telemetry(2, 4000))
    assert all(a["ended_at"] is not None for a in engine.reporting.alerts()["items"])


def test_offline_and_old_session_cannot_reclaim(rig):
    engine, _, _ = rig
    ready(engine)
    ingest(engine, "availability", availability(status="OFFLINE"))
    assert ingest(engine, "telemetry", telemetry(1, 2000)) == "UNCONFIRMED_BOOT"
    assert not engine.node_views()[0]["can_command"]
    assert ingest(engine, "telemetry", telemetry(2, 4000), session="other") == "OLD_SESSION"


def test_ack_validation():
    ack = dict(
        schema_version=1, node_id="node_01", boot_id="b7e28a10", command_id=str(uuid4()), result="APPLIED", reason=None
    )
    assert parse_wire(f"{PREFIX}/node_01/ack", json.dumps(ack).encode())[1].result == "APPLIED"
    ack["reason"] = "EXPIRED"
    with pytest.raises(ValidationError):
        parse_wire(f"{PREFIX}/node_01/ack", json.dumps(ack).encode())


@pytest.mark.parametrize(
    "body",
    [
        {"mode": "AUTO", "brightness": 0},
        {"mode": "AUTO", "brightness": None},
        {"mode": "MANUAL"},
        {"mode": "MANUAL", "brightness": True},
        {"mode": "MANUAL", "brightness": 80.5},
        {"mode": "MANUAL", "brightness": -1},
        {"mode": "MANUAL", "brightness": 101},
        {"mode": "AUTO", "extra": 1},
    ],
)
def test_command_request_strict(body):
    with pytest.raises(ValidationError):
        CommandInput.model_validate(body)


def test_second_database_owner_fails_without_duplicate_collector(rig, tmp_path):
    engine, clock, transport = rig
    with pytest.raises(RuntimeError, match="another backend"):
        Engine(tmp_path / "test.db", transport, clock)
    assert engine.db.execute("PRAGMA user_version").fetchone()[0] == 1

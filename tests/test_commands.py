import pytest
from conftest import ingest, ready, telemetry

from smartlight.engine import Engine
from smartlight.errors import DomainError
from smartlight.models import CommandInput


def ack_for(command, **changes):
    return dict(
        dict(
            schema_version=1,
            node_id="node_01",
            boot_id=command["boot_id"],
            command_id=command["command_id"],
            result="APPLIED",
            reason=None,
        ),
        **changes,
    )


def test_persist_before_publish_retry_once_then_timeout(rig):
    engine, clock, transport = rig
    ready(engine)
    transport.before_publish = lambda: (
        engine.db.execute("SELECT command_id FROM commands WHERE result='PENDING'").fetchone()
        or pytest.fail("not persisted")
    )
    command = engine.create_command("node_01", CommandInput(mode="MANUAL", brightness=80))
    assert command["result"] == "PENDING" and len(transport.published) == 1
    with pytest.raises(DomainError) as error:
        engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert error.value.status == 409
    clock.advance(1.99)
    engine.tick()
    assert len(transport.published) == 1
    clock.advance(0.01)
    engine.tick()
    assert len(transport.published) == 2 and transport.published[0] == transport.published[1]
    clock.advance(8)
    engine.tick()
    assert engine.command_view(command["command_id"])["result"] == "TIMEOUT"
    assert ingest(engine, "ack", ack_for(command)) == "LATE_OR_DUPLICATE"
    assert engine.command_view(command["command_id"])["result"] == "TIMEOUT"


@pytest.mark.parametrize("result,reason", [("APPLIED", None), ("REJECTED", "EXPIRED"), ("REJECTED", "INVALID_COMMAND")])
def test_terminal_acks(rig, result, reason):
    engine, clock, transport = rig
    ready(engine)
    command = engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert ingest(engine, "ack", ack_for(command, result=result, reason=reason)) == "ACCEPTED"
    assert engine.command_view(command["command_id"])["result"] == result
    clock.advance(20)
    engine.tick()
    assert len(transport.published) == 1


def test_wrong_node_boot_and_boot_mismatch_exception(rig):
    engine, _, _ = rig
    ready(engine)
    command = engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert ingest(engine, "ack", ack_for(command, node_id="node_02")) == "UNMATCHED"
    assert ingest(engine, "ack", ack_for(command, boot_id="00000000")) == "BOOT_MISMATCH"
    assert (
        ingest(engine, "ack", ack_for(command, boot_id="00000000", result="REJECTED", reason="BOOT_MISMATCH"))
        == "ACCEPTED"
    )


def test_disconnect_no_retry_and_no_restart_replay(rig, tmp_path):
    engine, clock, transport = rig
    ready(engine)
    command = engine.create_command("node_01", CommandInput(mode="AUTO"))
    engine.connection(False, "session-1")
    transport.session = "session-2"
    engine.connection(True, transport.session)
    ready(engine, telemetry(1, 2000))
    clock.advance(2)
    engine.tick()
    assert len(transport.published) == 1
    engine.close()
    again = Engine(tmp_path / "test.db", transport, clock)
    try:
        assert again.command_view(command["command_id"])["result"] == "TIMEOUT"
        assert again.command_view(command["command_id"])["reason"] == "BACKEND_RESTART"
        assert len(transport.published) == 1
    finally:
        again.close()


def test_stale_db_broker_queue_and_unknown_node(rig):
    engine, clock, transport = rig
    ready(engine)
    clock.advance(3.01)
    with pytest.raises(DomainError) as error:
        engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert error.value.status == 409
    transport.connected = False
    with pytest.raises(DomainError) as error:
        engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert error.value.status == 503
    with pytest.raises(DomainError) as error:
        engine.create_command("node_99", CommandInput(mode="AUTO"))
    assert error.value.status == 404
    transport.connected = True
    ready(engine, telemetry(1, 2000))
    engine.pipeline_gap()
    with pytest.raises(DomainError) as error:
        engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert error.value.status == 503


def test_publish_failed_never_means_applied(rig):
    engine, _, transport = rig
    ready(engine)
    transport.succeeds = False
    command = engine.create_command("node_01", CommandInput(mode="AUTO"))
    assert command["result"] == "PENDING"


def test_ack_at_deadline_cannot_win_before_timer_tick(rig):
    engine, clock, _ = rig
    ready(engine)
    command = engine.create_command("node_01", CommandInput(mode="AUTO"))
    clock.advance(10)
    assert ingest(engine, "ack", ack_for(command)) == "LATE_OR_DUPLICATE"
    assert engine.command_view(command["command_id"])["result"] == "TIMEOUT"

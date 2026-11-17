"""Actor wake-up races, using synthetic MQTT events rather than live captures."""

import asyncio
import json

import pytest
from conftest import FakeClock, FakeTransport, availability, telemetry

from smartlight.config import Settings
from smartlight.models import PREFIX
from smartlight.service import Service


@pytest.mark.parametrize("initially_connected", [False, True])
@pytest.mark.parametrize("event_session", ["new-session", "old-session"])
def test_retained_online_arrives_during_actor_queue_wait(tmp_path, initially_connected, event_session):
    transport, clock = FakeTransport(), FakeClock()
    transport.connected = initially_connected
    transport.session = "old-session"
    service = Service(Settings(database=tmp_path / "actor.db"), transport, clock)
    original_get = service.jobs.get
    transitioned = False

    def get_with_session_transition(*args, **kwargs):
        nonlocal transitioned
        job = original_get(*args, **kwargs)
        if job[0] == "ingest" and not transitioned:
            # Simulate SUBACK becoming ready and delivering retained ONLINE while
            # the actor is blocked in Queue.get, after its preceding readiness check.
            transport.session = "new-session"
            transport.connected = True
            transitioned = True
        return job

    service.jobs.get = get_with_session_transition
    service.start()
    try:
        assert service.ingest(f"{PREFIX}/node_01/availability", json.dumps(availability()).encode(), event_session)
        assert service.ingest(f"{PREFIX}/node_01/telemetry", json.dumps(telemetry(1, 2000)).encode(), "new-session")
        node = asyncio.run(service.call("node_views"))[0]
        assert transitioned
        assert node["can_command"] is (event_session == "new-session")
        assert node["freshness"] == ("FRESH" if event_session == "new-session" else "STALE")
    finally:
        service.stop()

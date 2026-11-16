"""All MQTT data in this suite are labeled synthetic fixtures, never Wokwi captures."""

import json

import pytest

from smartlight.engine import Engine
from smartlight.models import PREFIX


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.wall = 1700000000.0

    def utc(self):
        return self.wall + self.value

    def mono(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class FakeTransport:
    def __init__(self):
        self.connected = True
        self.session = "session-1"
        self.published = []
        self.succeeds = True
        self.sink = None
        self.before_publish = None

    def start(self, sink):
        self.sink = sink

    def stop(self):
        self.connected = False

    def publish(self, topic, payload, session):
        if self.before_publish:
            self.before_publish()
        if not self.connected or session != self.session:
            return False
        self.published.append((topic, json.loads(payload), session))
        return self.succeeds


def telemetry(seq=0, uptime=0, **changes):
    value = dict(
        schema_version=1,
        node_id="node_01",
        boot_id="b7e28a10",
        seq=seq,
        uptime_ms=uptime,
        mode="AUTO",
        environment="NIGHT",
        ldr_adc=3100,
        motion="NONE",
        distance_cm=150.0,
        brightness_target=25,
        brightness_applied=25,
        current_adc=4095,
        current_ma_sim=500.0,
        power_sim_w=6.0,
        faults=[],
    )
    return dict(value, **changes)


def availability(boot="b7e28a10", status="ONLINE", node="node_01"):
    return dict(schema_version=1, node_id=node, boot_id=boot, status=status)


def ingest(engine, kind, value, **kwargs):
    return engine.ingest(f"{PREFIX}/{value['node_id']}/{kind}", json.dumps(value).encode(), **kwargs)


def ready(engine, value=None):
    ingest(engine, "availability", availability())
    return ingest(engine, "telemetry", value or telemetry())


@pytest.fixture
def rig(tmp_path):
    clock, transport = FakeClock(), FakeTransport()
    engine = Engine(tmp_path / "test.db", transport, clock)
    engine.connection(True, transport.session)
    yield engine, clock, transport
    engine.close()

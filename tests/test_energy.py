import csv
import io

import pytest
from conftest import availability, ingest, ready, telemetry

from smartlight.models import ExperimentInput


@pytest.mark.parametrize("power,brightness,expected", [(24, 100, 0.4), (6, 25, 0.1)])
def test_known_energy_and_simulated_vs_real_clocks(rig, power, brightness, expected):
    engine, clock, _ = rig
    ready(engine, telemetry(power_sim_w=power, brightness_applied=brightness))
    for i in range(1, 31):
        clock.advance(6)  # Wokwi 2s/node for 6s real; freshness remains valid.
        ingest(engine, "telemetry", telemetry(i, i * 2000, power_sim_w=power, brightness_applied=brightness))
    stats = engine.reporting.stats("node_01", clock.wall, clock.utc())
    assert stats["total_energy_wh"] == pytest.approx(expected)
    assert stats["nodes"][0]["valid_uptime_s"] == 60
    assert stats["nodes"][0]["covered_wall_s"] == 180
    assert stats["nodes"][0]["coverage_percent"] == 100
    assert stats["baseline_wh"] == pytest.approx(0.4)
    assert stats["saving_percent"] == pytest.approx(0 if power == 24 else 75)
    assert (
        ingest(engine, "telemetry", telemetry(30, 60000, power_sim_w=power, brightness_applied=brightness))
        == "DUPLICATE"
    )
    assert engine.reporting.stats("node_01", clock.wall, clock.utc())["total_energy_wh"] == stats["total_energy_wh"]


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"mode": "MANUAL"}, "MANUAL"),
        ({"faults": ["OPEN_LOAD"]}, "FAULT_OR_RECOVERY"),
        ({"current_adc": 3890}, "LOAD_CALIBRATION_OR_FAULT_CONFIRMATION"),
        ({"environment": "UNKNOWN"}, "UNKNOWN_ENVIRONMENT"),
    ],
)
def test_auto_exclusions_keep_total_energy(rig, changes, reason):
    engine, clock, _ = rig
    ready(engine, telemetry(**changes))
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(1, 2000, **changes))
    stats = engine.reporting.stats("node_01", clock.wall, clock.utc())
    assert stats["total_energy_wh"] > 0 and stats["auto_energy_wh"] == 0
    assert stats["nodes"][0]["excluded_intervals"][reason] == 1


def test_day_zero_baseline_and_state_transition(rig):
    engine, clock, _ = rig
    ready(engine, telemetry(environment="DAY", power_sim_w=0, brightness_applied=0))
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(1, 2000, environment="DAY", power_sim_w=0, brightness_applied=0))
    assert engine.reporting.stats("node_01", clock.wall, clock.utc())["saving_percent"] is None
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(2, 4000))
    stats = engine.reporting.stats("node_01", clock.wall, clock.utc())
    assert stats["nodes"][0]["excluded_intervals"]["STATE_CHANGE"] == 1
    assert stats["baseline_wh"] == 0 and stats["total_energy_wh"] > 0


@pytest.mark.parametrize("case", ["gap", "boot", "disconnect", "invalid", "overflow", "stale"])
def test_never_bridge_missing_or_invalid_data(rig, case):
    engine, clock, transport = rig
    ready(engine)
    clock.advance(2)
    if case == "gap":
        next_value = telemetry(1, 7000)
    elif case == "boot":
        ingest(engine, "availability", availability("12345678"))
        next_value = telemetry(0, 0, boot_id="12345678")
    elif case == "disconnect":
        engine.connection(False, "session-1")
        engine.connection(True, "session-2")
        ingest(engine, "availability", availability())
        next_value = telemetry(1, 2000)
    elif case == "invalid":
        ingest(engine, "telemetry", telemetry(1, 1000, current_adc=-1))
        next_value = telemetry(2, 2000)
    elif case == "overflow":
        engine.pipeline_gap()
        next_value = telemetry(1, 2000)
    else:
        clock.advance(10)
        engine.tick()
        next_value = telemetry(1, 2000)
    ingest(engine, "telemetry", next_value)
    assert engine.reporting.stats("node_01", clock.wall, clock.utc())["total_energy_wh"] == 0


def test_experiment_limits_idempotence_and_csv_agreement(rig):
    engine, clock, _ = rig
    ready(engine)
    clock.advance(1)
    experiment = engine.reporting.start_experiment(
        ExperimentInput(name="fixture", configuration={"source": "synthetic"})
    )
    with pytest.raises(Exception) as error:
        engine.reporting.start_experiment(ExperimentInput(name="second"))
    assert error.value.status == 409
    clock.advance(1)
    ingest(engine, "telemetry", telemetry(1, 2000))
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(2, 4000))
    clock.advance(1)
    stopped = engine.reporting.stop_experiment(experiment["id"])
    clock.advance(1)
    ingest(engine, "telemetry", telemetry(3, 6000))
    assert engine.reporting.stop_experiment(experiment["id"]) == stopped
    stats = engine.reporting.stats("node_01", experiment_id=experiment["id"])
    rows = list(csv.DictReader(io.StringIO(engine.reporting.export(experiment["id"]))))
    assert len(rows) == 2
    assert sum(float(row["total_energy_wh"]) for row in rows) == pytest.approx(stats["total_energy_wh"])
    assert stats["total_energy_wh"] == pytest.approx(6 * 2 / 3600)
    assert all(row["received_at"].endswith("Z") and row["experiment_name"] == "fixture" for row in rows)


def test_experiment_default_includes_older_than_one_hour(rig):
    engine, clock, _ = rig
    experiment = engine.reporting.start_experiment(ExperimentInput(name="long fixture"))
    ready(engine)
    clock.advance(2)
    ingest(engine, "telemetry", telemetry(1, 2000))
    clock.advance(4000)
    engine.reporting.stop_experiment(experiment["id"])
    assert engine.reporting.stats("node_01", experiment_id=experiment["id"])["total_energy_wh"] > 0

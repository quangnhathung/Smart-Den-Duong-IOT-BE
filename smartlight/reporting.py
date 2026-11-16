"""One source of energy/quality calculations for HTTP statistics and CSV export."""

import csv
import io
import json
from collections import Counter
from datetime import datetime, timezone

from .errors import DomainError
from .models import NODE_IDS


def segments(rows):
    previous = None
    for row in rows:
        current = dict(row)
        current["data"] = json.loads(current["payload"])
        reasons = []
        energy = baseline = auto_energy = duration = covered = 0.0
        if previous is None:
            reasons.append("FIRST_SAMPLE")
        else:
            left, right = previous["data"], current["data"]
            delta = (right["uptime_ms"] - left["uptime_ms"]) / 1000
            if previous["quality"] != "ACCEPTED" or current["quality"] != "ACCEPTED":
                reasons.append("QUALITY")
            if left["boot_id"] != right["boot_id"]:
                reasons.append("BOOT_CHANGE")
            if previous["continuity"] != current["continuity"]:
                reasons.append("DISCONTINUITY")
            if right["seq"] <= left["seq"] or not 0 < delta <= 6:
                reasons.append("ORDER_OR_GAP")
            if current["received_at"] <= previous["received_at"]:
                reasons.append("RECEIVE_CLOCK_ORDER")
            if not reasons:
                duration = delta
                covered = current["received_at"] - previous["received_at"]
                energy = (left["power_sim_w"] / 2 + right["power_sim_w"] / 2) / 3600 * delta
                if left["mode"] != "AUTO" or right["mode"] != "AUTO":
                    reasons.append("MANUAL")
                if left["faults"] or right["faults"]:
                    reasons.append("FAULT_OR_RECOVERY")
                if min(left["current_adc"], right["current_adc"]) / 4095 < 0.95:
                    reasons.append("LOAD_CALIBRATION_OR_FAULT_CONFIRMATION")
                if left["environment"] == "UNKNOWN" or right["environment"] == "UNKNOWN":
                    reasons.append("UNKNOWN_ENVIRONMENT")
                if (
                    left["environment"] != right["environment"]
                    or left["mode"] != right["mode"]
                    or left["faults"] != right["faults"]
                ):
                    reasons.append("STATE_CHANGE")
                if not reasons:
                    auto_energy = energy
                    baseline = (0 if right["environment"] == "DAY" else 24) * delta / 3600
        current.update(
            total_energy_wh=energy,
            auto_energy_wh=auto_energy,
            baseline_wh=baseline,
            valid_uptime_s=duration,
            covered_wall_s=covered,
            auto_eligible=bool(previous is not None and not reasons),
            excluded_reasons=reasons,
        )
        yield current
        previous = current


class Reporting:
    def __init__(self, db, clock):
        self.db, self.clock = db, clock

    def window(self, start=None, end=None, experiment_id=None):
        if start is not None and end is not None and start >= end:
            raise DomainError(422, "from must precede to")
        experiment = self.experiment(experiment_id) if experiment_id is not None else None
        if experiment is not None:
            start = experiment["started_at"] if start is None else max(start, experiment["started_at"])
            finish = experiment["ended_at"] if experiment["ended_at"] is not None else self.clock.utc()
            end = finish if end is None else min(end, finish)
            if start > end:
                raise DomainError(422, "time range does not overlap experiment")
        else:
            end = self.clock.utc() if end is None else end
            start = end - 3600 if start is None else start
            if start >= end:
                raise DomainError(422, "from must precede to")
        return start, end, experiment

    def rows(self, node, start, end):
        return self.db.execute(
            "SELECT * FROM telemetry WHERE node_id=? AND received_at>=? AND received_at<=? ORDER BY received_at,id",
            (node, start, end),
        ).fetchall()

    def history(self, node, start=None, end=None, experiment_id=None, limit=100, offset=0):
        if node not in NODE_IDS:
            raise DomainError(404, "Node not found")
        start, end, _ = self.window(start, end, experiment_id)
        total = self.db.execute(
            "SELECT count(*) FROM telemetry WHERE node_id=? AND received_at>=? AND received_at<=?", (node, start, end)
        ).fetchone()[0]
        rows = self.db.execute(
            """SELECT * FROM telemetry WHERE node_id=? AND received_at>=? AND received_at<=?
            ORDER BY received_at,id LIMIT ? OFFSET ?""",
            (node, start, end, limit, offset),
        ).fetchall()
        items = [
            dict(id=r["id"], received_at=r["received_at"], quality=r["quality"], telemetry=json.loads(r["payload"]))
            for r in rows
        ]
        return dict(items=items, total=total, limit=limit, offset=offset)

    def alerts(self, node=None, limit=100, offset=0):
        if node is not None and node not in NODE_IDS:
            raise DomainError(404, "Node not found")
        condition, values = ("WHERE node_id=?", (node,)) if node else ("", ())
        total = self.db.execute(f"SELECT count(*) FROM alerts {condition}", values).fetchone()[0]
        rows = self.db.execute(
            f"SELECT * FROM alerts {condition} ORDER BY started_at DESC,id DESC LIMIT ? OFFSET ?",
            (*values, limit, offset),
        )
        return dict(items=[dict(row) for row in rows], total=total, limit=limit, offset=offset)

    def stats(self, node=None, start=None, end=None, experiment_id=None):
        if node is not None and node not in NODE_IDS:
            raise DomainError(404, "Node not found")
        start, end, _ = self.window(start, end, experiment_id)
        results = []
        for selected in (node,) if node else NODE_IDS:
            data = list(segments(self.rows(selected, start, end)))
            excluded = Counter(reason for item in data for reason in item["excluded_reasons"])
            elapsed = max(0.0, end - start)
            valid = sum(item["valid_uptime_s"] for item in data)
            covered = min(elapsed, sum(item["covered_wall_s"] for item in data))
            smart = sum(item["auto_energy_wh"] for item in data)
            baseline = sum(item["baseline_wh"] for item in data)
            results.append(
                dict(
                    node_id=selected,
                    total_energy_wh=sum(item["total_energy_wh"] for item in data),
                    auto_energy_wh=smart,
                    baseline_wh=baseline,
                    saving_percent=(baseline - smart) / baseline * 100 if baseline else None,
                    valid_uptime_s=valid,
                    covered_wall_s=covered,
                    missing_wall_s=max(0, elapsed - covered),
                    coverage_percent=covered / elapsed * 100 if elapsed else None,
                    auto_valid_uptime_s=sum(item["valid_uptime_s"] for item in data if item["auto_eligible"]),
                    sample_count=len(data),
                    excluded_intervals=dict(excluded),
                )
            )
        baseline = sum(item["baseline_wh"] for item in results)
        smart = sum(item["auto_energy_wh"] for item in results)
        elapsed = max(0, end - start) * len(results)
        covered = sum(item["covered_wall_s"] for item in results)
        return dict(
            valid_uptime_s=sum(item["valid_uptime_s"] for item in results),
            covered_wall_s=covered,
            missing_wall_s=max(0, elapsed - covered),
            coverage_percent=covered / elapsed * 100 if elapsed else None,
            from_time=start,
            to_time=end,
            clock_basis=dict(energy="node_uptime", coverage="receiver_utc"),
            nodes=results,
            total_energy_wh=sum(item["total_energy_wh"] for item in results),
            auto_energy_wh=smart,
            baseline_wh=baseline,
            saving_percent=(baseline - smart) / baseline * 100 if baseline else None,
        )

    def experiment(self, experiment_id):
        row = self.db.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if not row:
            raise DomainError(404, "Experiment not found")
        result = dict(row)
        result["configuration"] = json.loads(result["configuration"])
        return result

    def start_experiment(self, request):
        if self.db.execute("SELECT id FROM experiments WHERE ended_at IS NULL").fetchone():
            raise DomainError(409, "An experiment is already open")
        with self.db:
            cursor = self.db.execute(
                "INSERT INTO experiments(name,configuration,notes,started_at) VALUES (?,?,?,?)",
                (request.name, json.dumps(request.configuration, allow_nan=False), request.notes, self.clock.utc()),
            )
        return self.experiment(cursor.lastrowid)

    def stop_experiment(self, experiment_id):
        self.experiment(experiment_id)
        with self.db:
            self.db.execute(
                "UPDATE experiments SET ended_at=? WHERE id=? AND ended_at IS NULL", (self.clock.utc(), experiment_id)
            )
        return self.experiment(experiment_id)

    def export(self, experiment_id):
        experiment = self.experiment(experiment_id)
        start, end = experiment["started_at"], experiment["ended_at"] or self.clock.utc()
        output = io.StringIO(newline="")
        columns = [
            "experiment_id",
            "experiment_name",
            "configuration",
            "notes",
            "started_at",
            "ended_at",
            "node_id",
            "received_at",
            "quality",
            "continuity",
            "schema_version",
            "boot_id",
            "seq",
            "uptime_ms",
            "mode",
            "environment",
            "ldr_adc",
            "motion",
            "distance_cm",
            "brightness_target",
            "brightness_applied",
            "current_adc",
            "current_ma_sim",
            "power_sim_w",
            "faults",
            "total_energy_wh",
            "auto_energy_wh",
            "baseline_wh",
            "valid_uptime_s",
            "covered_wall_s",
            "auto_eligible",
            "excluded_reasons",
        ]
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for node in NODE_IDS:
            for row in segments(self.rows(node, start, end)):
                result = dict(
                    experiment_id=experiment_id,
                    experiment_name=experiment["name"],
                    configuration=json.dumps(experiment["configuration"]),
                    notes=experiment["notes"],
                    started_at=start,
                    ended_at=experiment["ended_at"],
                    received_at=row["received_at"],
                    quality=row["quality"],
                    continuity=row["continuity"],
                    **row["data"],
                )
                result.update({key: row[key] for key in columns if key in row and key not in result})
                result["faults"] = json.dumps(result["faults"])
                result["excluded_reasons"] = "|".join(row["excluded_reasons"])
                for key in ("received_at", "started_at", "ended_at"):
                    if result[key] is not None:
                        result[key] = (
                            datetime.fromtimestamp(result[key], timezone.utc).isoformat().replace("+00:00", "Z")
                        )
                for key in ("experiment_name", "notes"):
                    if result[key].lstrip().startswith(("=", "+", "-", "@")):
                        result[key] = "'" + result[key]
                writer.writerow(result)
        return output.getvalue()

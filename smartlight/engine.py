"""Serial domain service. Invoke only on its owning worker (tests may own it directly)."""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from uuid import uuid4

from .errors import DomainError
from .models import NODE_IDS, PREFIX, Ack, Availability, CommandInput, Telemetry, parse_wire
from .reporting import Reporting
from .storage import DatabaseLock, open_database

LOG = logging.getLogger(__name__)


class Clock:
    def utc(self):
        return time.time()

    def mono(self):
        return time.monotonic()


@dataclass
class Runtime:
    boot: str | None = None
    payload: dict | None = None
    seen: float | None = None
    received_mono: float | None = None
    fresh: bool = False
    barrier: int = 0
    candidate: str | None = None
    retired: set[str] = field(default_factory=set)


class Engine:
    def __init__(self, database, transport, clock=None):
        self.clock = clock or Clock()
        self.transport = transport
        self.instance_lock = DatabaseLock(database)
        try:
            self.db = open_database(database)
            with self.db:
                self.db.execute("UPDATE commands SET result='TIMEOUT',reason='BACKEND_RESTART' WHERE result='PENDING'")
        except Exception:
            self.instance_lock.close()
            raise
        self.db_ok = True
        self.reporting = Reporting(self.db, self.clock)
        self.queue_ok = True
        self.broker = False
        self.session = ""
        self.run = str(uuid4())
        self.pending = {}
        self.nodes = {}
        for row in self.db.execute("SELECT * FROM nodes"):
            retired = {r[0] for r in self.db.execute("SELECT boot_id FROM boots WHERE node_id=?", (row["node_id"],))}
            retired.discard(row["boot_id"])
            self.nodes[row["node_id"]] = Runtime(
                row["boot_id"],
                json.loads(row["payload"]) if row["payload"] else None,
                row["last_seen"],
                retired=retired,
            )

    def close(self):
        self.db.close()
        self.instance_lock.close()

    def require_node(self, node):
        if node not in NODE_IDS:
            raise DomainError(404, "Node not found")

    def require_database(self):
        if not self.db_ok:
            raise DomainError(503, "Database unavailable")

    def connection(self, connected, session):
        self.broker = connected
        self.session = session
        # Never carry a pending publisher retry into another TCP session.
        for item in self.pending.values():
            item["retry_allowed"] = False
        for node, state in self.nodes.items():
            state.fresh = False
            state.received_mono = None
            state.candidate = None
            state.barrier += 1
            if not connected and state.payload:
                with self.db:
                    self._alert(node, "OFFLINE", True, self.clock.utc())

    def pipeline_gap(self):
        self.queue_ok = False
        for state in self.nodes.values():
            state.fresh = False
            state.barrier += 1

    def _alert(self, node, kind, active, stamp):
        if active:
            self.db.execute("INSERT OR IGNORE INTO alerts(node_id,kind,started_at) VALUES (?,?,?)", (node, kind, stamp))
        else:
            self.db.execute(
                "UPDATE alerts SET ended_at=? WHERE node_id=? AND kind=? AND ended_at IS NULL", (stamp, node, kind)
            )

    def ingest(self, topic, payload, received=None, monotonic=None, session=None, generation=0):
        stamp = self.clock.utc() if received is None else received
        mono = self.clock.mono() if monotonic is None else monotonic
        try:
            kind, value = parse_wire(topic, payload)
        except (ValueError, UnicodeError) as error:
            LOG.warning("MQTT rejected %s: %s", topic, error)
            with self.db:
                self.db.execute(
                    "INSERT INTO ingest_errors(received_at,topic,reason,payload) VALUES (?,?,?,?)",
                    (stamp, topic, str(error)[:500], payload[:2049]),
                )
            parts = topic.split("/")
            if len(parts) == 6 and parts[4] in self.nodes and parts[5] == "telemetry":
                self.nodes[parts[4]].barrier += 1
            return "INVALID"
        event_session = self.session if session is None else session
        if event_session != self.session or not self.broker:
            return "OLD_SESSION"
        if isinstance(value, Telemetry):
            return self._telemetry(value, stamp, mono, generation)
        if isinstance(value, Availability):
            return self._availability(value, stamp)
        if isinstance(value, Ack):
            return self._ack(value, stamp)
        raise AssertionError(kind)

    def _availability(self, value, stamp):
        state = self.nodes[value.node_id]
        with self.db:
            self.db.execute(
                "INSERT INTO availability(node_id,boot_id,status,received_at,session) VALUES (?,?,?,?,?)",
                (value.node_id, value.boot_id, value.status, stamp, self.session),
            )
            if value.status == "OFFLINE" and value.boot_id == state.boot:
                self._alert(value.node_id, "OFFLINE", True, stamp)
        if value.status == "ONLINE" and value.boot_id not in state.retired:
            if value.boot_id != state.boot:
                state.fresh = False
                state.barrier += 1
            state.candidate = value.boot_id
        elif value.status == "OFFLINE" and value.boot_id in (state.boot, state.candidate):
            state.fresh = False
            state.candidate = None
            state.barrier += 1
        return "RECORDED"

    def _telemetry(self, value, stamp, mono, generation):
        state = self.nodes[value.node_id]
        payload = value.model_dump()
        quality = "ACCEPTED"
        if value.boot_id in state.retired:
            quality = "RETIRED_BOOT"
        elif state.candidate != value.boot_id:
            quality = "UNCONFIRMED_BOOT"
        elif (
            state.boot == value.boot_id
            and state.payload
            and (value.seq <= state.payload["seq"] or value.uptime_ms <= state.payload["uptime_ms"])
        ):
            quality = "OUT_OF_ORDER"
        continuity = f"{self.run}/{self.session}/{state.barrier}/{generation}"
        with self.db:
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO telemetry
                (node_id,boot_id,seq,uptime_ms,received_at,payload,quality,continuity) VALUES (?,?,?,?,?,?,?,?)""",
                (
                    value.node_id,
                    value.boot_id,
                    str(value.seq),
                    str(value.uptime_ms),
                    stamp,
                    json.dumps(payload),
                    quality,
                    continuity,
                ),
            )
            if cursor.rowcount == 0:
                return "DUPLICATE"
            if quality == "ACCEPTED":
                self.db.execute("INSERT OR IGNORE INTO boots VALUES (?,?)", (value.node_id, value.boot_id))
                self.db.execute(
                    "UPDATE nodes SET boot_id=?,last_seen=?,payload=? WHERE node_id=?",
                    (value.boot_id, stamp, json.dumps(payload), value.node_id),
                )
                self._alert(value.node_id, "OPEN_LOAD", bool(value.faults), stamp)
                if self.clock.mono() - mono < 10:
                    self._alert(value.node_id, "OFFLINE", False, stamp)
        if quality == "ACCEPTED":
            if state.boot and state.boot != value.boot_id:
                state.retired.add(state.boot)
            state.boot, state.payload, state.seen = value.boot_id, payload, stamp
            state.received_mono = mono
            state.fresh = self.clock.mono() - mono < 10
        return quality

    def _ack(self, value, stamp):
        pending = self.pending.get(value.command_id)
        if pending and self.clock.mono() - pending["start"] >= 10:
            with self.db:
                self.db.execute(
                    "UPDATE commands SET result='TIMEOUT',reason='ACK_TIMEOUT' WHERE command_id=? AND result='PENDING'",
                    (value.command_id,),
                )
            self.pending.pop(value.command_id, None)
        row = self.db.execute("SELECT * FROM commands WHERE command_id=?", (value.command_id,)).fetchone()
        disposition = "UNMATCHED"
        if row and row["node_id"] == value.node_id:
            if row["result"] != "PENDING":
                disposition = "LATE_OR_DUPLICATE"
            elif row["boot_id"] != value.boot_id and not (
                value.result == "REJECTED" and value.reason == "BOOT_MISMATCH"
            ):
                disposition = "BOOT_MISMATCH"
            else:
                disposition = "ACCEPTED"
        with self.db:
            self.db.execute(
                "INSERT INTO ack_events(command_id,received_at,payload,disposition) VALUES (?,?,?,?)",
                (value.command_id, stamp, value.model_dump_json(), disposition),
            )
            if disposition == "ACCEPTED":
                self.db.execute(
                    "UPDATE commands SET result=?,reason=?,ack_at=? WHERE command_id=?",
                    (value.result, value.reason, stamp, value.command_id),
                )
        if disposition == "ACCEPTED":
            self.pending.pop(value.command_id, None)
        return disposition

    def node_views(self):
        result = []
        for node, state in self.nodes.items():
            age = None if state.received_mono is None else max(0, self.clock.mono() - state.received_mono)
            fresh = bool(self.broker and state.fresh and age is not None and age < 10)
            pending = self.db.execute(
                "SELECT command_id FROM commands WHERE node_id=? AND result='PENDING'", (node,)
            ).fetchone()
            can_command = bool(
                fresh and age <= 3 and not pending and self.db_ok and self.queue_ok and self.transport.connected
            )
            result.append(
                dict(
                    node_id=node,
                    connection="ONLINE" if fresh else ("UNKNOWN" if not state.payload else "OFFLINE"),
                    freshness="FRESH" if fresh else "STALE",
                    last_seen=state.seen,
                    age_s=age,
                    telemetry=state.payload,
                    faults=state.payload["faults"] if state.payload else [],
                    pending_command_id=pending[0] if pending else None,
                    can_command=can_command,
                )
            )
        return result

    def health(self):
        try:
            with self.db:
                self.db.execute("UPDATE nodes SET last_seen=last_seen WHERE 0")
            self.db_ok = True
        except sqlite3.Error:
            self.db_ok = False
        broker = self.broker and self.transport.connected
        return dict(
            status="OK" if self.db_ok and broker and self.queue_ok else "DEGRADED",
            database=self.db_ok,
            broker=broker,
            queue=self.queue_ok,
        )

    def create_command(self, node, request: CommandInput):
        self.require_node(node)
        self.require_database()
        if not self.broker or not self.transport.connected or not self.queue_ok:
            raise DomainError(503, "Broker or processing queue unavailable")
        view = next(item for item in self.node_views() if item["node_id"] == node)
        if not view["can_command"]:
            raise DomainError(409, "Node offline, telemetry older than 3s, or command pending")
        state = self.nodes[node]
        if state.payload["uptime_ms"] > 2**64 - 5001:
            raise DomainError(409, "Node uptime cannot accommodate deadline")
        command_id = str(uuid4())
        payload = dict(
            schema_version=1,
            node_id=node,
            boot_id=state.boot,
            command_id=command_id,
            deadline_uptime_ms=state.payload["uptime_ms"] + 5000,
            **request.model_dump(exclude_unset=True),
        )
        with self.db:
            self.db.execute(
                """INSERT INTO commands(command_id,node_id,boot_id,payload,created_at,result)
                VALUES (?,?,?,?,?,'PENDING')""",
                (command_id, node, state.boot, json.dumps(payload), self.clock.utc()),
            )
        self.pending[command_id] = dict(
            start=self.clock.mono(), session=self.session, retry_allowed=True, checked=False
        )
        try:
            self._publish(command_id, payload, False)
        except sqlite3.Error:
            self.pending[command_id]["retry_allowed"] = False
            raise
        return self.command_view(command_id)

    def _publish(self, command_id, payload, retry):
        column = "retry_at" if retry else "sent_at"
        with self.db:
            self.db.execute(
                f"UPDATE commands SET {column}=?,attempts=attempts+1 WHERE command_id=?", (self.clock.utc(), command_id)
            )
        try:
            sent = self.transport.publish(
                f"{PREFIX}/{payload['node_id']}/command", json.dumps(payload, separators=(",", ":")), self.session
            )
        except (OSError, RuntimeError):
            sent = False
        if not sent:
            LOG.warning("Command publish failed: %s (accepted in DB, not APPLIED)", command_id)

    def command_view(self, command_id):
        row = self.db.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
        if not row:
            raise DomainError(404, "Command not found")
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def tick(self):
        now = self.clock.mono()
        for node, state in self.nodes.items():
            if state.fresh and state.received_mono is not None and now - state.received_mono >= 10:
                with self.db:
                    self._alert(node, "OFFLINE", True, self.clock.utc())
                state.fresh = False
                state.barrier += 1
        for command_id, item in list(self.pending.items()):
            elapsed = now - item["start"]
            if elapsed >= 10:
                with self.db:
                    self.db.execute(
                        "UPDATE commands SET result='TIMEOUT',reason='ACK_TIMEOUT' WHERE command_id=? AND result='PENDING'",
                        (command_id,),
                    )
                self.pending.pop(command_id, None)
            elif elapsed >= 2 and not item["checked"]:
                item["checked"] = True
                row = self.command_view(command_id)
                view = next(v for v in self.node_views() if v["node_id"] == row["node_id"])
                if (
                    self.db_ok
                    and item["retry_allowed"]
                    and self.session == item["session"]
                    and self.broker
                    and self.transport.connected
                    and self.queue_ok
                    and view["freshness"] == "FRESH"
                    and view["age_s"] <= 3
                    and self.nodes[row["node_id"]].boot == row["boot_id"]
                ):
                    self._publish(command_id, row["payload"], True)

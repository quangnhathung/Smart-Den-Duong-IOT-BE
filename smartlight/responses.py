"""Public HTTP types. MQTT uint64 fields are represented as decimal strings."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from .models import Telemetry

DecimalUInt64 = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]


class HttpTelemetry(Telemetry):
    seq: DecimalUInt64
    uptime_ms: DecimalUInt64


class HealthResponse(BaseModel):
    status: Literal["OK", "DEGRADED"]
    database: bool
    broker: bool
    queue: bool
    dropped_events: int


class NodeResponse(BaseModel):
    node_id: str
    connection: Literal["ONLINE", "OFFLINE", "UNKNOWN"]
    freshness: Literal["FRESH", "STALE"]
    last_seen: datetime | None
    age_s: float | None
    telemetry: HttpTelemetry | None
    faults: list[str]
    pending_command_id: str | None
    can_command: bool


class HistoryItem(BaseModel):
    id: int
    received_at: datetime
    quality: str
    telemetry: HttpTelemetry


class HistoryPage(BaseModel):
    items: list[HistoryItem]
    total: int
    limit: int
    offset: int


class CommandResponse(BaseModel):
    command_id: str
    node_id: str
    boot_id: str
    payload: dict
    created_at: datetime
    sent_at: datetime | None
    retry_at: datetime | None
    result: Literal["PENDING", "APPLIED", "REJECTED", "TIMEOUT"]
    reason: str | None
    ack_at: datetime | None
    attempts: int


class AlertResponse(BaseModel):
    id: int
    node_id: str
    kind: Literal["OPEN_LOAD", "OFFLINE"]
    started_at: datetime
    ended_at: datetime | None


class AlertPage(BaseModel):
    items: list[AlertResponse]
    total: int
    limit: int
    offset: int


class NodeStats(BaseModel):
    node_id: str
    total_energy_wh: float
    auto_energy_wh: float
    baseline_wh: float
    saving_percent: float | None
    valid_uptime_s: float
    covered_wall_s: float
    missing_wall_s: float
    coverage_percent: float | None
    auto_valid_uptime_s: float
    sample_count: int
    excluded_intervals: dict[str, int]


class StatsResponse(BaseModel):
    from_time: datetime
    to_time: datetime
    clock_basis: dict[str, str]
    nodes: list[NodeStats]
    total_energy_wh: float
    auto_energy_wh: float
    baseline_wh: float
    saving_percent: float | None
    valid_uptime_s: float
    covered_wall_s: float
    missing_wall_s: float
    coverage_percent: float | None


class ExperimentResponse(BaseModel):
    id: int
    name: str
    configuration: dict
    notes: str
    started_at: datetime
    ended_at: datetime | None

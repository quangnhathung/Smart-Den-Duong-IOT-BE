"""Wire validation is strict; JSON numbers retain Python integer precision."""

import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

NODE_IDS = ("node_01", "node_02", "node_03")
PREFIX = "huit/nhom7/smartlight/v1"
UInt64 = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]
Adc = Annotated[int, Field(strict=True, ge=0, le=4095)]
Brightness = Annotated[int, Field(strict=True, ge=0, le=100)]
Finite = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Envelope(StrictModel):
    schema_version: Annotated[int, Field(strict=True, ge=1, le=1)]
    node_id: Literal["node_01", "node_02", "node_03"]
    boot_id: Annotated[str, Field(pattern=r"^[0-9a-fA-F]{8}$")]

    @model_validator(mode="after")
    def normalize_boot(self):
        self.boot_id = self.boot_id.lower()
        return self


class Telemetry(Envelope):
    seq: UInt64
    uptime_ms: UInt64
    mode: Literal["AUTO", "MANUAL"]
    environment: Literal["UNKNOWN", "DAY", "TWILIGHT", "NIGHT"]
    ldr_adc: Adc
    motion: Literal["NONE", "PEDESTRIAN", "VEHICLE", "UNKNOWN"]
    distance_cm: Annotated[float, Field(gt=0, allow_inf_nan=False)] | None
    brightness_target: Brightness
    brightness_applied: Brightness
    current_adc: Adc
    current_ma_sim: Finite
    power_sim_w: Finite
    faults: list[Literal["OPEN_LOAD"]]

    @model_validator(mode="after")
    def unique_faults(self):
        if len(self.faults) != len(set(self.faults)):
            raise ValueError("duplicate fault")
        return self


class Availability(Envelope):
    status: Literal["ONLINE", "OFFLINE"]


class Ack(Envelope):
    command_id: str
    result: Literal["APPLIED", "REJECTED"]
    reason: Literal["INVALID_COMMAND", "BOOT_MISMATCH", "EXPIRED"] | None

    @model_validator(mode="after")
    def check_result(self):
        command_id = UUID(self.command_id)
        if command_id.version != 4:
            raise ValueError("command_id must be UUIDv4")
        self.command_id = str(command_id)
        if (self.result == "APPLIED") != (self.reason is None):
            raise ValueError("invalid result/reason combination")
        return self


class CommandInput(StrictModel):
    mode: Literal["AUTO", "MANUAL"]
    brightness: Brightness | None = None

    @model_validator(mode="after")
    def check_brightness(self):
        if self.mode == "AUTO" and "brightness" in self.model_fields_set:
            raise ValueError("AUTO must not contain brightness")
        if self.mode == "MANUAL" and self.brightness is None:
            raise ValueError("MANUAL requires brightness")
        return self


class ExperimentInput(StrictModel):
    name: Annotated[str, Field(min_length=1, max_length=120)]
    configuration: dict = Field(default_factory=dict)
    notes: Annotated[str, Field(max_length=4000)] = ""

    @model_validator(mode="after")
    def finite_configuration(self):
        json.dumps(self.configuration, allow_nan=False)
        if not self.name.strip():
            raise ValueError("experiment name must not be blank")
        return self


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def parse_wire(topic: str, payload: bytes):
    parts = topic.split("/")
    if len(parts) != 6 or "/".join(parts[:4]) != PREFIX or parts[4] not in NODE_IDS:
        raise ValueError("unexpected MQTT topic")
    kinds = {"telemetry": Telemetry, "availability": Availability, "ack": Ack}
    model = kinds.get(parts[5])
    if model is None or len(payload) > 2048:
        raise ValueError("unexpected topic or oversized payload")
    value = json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=no_duplicate_keys,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
    )
    data = model.model_validate(value)
    if data.node_id != parts[4]:
        raise ValueError("topic/payload node mismatch")
    return parts[5], data


def api_payload(value):
    """Only public HTTP JSON converts wire uint64 values to decimal strings."""
    if isinstance(value, list):
        return [api_payload(item) for item in value]
    if isinstance(value, dict):
        return {
            key: str(item)
            if key in ("seq", "uptime_ms", "deadline_uptime_ms") and item is not None
            else api_payload(item)
            for key, item in value.items()
        }
    return value

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BeforeValidator

from .config import Settings
from .errors import DomainError
from .models import CommandInput, ExperimentInput, api_payload
from .responses import (
    AlertPage,
    CommandResponse,
    ExperimentPage,
    ExperimentResponse,
    HealthResponse,
    HistoryPage,
    NodeResponse,
    StatsResponse,
)
from .service import Service
from .transport import MqttTransport

TIME_KEYS = {
    "last_seen",
    "received_at",
    "created_at",
    "sent_at",
    "retry_at",
    "ack_at",
    "started_at",
    "ended_at",
    "from_time",
    "to_time",
}


def public(value):
    if isinstance(value, list):
        return [public(item) for item in value]
    if isinstance(value, dict):
        return {
            key: datetime.fromtimestamp(item, timezone.utc).isoformat().replace("+00:00", "Z")
            if key in TIME_KEYS and isinstance(item, (int, float))
            else public(item)
            for key, item in api_payload(value).items()
        }
    return value


def require_iso(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("Expected ISO8601 UTC datetime")
    return value


UtcInput = Annotated[datetime, BeforeValidator(require_iso)]


def utc_query(value):
    if value is None:
        return None
    if value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
        raise DomainError(422, "from/to must include UTC timezone (Z or +00:00)")
    return value.timestamp()


def create_app(settings=None, transport=None, clock=None):
    settings = settings or Settings.from_env()
    transport = transport or MqttTransport(settings.mqtt_host, settings.mqtt_port)
    service = Service(settings, transport, clock)

    @asynccontextmanager
    async def lifespan(_app):
        service.start()
        try:
            yield
        finally:
            service.stop()

    app = FastAPI(
        title="Smart Streetlight API",
        version="1.0.0",
        lifespan=lifespan,
        description="MQTT v1 / SRS7/9. uint64 HTTP fields are decimal strings; measurements are simulated.",
    )
    app.state.service = service
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.origins),
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    @app.exception_handler(DomainError)
    async def domain_error(_request, error):
        return JSONResponse(status_code=error.status, content={"detail": error.detail})

    async def call(operation, *args, **kwargs):
        return public(await service.call(operation, *args, **kwargs))

    @app.get("/api/health", response_model=HealthResponse, responses={503: {"model": HealthResponse}})
    async def health():
        value = await call("health")
        value["dropped_events"] = service.dropped
        return JSONResponse(status_code=200 if value["status"] == "OK" else 503, content=value)

    @app.get("/api/nodes", response_model=list[NodeResponse])
    async def nodes():
        return await call("node_views")

    @app.get("/api/nodes/{node_id}/history", response_model=HistoryPage)
    async def history(
        node_id: str,
        start: Annotated[UtcInput | None, Query(alias="from")] = None,
        end: Annotated[UtcInput | None, Query(alias="to")] = None,
        experiment_id: int | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        return await call("reporting.history", node_id, utc_query(start), utc_query(end), experiment_id, limit, offset)

    @app.post("/api/nodes/{node_id}/commands", status_code=202, response_model=CommandResponse)
    async def commands(node_id: str, request: CommandInput):
        return await call("create_command", node_id, request)

    @app.get("/api/commands/{command_id}", response_model=CommandResponse)
    async def command(command_id: str):
        return await call("command_view", command_id)

    @app.get("/api/alerts", response_model=AlertPage)
    async def alerts(
        node_id: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        return await call("reporting.alerts", node_id, limit, offset)

    @app.get("/api/stats", response_model=StatsResponse)
    async def stats(
        node_id: str | None = None,
        start: Annotated[UtcInput | None, Query(alias="from")] = None,
        end: Annotated[UtcInput | None, Query(alias="to")] = None,
        experiment_id: int | None = None,
    ):
        return await call("reporting.stats", node_id, utc_query(start), utc_query(end), experiment_id)

    @app.get("/api/experiments", response_model=ExperimentPage)
    async def list_experiments(
        limit: Annotated[int, Query(ge=1, le=1000)] = 100, offset: Annotated[int, Query(ge=0)] = 0
    ):
        return await call("reporting.experiments", limit, offset)

    @app.get("/api/experiments/{experiment_id}", response_model=ExperimentResponse)
    async def get_experiment(experiment_id: int):
        return await call("reporting.experiment", experiment_id)

    @app.post("/api/experiments", status_code=201, response_model=ExperimentResponse)
    async def experiments(request: ExperimentInput):
        return await call("reporting.start_experiment", request)

    @app.post("/api/experiments/{experiment_id}/stop", response_model=ExperimentResponse)
    async def stop(experiment_id: int):
        return await call("reporting.stop_experiment", experiment_id)

    @app.get("/api/experiments/{experiment_id}/export.csv")
    async def export(experiment_id: int):
        value = await service.call("reporting.export", experiment_id)
        return Response(
            value,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="experiment-{experiment_id}.csv"'},
        )

    return app


app = create_app()

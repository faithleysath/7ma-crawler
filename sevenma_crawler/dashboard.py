from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg.rows import tuple_row
from pydantic import BaseModel

from .config import DashboardSettings


class DashboardPoint(BaseModel):
    id: str
    name: str
    latitude: float
    longitude: float
    radius_m: int


class LatestSweep(BaseModel):
    id: str
    source_namespace: str
    collector_id: str
    logical_slot: str
    started_at: str
    finished_at: str | None
    status: str
    point_count: int
    success_count: int
    failure_count: int


class DashboardSummary(BaseModel):
    current_vehicle_total: int
    danche_total: int
    zhuli_total: int
    latest_sweep_raw_observation_count: int
    latest_sweep_unique_vehicle_count: int
    latest_sweep_point_count: int
    latest_sweep_success_count: int
    latest_sweep_failure_count: int
    latest_observed_at: str | None
    is_stale: bool
    stale_reason: str | None


class SweepHistoryItem(BaseModel):
    sweep_id: str
    logical_slot: str
    status: str
    raw_observation_count: int
    unique_vehicle_count: int
    success_count: int
    failure_count: int


class DashboardTopPoint(BaseModel):
    name: str
    raw_observation_count: int
    unique_vehicle_count: int


class DashboardVehicle(BaseModel):
    vehicle_uid: str
    bucket: str
    number: str | None
    vendor_lock_id: str | None
    battery_name: str | None
    distance_m: float | None
    longitude: float
    latitude: float
    observed_at: str
    point_id: str


class DashboardFailurePoint(BaseModel):
    name: str
    error_type: str
    error_message: str | None
    http_status: int | None
    requested_at: str


class DashboardBootstrapResponse(BaseModel):
    source_namespace: str
    generated_at: str
    summary: DashboardSummary
    latest_sweep: LatestSweep | None
    history: list[SweepHistoryItem]
    failure_points: list[DashboardFailurePoint]
    points: list[DashboardPoint]
    top_points: list[DashboardTopPoint]
    vehicles: list[DashboardVehicle]


class DashboardRepository:
    """Read-only query layer backing the monitoring dashboard."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def fetch_bootstrap(
        self,
        *,
        source_namespace: str,
        vehicle_limit: int,
        stale_after_seconds: int,
    ) -> DashboardBootstrapResponse:
        with psycopg.connect(self._database_url, row_factory=tuple_row) as connection:
            latest_sweep = self._fetch_latest_sweep(connection, source_namespace)
            summary = self._fetch_summary(
                connection,
                source_namespace,
                latest_sweep,
                stale_after_seconds=stale_after_seconds,
            )
            history = self._fetch_history(connection, source_namespace)
            failure_points = (
                self._fetch_failure_points(connection, latest_sweep.id)
                if latest_sweep is not None
                else []
            )
            points = self._fetch_points(connection)
            top_points = (
                self._fetch_top_points(connection, latest_sweep.id)
                if latest_sweep is not None
                else []
            )
            vehicles = self._fetch_vehicles(
                connection,
                source_namespace=source_namespace,
                vehicle_limit=vehicle_limit,
            )
        return DashboardBootstrapResponse(
            source_namespace=source_namespace,
            generated_at=_to_iso_datetime_literal(),
            summary=summary,
            latest_sweep=latest_sweep,
            history=history,
            failure_points=failure_points,
            points=points,
            top_points=top_points,
            vehicles=vehicles,
        )

    def _fetch_latest_sweep(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        source_namespace: str,
    ) -> LatestSweep | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    id,
                    source_namespace,
                    collector_id,
                    logical_slot,
                    started_at,
                    finished_at,
                    status,
                    point_count,
                    success_count,
                    failure_count
                from crawl_sweep
                where source_namespace = %s
                order by started_at desc
                limit 1
                """,
                (source_namespace,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return LatestSweep(
            id=str(row[0]),
            source_namespace=str(row[1]),
            collector_id=str(row[2]),
            logical_slot=_to_iso(row[3]),
            started_at=_to_iso(row[4]),
            finished_at=_to_optional_iso(row[5]),
            status=str(row[6]),
            point_count=int(row[7]),
            success_count=int(row[8]),
            failure_count=int(row[9]),
        )

    def _fetch_summary(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        source_namespace: str,
        latest_sweep: LatestSweep | None,
        *,
        stale_after_seconds: int,
    ) -> DashboardSummary:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    count(*)::int,
                    count(*) filter (where bucket = 'danche')::int,
                    count(*) filter (where bucket = 'zhuli')::int,
                    max(observed_at)
                from vehicle_latest
                where source_namespace = %s
                """,
                (source_namespace,),
            )
            vehicle_row = cursor.fetchone()

        latest_observed_at_raw = None if vehicle_row is None else vehicle_row[3]
        raw_observation_count = 0
        unique_vehicle_count = 0
        point_count = latest_sweep.point_count if latest_sweep is not None else 0
        success_count = latest_sweep.success_count if latest_sweep is not None else 0
        failure_count = latest_sweep.failure_count if latest_sweep is not None else 0

        if latest_sweep is not None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select
                        count(*)::int,
                        count(distinct vehicle_uid)::int
                    from raw_observation
                    where sweep_id = %s
                    """,
                    (latest_sweep.id,),
                )
                sweep_row = cursor.fetchone()
            if sweep_row is not None:
                raw_observation_count = int(sweep_row[0])
                unique_vehicle_count = int(sweep_row[1])

        is_stale, stale_reason = _build_stale_status(
            latest_sweep=latest_sweep,
            latest_observed_at=latest_observed_at_raw,
            stale_after_seconds=stale_after_seconds,
        )

        if vehicle_row is None:
            return DashboardSummary(
                current_vehicle_total=0,
                danche_total=0,
                zhuli_total=0,
                latest_sweep_raw_observation_count=raw_observation_count,
                latest_sweep_unique_vehicle_count=unique_vehicle_count,
                latest_sweep_point_count=point_count,
                latest_sweep_success_count=success_count,
                latest_sweep_failure_count=failure_count,
                latest_observed_at=None,
                is_stale=is_stale,
                stale_reason=stale_reason,
            )

        return DashboardSummary(
            current_vehicle_total=int(vehicle_row[0]),
            danche_total=int(vehicle_row[1]),
            zhuli_total=int(vehicle_row[2]),
            latest_sweep_raw_observation_count=raw_observation_count,
            latest_sweep_unique_vehicle_count=unique_vehicle_count,
            latest_sweep_point_count=point_count,
            latest_sweep_success_count=success_count,
            latest_sweep_failure_count=failure_count,
            latest_observed_at=_to_optional_iso(vehicle_row[3]),
            is_stale=is_stale,
            stale_reason=stale_reason,
        )

    def _fetch_history(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        source_namespace: str,
    ) -> list[SweepHistoryItem]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    s.id,
                    s.logical_slot,
                    s.status,
                    s.success_count,
                    s.failure_count,
                    count(ro.id)::int as raw_observation_count,
                    count(distinct ro.vehicle_uid)::int as unique_vehicle_count
                from crawl_sweep as s
                left join raw_observation as ro on ro.sweep_id = s.id
                where s.source_namespace = %s
                group by s.id
                order by s.started_at desc
                limit 12
                """,
                (source_namespace,),
            )
            rows = cursor.fetchall()
        return [
            SweepHistoryItem(
                sweep_id=str(row[0]),
                logical_slot=_to_iso(row[1]),
                status=str(row[2]),
                success_count=int(row[3]),
                failure_count=int(row[4]),
                raw_observation_count=int(row[5]),
                unique_vehicle_count=int(row[6]),
            )
            for row in rows
        ]

    def _fetch_points(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
    ) -> list[DashboardPoint]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select id, name, latitude, longitude, radius_m
                from crawl_point
                where enabled = true
                order by name
                """
            )
            rows = cursor.fetchall()
        return [
            DashboardPoint(
                id=str(row[0]),
                name=str(row[1]),
                latitude=float(row[2]),
                longitude=float(row[3]),
                radius_m=int(row[4]),
            )
            for row in rows
        ]

    def _fetch_top_points(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        latest_sweep_id: str,
    ) -> list[DashboardTopPoint]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    p.name,
                    count(ro.id)::int as raw_observation_count,
                    count(distinct ro.vehicle_uid)::int as unique_vehicle_count
                from raw_observation as ro
                join crawl_point as p on p.id = ro.point_id
                where ro.sweep_id = %s
                group by p.name
                order by unique_vehicle_count desc, raw_observation_count desc, p.name asc
                limit 8
                """,
                (latest_sweep_id,),
            )
            rows = cursor.fetchall()
        return [
            DashboardTopPoint(
                name=str(row[0]),
                raw_observation_count=int(row[1]),
                unique_vehicle_count=int(row[2]),
            )
            for row in rows
        ]

    def _fetch_failure_points(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        latest_sweep_id: str,
    ) -> list[DashboardFailurePoint]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    p.name,
                    pf.error_type,
                    pf.error_message,
                    pf.http_status,
                    pf.requested_at
                from point_fetch as pf
                join crawl_point as p on p.id = pf.point_id
                where
                    pf.sweep_id = %s
                    and pf.error_type is not null
                order by pf.requested_at desc, p.name asc
                limit 8
                """,
                (latest_sweep_id,),
            )
            rows = cursor.fetchall()
        return [
            DashboardFailurePoint(
                name=str(row[0]),
                error_type=str(row[1]),
                error_message=row[2] if row[2] is None else str(row[2]),
                http_status=row[3] if row[3] is None else int(row[3]),
                requested_at=_to_iso(row[4]),
            )
            for row in rows
        ]

    def _fetch_vehicles(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        *,
        source_namespace: str,
        vehicle_limit: int,
    ) -> list[DashboardVehicle]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    vehicle_uid,
                    bucket,
                    number,
                    vendor_lock_id,
                    battery_name,
                    distance_m,
                    vehicle_longitude,
                    vehicle_latitude,
                    observed_at,
                    point_id
                from vehicle_latest
                where
                    source_namespace = %s
                    and vehicle_longitude is not null
                    and vehicle_latitude is not null
                order by observed_at desc
                limit %s
                """,
                (source_namespace, vehicle_limit),
            )
            rows = cursor.fetchall()
        return [
            DashboardVehicle(
                vehicle_uid=str(row[0]),
                bucket=str(row[1]),
                number=row[2] if row[2] is None else str(row[2]),
                vendor_lock_id=row[3] if row[3] is None else str(row[3]),
                battery_name=row[4] if row[4] is None else str(row[4]),
                distance_m=row[5] if row[5] is None else float(row[5]),
                longitude=float(row[6]),
                latitude=float(row[7]),
                observed_at=_to_iso(row[8]),
                point_id=str(row[9]),
            )
            for row in rows
        ]


def create_dashboard_app(settings: DashboardSettings) -> FastAPI:
    """Create the FastAPI application serving the monitoring dashboard."""

    package_dir = Path(__file__).resolve().parent
    templates = Jinja2Templates(directory=str(package_dir / "templates"))
    repository = DashboardRepository(settings.database_url)

    app = FastAPI(title="7ma crawler dashboard")
    app.mount("/static", StaticFiles(directory=str(package_dir / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    def dashboard_page(request: Request) -> HTMLResponse:  # pyright: ignore[reportUnusedFunction]
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "dashboard_config": {
                    "sourceNamespace": settings.source_namespace,
                    "refreshIntervalSeconds": settings.refresh_interval_seconds,
                    "amapKey": settings.amap_key,
                    "amapSecurityJsCode": settings.amap_security_js_code,
                }
            },
        )

    @app.get("/api/dashboard/bootstrap", response_model=DashboardBootstrapResponse)
    def dashboard_bootstrap(  # pyright: ignore[reportUnusedFunction]
        source_namespace: str | None = None,
    ) -> DashboardBootstrapResponse:
        namespace = source_namespace or settings.source_namespace
        return repository.fetch_bootstrap(
            source_namespace=namespace,
            vehicle_limit=settings.vehicle_limit,
            stale_after_seconds=max(settings.refresh_interval_seconds * 3, 180),
        )

    @app.get("/healthz")
    def healthcheck() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    return app


def serve_dashboard(settings: DashboardSettings) -> None:
    """Serve the monitoring dashboard via Uvicorn."""

    app = create_dashboard_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")


def _to_iso(value: datetime) -> str:
    return value.isoformat()


def _to_optional_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _to_iso(value)


def _to_iso_datetime_literal() -> str:
    return _to_iso(datetime.now(UTC))


def _build_stale_status(
    *,
    latest_sweep: LatestSweep | None,
    latest_observed_at: datetime | None,
    stale_after_seconds: int,
) -> tuple[bool, str | None]:
    if latest_sweep is None:
        return True, "尚未采集到任何批次"

    reference_time = latest_observed_at
    if reference_time is None:
        fallback_iso = latest_sweep.finished_at or latest_sweep.started_at
        reference_time = datetime.fromisoformat(fallback_iso)

    age_seconds = int((datetime.now(UTC) - reference_time).total_seconds())
    if age_seconds > stale_after_seconds:
        return True, f"{age_seconds} 秒未收到新数据"
    return False, None

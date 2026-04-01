from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Final

from curl_cffi import requests

from .api import (
    SevenMateDecodeError,
    SevenMateError,
    SevenMateHTTPError,
    StructuredSurroundingCarData,
    SurroundingCar,
    SurroundingCarResponse,
    fetch_surrounding_cars,
)
from .config import CollectorSettings
from .db import Database
from .fetch_audit import FetchAttemptLogRecord, RawFetchAuditLogger
from .points import CrawlPoint
from .records import (
    PointFetchRecord,
    RawObservationRecord,
    SweepRecord,
    SweepStatus,
    VehicleBucket,
)

LOGGER: Final = logging.getLogger(__name__)


def floor_to_logical_slot(timestamp: datetime, interval_seconds: int) -> datetime:
    """Round a timestamp down to the nearest logical sweep boundary."""

    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than 0.")

    timestamp_utc = timestamp.astimezone(UTC)
    floored_epoch = int(timestamp_utc.timestamp()) // interval_seconds * interval_seconds
    return datetime.fromtimestamp(floored_epoch, tz=UTC)


def build_vehicle_uid(bucket: VehicleBucket, car: SurroundingCar) -> str:
    """Build a stable vehicle identifier from the best fields the API exposes."""

    if car.vendor_lock_id:
        return f"{bucket}:vendor_lock:{car.vendor_lock_id}"
    if car.id is not None:
        return f"{bucket}:id:{car.id}"
    if car.number:
        return f"{bucket}:number:{car.number}"

    digest_source = json.dumps(
        {
            "api_type": car.api_type,
            "battery_name": car.battery_name,
            "latitude": car.latitude,
            "lock_id": car.lock_id,
            "longitude": car.longitude,
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    digest = hashlib.blake2b(
        digest_source.encode("utf-8"),
        digest_size=8,
    ).hexdigest()
    return f"{bucket}:anon:{digest}"


async def run_sweep(
    *,
    settings: CollectorSettings,
    database: Database,
    points: tuple[CrawlPoint, ...],
    logical_slot: datetime | None = None,
) -> SweepRecord:
    """Run one collector sweep and persist its results."""

    started_at = datetime.now(UTC)
    sweep = SweepRecord(
        id=uuid.uuid7(),
        source_namespace=settings.source_namespace,
        collector_id=settings.collector_id,
        logical_slot=logical_slot
        if logical_slot is not None
        else floor_to_logical_slot(started_at, settings.interval_seconds),
        started_at=started_at,
        point_count=len(points),
    )
    database.insert_sweep(sweep)
    audit_logger = RawFetchAuditLogger(
        log_dir=settings.raw_fetch_log_dir,
        database=database,
    )

    try:
        point_fetches = await _collect_points(
            settings=settings,
            points=points,
            sweep=sweep,
            audit_logger=audit_logger,
        )
        finished_at = datetime.now(UTC)
        success_count = sum(1 for point_fetch in point_fetches if point_fetch.is_success)
        failure_count = len(point_fetches) - success_count
        status: SweepStatus
        if success_count == len(points):
            status = "completed"
        elif success_count == 0:
            status = "failed"
        else:
            status = "partial"
        completed_sweep = replace(
            sweep,
            finished_at=finished_at,
            success_count=success_count,
            failure_count=failure_count,
            status=status,
        )
        database.finalize_sweep(completed_sweep, point_fetches)
        LOGGER.info(
            "sweep completed id=%s slot=%s success=%s failure=%s observations=%s",
            completed_sweep.id,
            completed_sweep.logical_slot.isoformat(),
            completed_sweep.success_count,
            completed_sweep.failure_count,
            sum(len(point_fetch.observations) for point_fetch in point_fetches),
        )
        return completed_sweep
    except Exception:
        failed_sweep = replace(
            sweep,
            finished_at=datetime.now(UTC),
            success_count=0,
            failure_count=len(points),
            status="failed",
        )
        database.update_sweep_status(failed_sweep)
        raise


async def run_forever(
    *,
    settings: CollectorSettings,
    database: Database,
    points: tuple[CrawlPoint, ...],
) -> None:
    """Continuously run aligned sweeps forever."""

    next_slot = floor_to_logical_slot(datetime.now(UTC), settings.interval_seconds)
    slot_delta = timedelta(seconds=settings.interval_seconds)
    while True:
        now = datetime.now(UTC)
        if now < next_slot:
            await asyncio.sleep((next_slot - now).total_seconds())
        await run_sweep(
            settings=settings,
            database=database,
            points=points,
            logical_slot=next_slot,
        )
        next_slot += slot_delta


async def _collect_points(
    *,
    settings: CollectorSettings,
    points: tuple[CrawlPoint, ...],
    sweep: SweepRecord,
    audit_logger: RawFetchAuditLogger | None = None,
) -> tuple[PointFetchRecord, ...]:
    semaphore = asyncio.Semaphore(settings.concurrency)
    async with requests.AsyncSession(timeout=settings.timeout_seconds) as session:
        tasks = [
            asyncio.create_task(
                _collect_point(
                    point=point,
                    settings=settings,
                    sweep=sweep,
                    session=session,
                    semaphore=semaphore,
                    audit_logger=audit_logger,
                )
            )
            for point in points
        ]
        point_fetches = await asyncio.gather(*tasks)
    return tuple(point_fetches)


async def _collect_point(
    *,
    point: CrawlPoint,
    settings: CollectorSettings,
    sweep: SweepRecord,
    session: requests.AsyncSession,
    semaphore: asyncio.Semaphore,
    audit_logger: RawFetchAuditLogger | None = None,
) -> PointFetchRecord:
    async with semaphore:
        if settings.request_jitter_seconds > 0:
            await asyncio.sleep(random.uniform(0.0, settings.request_jitter_seconds))

        fetch_id = uuid.uuid7()
        requested_at = datetime.now(UTC)
        for attempt in range(1, settings.max_request_attempts + 1):
            attempt_requested_at = datetime.now(UTC)
            attempt_started = monotonic()
            response: SurroundingCarResponse | None = None
            try:
                response = await fetch_surrounding_cars(
                    latitude=point.latitude,
                    longitude=point.longitude,
                    session=session,
                    timeout=settings.timeout_seconds,
                )
                finished_at = datetime.now(UTC)
                latency_ms = int((monotonic() - attempt_started) * 1000)
                observations = _build_observations(
                    response=response,
                    fetch_id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    observed_at=finished_at,
                )
                _write_attempt_audit_log(
                    audit_logger=audit_logger,
                    fetch_id=fetch_id,
                    sweep=sweep,
                    point=point,
                    attempt=attempt,
                    requested_at=attempt_requested_at,
                    finished_at=finished_at,
                    http_status=response.http_status,
                    status_code=response.status_code,
                    trace_id=response.trace_id,
                    error_type=None
                    if response.status_code == 200
                    else "SevenMateBusinessError",
                    error_message=None if response.status_code == 200 else response.message,
                    response_body=response.raw_body,
                )
                if response.status_code == 200:
                    LOGGER.info(
                        "point fetch completed point=%s attempt=%s latency_ms=%s http_status=%s status_code=%s trace_id=%s observations=%s",
                        point.name,
                        attempt,
                        latency_ms,
                        response.http_status,
                        response.status_code,
                        response.trace_id,
                        len(observations),
                    )
                else:
                    LOGGER.warning(
                        "point fetch business error point=%s attempt=%s latency_ms=%s http_status=%s status_code=%s trace_id=%s",
                        point.name,
                        attempt,
                        latency_ms,
                        response.http_status,
                        response.status_code,
                        response.trace_id,
                    )
                return PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    requested_at=requested_at,
                    finished_at=finished_at,
                    http_status=response.http_status,
                    status_code=response.status_code,
                    trace_id=response.trace_id,
                    error_type=None
                    if response.status_code == 200
                    else "SevenMateBusinessError",
                    error_message=None if response.status_code == 200 else response.message,
                    raw_json=_decode_json_or_none(response.raw_body),
                    observations=observations,
                )
            except SevenMateHTTPError as exc:
                finished_at = datetime.now(UTC)
                latency_ms = int((monotonic() - attempt_started) * 1000)
                _write_attempt_audit_log(
                    audit_logger=audit_logger,
                    fetch_id=fetch_id,
                    sweep=sweep,
                    point=point,
                    attempt=attempt,
                    requested_at=attempt_requested_at,
                    finished_at=finished_at,
                    http_status=exc.http_status,
                    status_code=None,
                    trace_id=exc.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    response_body=exc.response_text,
                )
                if _should_retry_http_error(exc) and attempt < settings.max_request_attempts:
                    LOGGER.warning(
                        "point fetch retrying point=%s attempt=%s latency_ms=%s error=%s http_status=%s next_delay_seconds=%.3f",
                        point.name,
                        attempt,
                        latency_ms,
                        type(exc).__name__,
                        exc.http_status,
                        _retry_delay_seconds(
                            base_delay_seconds=settings.retry_backoff_seconds,
                            attempt=attempt,
                        ),
                    )
                    await asyncio.sleep(
                        _retry_delay_seconds(
                            base_delay_seconds=settings.retry_backoff_seconds,
                            attempt=attempt,
                        )
                    )
                    continue

                LOGGER.warning(
                    "point fetch failed point=%s attempt=%s latency_ms=%s error=%s http_status=%s",
                    point.name,
                    attempt,
                    latency_ms,
                    type(exc).__name__,
                    exc.http_status,
                )
                return PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    requested_at=requested_at,
                    finished_at=finished_at,
                    http_status=exc.http_status,
                    status_code=None,
                    trace_id=exc.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    raw_json=_decode_json_or_none(exc.response_text),
                )
            except requests.RequestsError as exc:
                finished_at = datetime.now(UTC)
                latency_ms = int((monotonic() - attempt_started) * 1000)
                _write_attempt_audit_log(
                    audit_logger=audit_logger,
                    fetch_id=fetch_id,
                    sweep=sweep,
                    point=point,
                    attempt=attempt,
                    requested_at=attempt_requested_at,
                    finished_at=finished_at,
                    http_status=None,
                    status_code=None,
                    trace_id=None,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    response_body=None,
                )
                if attempt < settings.max_request_attempts:
                    LOGGER.warning(
                        "point fetch retrying point=%s attempt=%s latency_ms=%s error=%s next_delay_seconds=%.3f",
                        point.name,
                        attempt,
                        latency_ms,
                        type(exc).__name__,
                        _retry_delay_seconds(
                            base_delay_seconds=settings.retry_backoff_seconds,
                            attempt=attempt,
                        ),
                    )
                    await asyncio.sleep(
                        _retry_delay_seconds(
                            base_delay_seconds=settings.retry_backoff_seconds,
                            attempt=attempt,
                        )
                    )
                    continue

                LOGGER.warning(
                    "point fetch failed point=%s attempt=%s latency_ms=%s error=%s",
                    point.name,
                    attempt,
                    latency_ms,
                    type(exc).__name__,
                )
                return PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    requested_at=requested_at,
                    finished_at=finished_at,
                    http_status=None,
                    status_code=None,
                    trace_id=None,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    raw_json=None,
                )
            except SevenMateError as exc:
                finished_at = datetime.now(UTC)
                latency_ms = int((monotonic() - attempt_started) * 1000)
                decode_error = exc if isinstance(exc, SevenMateDecodeError) else None
                _write_attempt_audit_log(
                    audit_logger=audit_logger,
                    fetch_id=fetch_id,
                    sweep=sweep,
                    point=point,
                    attempt=attempt,
                    requested_at=attempt_requested_at,
                    finished_at=finished_at,
                    http_status=None if decode_error is None else decode_error.http_status,
                    status_code=None,
                    trace_id=None if decode_error is None else decode_error.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    response_body=None if decode_error is None else decode_error.response_text,
                )
                LOGGER.warning(
                    "point fetch failed point=%s attempt=%s latency_ms=%s error=%s",
                    point.name,
                    attempt,
                    latency_ms,
                    type(exc).__name__,
                )
                return PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    requested_at=requested_at,
                    finished_at=finished_at,
                    http_status=None if decode_error is None else decode_error.http_status,
                    status_code=None,
                    trace_id=None if decode_error is None else decode_error.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    raw_json=None
                    if decode_error is None
                    else _decode_json_or_none(decode_error.response_text),
                )
            except Exception as exc:
                finished_at = datetime.now(UTC)
                latency_ms = int((monotonic() - attempt_started) * 1000)
                _write_attempt_audit_log(
                    audit_logger=audit_logger,
                    fetch_id=fetch_id,
                    sweep=sweep,
                    point=point,
                    attempt=attempt,
                    requested_at=attempt_requested_at,
                    finished_at=finished_at,
                    http_status=None if response is None else response.http_status,
                    status_code=None if response is None else response.status_code,
                    trace_id=None if response is None else response.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    response_body=None if response is None else response.raw_body,
                )
                LOGGER.exception(
                    "unexpected collector error point=%s attempt=%s latency_ms=%s",
                    point.name,
                    attempt,
                    latency_ms,
                )
                return PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep.id,
                    point=point,
                    requested_at=requested_at,
                    finished_at=finished_at,
                    http_status=None if response is None else response.http_status,
                    status_code=None if response is None else response.status_code,
                    trace_id=None if response is None else response.trace_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    raw_json=None if response is None else _decode_json_or_none(response.raw_body),
                )

        raise AssertionError("collector retry loop exhausted without returning a point fetch record")


def _build_observations(
    *,
    response: SurroundingCarResponse,
    fetch_id: uuid.UUID,
    sweep_id: uuid.UUID,
    point: CrawlPoint,
    observed_at: datetime,
) -> tuple[RawObservationRecord, ...]:
    structured_data = response.data
    if not isinstance(structured_data, StructuredSurroundingCarData):
        return ()

    observations: list[RawObservationRecord] = []
    buckets: tuple[tuple[VehicleBucket, tuple[SurroundingCar, ...]], ...] = (
        ("danche", structured_data.danche.cars),
        ("zhuli", structured_data.zhuli.cars),
    )
    for bucket, cars in buckets:
        for car in cars:
            observations.append(
                RawObservationRecord(
                    id=uuid.uuid7(),
                    fetch_id=fetch_id,
                    sweep_id=sweep_id,
                    point_id=point.id,
                    observed_at=observed_at,
                    bucket=bucket,
                    vehicle_uid=build_vehicle_uid(bucket, car),
                    car_id=car.id,
                    number=car.number,
                    vendor_lock_id=car.vendor_lock_id,
                    carmodel_id=car.carmodel_id,
                    api_type=car.api_type,
                    lock_id=car.lock_id,
                    battery_name=car.battery_name,
                    distance_m=car.distance,
                    vehicle_longitude=_parse_optional_coordinate(car.longitude),
                    vehicle_latitude=_parse_optional_coordinate(car.latitude),
                    raw_vehicle=car.raw_payload or {},
                )
            )
    return tuple(observations)


def _parse_optional_coordinate(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        LOGGER.warning("discarding invalid coordinate value=%r", value)
        return None


def _write_attempt_audit_log(
    *,
    audit_logger: RawFetchAuditLogger | None,
    fetch_id: uuid.UUID,
    sweep: SweepRecord,
    point: CrawlPoint,
    attempt: int,
    requested_at: datetime,
    finished_at: datetime,
    http_status: int | None,
    status_code: int | None,
    trace_id: str | None,
    error_type: str | None,
    error_message: str | None,
    response_body: str | None,
) -> None:
    if audit_logger is None:
        return
    audit_logger.write(
        FetchAttemptLogRecord(
            id=uuid.uuid7(),
            fetch_id=fetch_id,
            sweep_id=sweep.id,
            point_id=point.id,
            point_name=point.name,
            source_namespace=sweep.source_namespace,
            collector_id=sweep.collector_id,
            attempt=attempt,
            requested_at=requested_at,
            finished_at=finished_at,
            request_latitude=point.latitude,
            request_longitude=point.longitude,
            http_status=http_status,
            status_code=status_code,
            trace_id=trace_id,
            error_type=error_type,
            error_message=error_message,
            response_body=response_body,
        )
    )


def _decode_json_or_none(text: str) -> object | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _should_retry_http_error(exc: SevenMateHTTPError) -> bool:
    return exc.http_status >= 500


def _retry_delay_seconds(*, base_delay_seconds: float, attempt: int) -> float:
    return base_delay_seconds * (2 ** (attempt - 1))

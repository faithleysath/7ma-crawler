from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from .points import CrawlPoint

SweepStatus = Literal["running", "completed", "partial", "failed"]
VehicleBucket = Literal["danche", "zhuli"]
JSONMapping = Mapping[str, object]


@dataclass(slots=True, frozen=True)
class RawObservationRecord:
    """One vehicle observation persisted from a point fetch."""

    id: uuid.UUID
    fetch_id: uuid.UUID
    sweep_id: uuid.UUID
    point_id: uuid.UUID
    observed_at: datetime
    bucket: VehicleBucket
    vehicle_uid: str
    car_id: int | None
    number: str | None
    vendor_lock_id: str | None
    carmodel_id: int | None
    api_type: int | None
    lock_id: str | None
    battery_name: str | None
    distance_m: float | None
    vehicle_longitude: float | None
    vehicle_latitude: float | None
    raw_vehicle: JSONMapping


@dataclass(slots=True, frozen=True)
class PointFetchRecord:
    """One concrete HTTP request against a configured point."""

    id: uuid.UUID
    sweep_id: uuid.UUID
    point: CrawlPoint
    requested_at: datetime
    finished_at: datetime
    http_status: int | None
    status_code: int | None
    trace_id: str | None
    error_type: str | None
    error_message: str | None
    raw_json: object | None
    observations: tuple[RawObservationRecord, ...] = field(default_factory=tuple)

    @property
    def is_success(self) -> bool:
        return self.http_status == 200 and self.status_code == 200 and not self.error_type


@dataclass(slots=True, frozen=True)
class SweepRecord:
    """One logical collector run across all configured points."""

    id: uuid.UUID
    source_namespace: str
    collector_id: str
    logical_slot: datetime
    started_at: datetime
    point_count: int
    status: SweepStatus = "running"
    success_count: int = 0
    failure_count: int = 0
    finished_at: datetime | None = None

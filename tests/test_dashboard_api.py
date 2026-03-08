from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from sevenma_crawler.config import DashboardSettings
from sevenma_crawler.dashboard import create_dashboard_app
from sevenma_crawler.db import Database
from sevenma_crawler.points import CrawlPoint
from sevenma_crawler.records import PointFetchRecord, RawObservationRecord, SweepRecord


@pytest.mark.integration
def test_dashboard_bootstrap_returns_latest_vehicle_summary(test_database_url: str) -> None:
    now = datetime.now(UTC)
    logical_slot = now.replace(second=0, microsecond=0)
    point = CrawlPoint(
        id=uuid.uuid4(),
        name="nuidt-001",
        latitude=32.2021,
        longitude=118.7151,
    )
    sweep_id = uuid.uuid4()
    fetch_id = uuid.uuid4()
    with Database(test_database_url) as database:
        database.ensure_schema()
        database.upsert_points((point,))
        database.insert_sweep(
            SweepRecord(
                id=sweep_id,
                source_namespace="test",
                collector_id="collector-a",
                logical_slot=logical_slot,
                started_at=now - timedelta(seconds=40),
                point_count=1,
            )
        )
        database.finalize_sweep(
            SweepRecord(
                id=sweep_id,
                source_namespace="test",
                collector_id="collector-a",
                logical_slot=logical_slot,
                started_at=now - timedelta(seconds=40),
                point_count=1,
                success_count=1,
                failure_count=0,
                status="completed",
                finished_at=now - timedelta(seconds=10),
            ),
            (
                PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep_id,
                    point=point,
                    requested_at=now - timedelta(seconds=12),
                    finished_at=now - timedelta(seconds=10),
                    http_status=200,
                    status_code=200,
                    trace_id="trace-test",
                    error_type=None,
                    error_message=None,
                    raw_json={"status_code": 200},
                    observations=(
                        RawObservationRecord(
                            id=uuid.uuid4(),
                            fetch_id=fetch_id,
                            sweep_id=sweep_id,
                            point_id=point.id,
                            observed_at=now - timedelta(seconds=10),
                            bucket="danche",
                            vehicle_uid="danche:vendor_lock:LOCK-9",
                            car_id=9,
                            number="D-9",
                            vendor_lock_id="LOCK-9",
                            carmodel_id=1,
                            api_type=7,
                            lock_id="14",
                            battery_name="7500mAH 3.6V",
                            distance_m=8.2,
                            vehicle_longitude=118.7159,
                            vehicle_latitude=32.2029,
                            raw_vehicle={"number": "D-9"},
                        ),
                    ),
                ),
            ),
        )

    app = create_dashboard_app(
        DashboardSettings(
            database_url=test_database_url,
            amap_key="test-key",
            amap_security_js_code="test-sec",
            source_namespace="test",
        )
    )
    with TestClient(app) as client:
        response = client.get("/api/dashboard/bootstrap")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_namespace"] == "test"
    assert payload["summary"]["current_vehicle_total"] == 1
    assert payload["summary"]["danche_total"] == 1
    assert payload["summary"]["zhuli_total"] == 0
    assert payload["summary"]["latest_sweep_success_count"] == 1
    assert payload["summary"]["is_stale"] is False
    assert payload["latest_sweep"]["status"] == "completed"
    assert len(payload["history"]) == 1
    assert payload["failure_points"] == []
    assert payload["top_points"][0]["name"] == "nuidt-001"
    assert payload["vehicles"][0]["vehicle_uid"] == "danche:vendor_lock:LOCK-9"


@pytest.mark.integration
def test_dashboard_bootstrap_reports_stale_and_failure_points(test_database_url: str) -> None:
    now = datetime.now(UTC)
    logical_slot = now.replace(second=0, microsecond=0) - timedelta(minutes=10)
    point = CrawlPoint(
        id=uuid.uuid4(),
        name="nuidt-002",
        latitude=32.2022,
        longitude=118.7152,
    )
    sweep_id = uuid.uuid4()
    fetch_id = uuid.uuid4()
    with Database(test_database_url) as database:
        database.ensure_schema()
        database.upsert_points((point,))
        database.insert_sweep(
            SweepRecord(
                id=sweep_id,
                source_namespace="test",
                collector_id="collector-a",
                logical_slot=logical_slot,
                started_at=now - timedelta(minutes=11),
                point_count=1,
            )
        )
        database.finalize_sweep(
            SweepRecord(
                id=sweep_id,
                source_namespace="test",
                collector_id="collector-a",
                logical_slot=logical_slot,
                started_at=now - timedelta(minutes=11),
                point_count=1,
                success_count=0,
                failure_count=1,
                status="failed",
                finished_at=now - timedelta(minutes=10),
            ),
            (
                PointFetchRecord(
                    id=fetch_id,
                    sweep_id=sweep_id,
                    point=point,
                    requested_at=now - timedelta(minutes=10, seconds=5),
                    finished_at=now - timedelta(minutes=10, seconds=4),
                    http_status=503,
                    status_code=None,
                    trace_id=None,
                    error_type="SevenMateHTTPError",
                    error_message="7mate API returned HTTP 503.",
                    raw_json={"detail": "upstream unavailable"},
                    observations=(),
                ),
            ),
        )

    app = create_dashboard_app(
        DashboardSettings(
            database_url=test_database_url,
            amap_key="test-key",
            amap_security_js_code="test-sec",
            source_namespace="test",
        )
    )
    with TestClient(app) as client:
        response = client.get("/api/dashboard/bootstrap")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["is_stale"] is True
    assert "未收到新数据" in payload["summary"]["stale_reason"]
    assert payload["summary"]["latest_sweep_failure_count"] == 1
    assert payload["failure_points"][0]["name"] == "nuidt-002"
    assert payload["failure_points"][0]["error_type"] == "SevenMateHTTPError"

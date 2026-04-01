from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime

import psycopg
import pytest

from sevenma_crawler.db import Database, Migration
from sevenma_crawler.fetch_audit import FetchAttemptLogRecord
from sevenma_crawler.points import CrawlPoint
from sevenma_crawler.records import PointFetchRecord, RawObservationRecord, SweepRecord


@pytest.mark.integration
def test_database_ensure_schema_serializes_concurrent_migrations(
    test_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = Migration(
        version="9001",
        name="9001_lock_probe.sql",
        sql="""
        select pg_sleep(0.25);
        create table if not exists migration_lock_probe (
            id integer primary key
        )
        """,
    )
    monkeypatch.setattr("sevenma_crawler.db._load_migrations", lambda: (migration,))

    start_barrier = threading.Barrier(3)
    failures: list[BaseException] = []

    def run_ensure_schema() -> None:
        try:
            with Database(test_database_url) as database:
                start_barrier.wait(timeout=5)
                database.ensure_schema()
        except BaseException as exc:
            failures.append(exc)

    threads = [
        threading.Thread(target=run_ensure_schema, name=f"migrate-{index}")
        for index in range(2)
    ]
    for thread in threads:
        thread.start()

    start_barrier.wait(timeout=5)

    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert failures == []

    with psycopg.connect(test_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from schema_migration where version = %s",
                ("9001",),
            )
            migration_count = int(cursor.fetchone()[0])

            cursor.execute("select to_regclass('public.migration_lock_probe')")
            probe_table = cursor.fetchone()[0]

    assert migration_count == 1
    assert probe_table == "migration_lock_probe"


@pytest.mark.integration
def test_database_finalize_sweep_persists_latest_vehicle_state(test_database_url: str) -> None:
    point = CrawlPoint(
        id=uuid.uuid4(),
        name="nuidt-001",
        latitude=32.2021,
        longitude=118.7151,
    )
    with Database(test_database_url) as database:
        database.ensure_schema()
        database.ensure_schema()
        database.upsert_points((point,))

        first_sweep = SweepRecord(
            id=uuid.uuid4(),
            source_namespace="test",
            collector_id="collector-a",
            logical_slot=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
            started_at=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
            point_count=1,
        )
        database.insert_sweep(first_sweep)
        database.insert_fetch_attempt_log(
            _build_fetch_attempt_log(
                sweep_id=first_sweep.id,
                point=point,
                fetch_id=uuid.uuid4(),
            )
        )
        first_fetch_id = uuid.uuid4()
        database.finalize_sweep(
            SweepRecord(
                id=first_sweep.id,
                source_namespace=first_sweep.source_namespace,
                collector_id=first_sweep.collector_id,
                logical_slot=first_sweep.logical_slot,
                started_at=first_sweep.started_at,
                point_count=1,
                success_count=1,
                failure_count=0,
                status="completed",
                finished_at=datetime(2026, 3, 8, 10, 0, 30, tzinfo=UTC),
            ),
            (
                _build_point_fetch(
                    fetch_id=first_fetch_id,
                    point=point,
                    sweep_id=first_sweep.id,
                    requested_at=datetime(2026, 3, 8, 10, 0, 1, tzinfo=UTC),
                    finished_at=datetime(2026, 3, 8, 10, 0, 2, tzinfo=UTC),
                    observations=(
                        _build_observation(
                            fetch_id=first_fetch_id,
                            sweep_id=first_sweep.id,
                            point_id=point.id,
                            fetch_time=datetime(2026, 3, 8, 10, 0, 2, tzinfo=UTC),
                            vehicle_uid="zhuli:vendor_lock:LOCK-1",
                            longitude=118.7151,
                            latitude=32.2021,
                        ),
                    ),
                ),
            ),
        )

        second_sweep = SweepRecord(
            id=uuid.uuid4(),
            source_namespace="test",
            collector_id="collector-a",
            logical_slot=datetime(2026, 3, 8, 10, 1, tzinfo=UTC),
            started_at=datetime(2026, 3, 8, 10, 1, tzinfo=UTC),
            point_count=1,
        )
        database.insert_sweep(second_sweep)
        second_fetch_id = uuid.uuid4()
        database.finalize_sweep(
            SweepRecord(
                id=second_sweep.id,
                source_namespace=second_sweep.source_namespace,
                collector_id=second_sweep.collector_id,
                logical_slot=second_sweep.logical_slot,
                started_at=second_sweep.started_at,
                point_count=1,
                success_count=1,
                failure_count=0,
                status="completed",
                finished_at=datetime(2026, 3, 8, 10, 1, 30, tzinfo=UTC),
            ),
            (
                _build_point_fetch(
                    fetch_id=second_fetch_id,
                    point=point,
                    sweep_id=second_sweep.id,
                    requested_at=datetime(2026, 3, 8, 10, 1, 1, tzinfo=UTC),
                    finished_at=datetime(2026, 3, 8, 10, 1, 2, tzinfo=UTC),
                    observations=(
                        _build_observation(
                            fetch_id=second_fetch_id,
                            sweep_id=second_sweep.id,
                            point_id=point.id,
                            fetch_time=datetime(2026, 3, 8, 10, 1, 2, tzinfo=UTC),
                            vehicle_uid="zhuli:vendor_lock:LOCK-1",
                            longitude=118.7162,
                            latitude=32.2032,
                            battery_name="4824",
                        ),
                    ),
                ),
            ),
        )

    with psycopg.connect(test_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select count(*) from crawl_point")
            point_count = int(cursor.fetchone()[0])

            cursor.execute("select count(*) from point_fetch")
            fetch_count = int(cursor.fetchone()[0])

            cursor.execute("select count(*) from fetch_attempt_log")
            fetch_attempt_log_count = int(cursor.fetchone()[0])

            cursor.execute("select count(*) from schema_migration")
            migration_count = int(cursor.fetchone()[0])

            cursor.execute("select version, name from schema_migration order by version")
            migration_rows = cursor.fetchall()

            cursor.execute(
                """
                select vehicle_longitude, vehicle_latitude, battery_name, logical_slot
                from vehicle_latest
                where source_namespace = %s and vehicle_uid = %s
                """,
                ("test", "zhuli:vendor_lock:LOCK-1"),
            )
            latest_row = cursor.fetchone()

    assert point_count == 1
    assert fetch_count == 2
    assert fetch_attempt_log_count == 1
    assert migration_count == 2
    assert migration_rows == [
        ("0001", "0001_initial.sql"),
        ("0002", "0002_fetch_attempt_log.sql"),
    ]
    assert latest_row is not None
    assert float(latest_row[0]) == pytest.approx(118.7162)
    assert float(latest_row[1]) == pytest.approx(32.2032)
    assert str(latest_row[2]) == "4824"
    assert latest_row[3] == datetime(2026, 3, 8, 10, 1, tzinfo=UTC)


def _build_point_fetch(
    *,
    fetch_id: uuid.UUID,
    point: CrawlPoint,
    sweep_id: uuid.UUID,
    requested_at: datetime,
    finished_at: datetime,
    observations: tuple[RawObservationRecord, ...],
) -> PointFetchRecord:
    return PointFetchRecord(
        id=fetch_id,
        sweep_id=sweep_id,
        point=point,
        requested_at=requested_at,
        finished_at=finished_at,
        http_status=200,
        status_code=200,
        trace_id="trace-test",
        error_type=None,
        error_message=None,
        raw_json={"status_code": 200},
        observations=observations,
    )


def _build_fetch_attempt_log(
    *,
    sweep_id: uuid.UUID,
    point: CrawlPoint,
    fetch_id: uuid.UUID,
) -> FetchAttemptLogRecord:
    return FetchAttemptLogRecord(
        id=uuid.uuid4(),
        fetch_id=fetch_id,
        sweep_id=sweep_id,
        point_id=point.id,
        point_name=point.name,
        source_namespace="test",
        collector_id="collector-a",
        attempt=1,
        requested_at=datetime(2026, 3, 8, 9, 59, tzinfo=UTC),
        finished_at=datetime(2026, 3, 8, 9, 59, 2, tzinfo=UTC),
        request_latitude=point.latitude,
        request_longitude=point.longitude,
        http_status=200,
        status_code=200,
        trace_id="trace-audit",
        error_type=None,
        error_message=None,
        response_body='{"status_code":200,"message":"ok"}',
    )


def _build_observation(
    *,
    fetch_id: uuid.UUID,
    sweep_id: uuid.UUID,
    point_id: uuid.UUID,
    fetch_time: datetime,
    vehicle_uid: str,
    longitude: float,
    latitude: float,
    battery_name: str = "7500mAH 3.6V",
) -> RawObservationRecord:
    return RawObservationRecord(
        id=uuid.uuid4(),
        fetch_id=fetch_id,
        sweep_id=sweep_id,
        point_id=point_id,
        observed_at=fetch_time,
        bucket="zhuli",
        vehicle_uid=vehicle_uid,
        car_id=101,
        number="A-1",
        vendor_lock_id="LOCK-1",
        carmodel_id=2,
        api_type=7,
        lock_id="11",
        battery_name=battery_name,
        distance_m=12.5,
        vehicle_longitude=longitude,
        vehicle_latitude=latitude,
        raw_vehicle={"vehicle_uid": vehicle_uid},
    )

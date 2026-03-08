from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from typing import Any, cast

import psycopg
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from .points import CrawlPoint
from .records import PointFetchRecord, SweepRecord


@dataclass(slots=True, frozen=True)
class Migration:
    """One versioned SQL migration bundled with the project."""

    version: str
    name: str
    sql: str


class Database:
    """Thin PostgreSQL wrapper for schema management and collector writes."""

    def __init__(self, database_url: str) -> None:
        self._connection: psycopg.Connection[tuple[Any, ...]] = psycopg.connect(
            database_url,
            row_factory=tuple_row,
        )

    def close(self) -> None:
        """Close the underlying database connection."""

        if not self._connection.closed:
            self._connection.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def ensure_schema(self) -> None:
        """Apply bundled SQL migrations in order."""

        self._ensure_migration_table()
        applied_versions = self._load_applied_migration_versions()
        for migration in _load_migrations():
            if migration.version in applied_versions:
                continue
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.execute(cast(Any, migration.sql))
                    cursor.execute(
                        """
                        insert into schema_migration (version, name)
                        values (%s, %s)
                        """,
                        (migration.version, migration.name),
                    )

    def upsert_points(self, points: Sequence[CrawlPoint]) -> None:
        """Insert or update configured crawl points."""

        rows = [
            (point.id, point.name, point.latitude, point.longitude, point.radius_m)
            for point in points
        ]
        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.executemany(
                    """
                    insert into crawl_point (
                        id,
                        name,
                        latitude,
                        longitude,
                        radius_m
                    )
                    values (%s, %s, %s, %s, %s)
                    on conflict (id) do update
                    set
                        name = excluded.name,
                        latitude = excluded.latitude,
                        longitude = excluded.longitude,
                        radius_m = excluded.radius_m,
                        enabled = true
                    """,
                    rows,
                )

    def insert_sweep(self, sweep: SweepRecord) -> None:
        """Persist a new running sweep before network requests start."""

        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into crawl_sweep (
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
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sweep.id,
                        sweep.source_namespace,
                        sweep.collector_id,
                        sweep.logical_slot,
                        sweep.started_at,
                        sweep.finished_at,
                        sweep.status,
                        sweep.point_count,
                        sweep.success_count,
                        sweep.failure_count,
                    ),
                )

    def finalize_sweep(
        self,
        sweep: SweepRecord,
        point_fetches: Sequence[PointFetchRecord],
    ) -> None:
        """Persist point-fetch rows, raw observations, and sweep completion metadata."""

        point_rows = [
            (
                point_fetch.id,
                point_fetch.sweep_id,
                point_fetch.point.id,
                point_fetch.requested_at,
                point_fetch.finished_at,
                point_fetch.http_status,
                point_fetch.status_code,
                point_fetch.trace_id,
                point_fetch.error_type,
                point_fetch.error_message,
                None if point_fetch.raw_json is None else Jsonb(point_fetch.raw_json),
            )
            for point_fetch in point_fetches
        ]
        observation_rows = [
            (
                observation.id,
                observation.fetch_id,
                observation.sweep_id,
                observation.point_id,
                observation.observed_at,
                observation.bucket,
                observation.vehicle_uid,
                observation.car_id,
                observation.number,
                observation.vendor_lock_id,
                observation.carmodel_id,
                observation.api_type,
                observation.lock_id,
                observation.battery_name,
                observation.distance_m,
                observation.vehicle_longitude,
                observation.vehicle_latitude,
                Jsonb(dict(observation.raw_vehicle)),
            )
            for point_fetch in point_fetches
            for observation in point_fetch.observations
        ]
        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                if point_rows:
                    cursor.executemany(
                        """
                        insert into point_fetch (
                            id,
                            sweep_id,
                            point_id,
                            requested_at,
                            finished_at,
                            http_status,
                            status_code,
                            trace_id,
                            error_type,
                            error_message,
                            raw_json
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        point_rows,
                    )
                if observation_rows:
                    cursor.executemany(
                        """
                        insert into raw_observation (
                            id,
                            fetch_id,
                            sweep_id,
                            point_id,
                            observed_at,
                            bucket,
                            vehicle_uid,
                            car_id,
                            number,
                            vendor_lock_id,
                            carmodel_id,
                            api_type,
                            lock_id,
                            battery_name,
                            distance_m,
                            vehicle_longitude,
                            vehicle_latitude,
                            raw_vehicle
                        )
                        values (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        observation_rows,
                    )
                cursor.execute(
                    """
                    update crawl_sweep
                    set
                        finished_at = %s,
                        status = %s,
                        success_count = %s,
                        failure_count = %s
                    where id = %s
                    """,
                    (
                        sweep.finished_at,
                        sweep.status,
                        sweep.success_count,
                        sweep.failure_count,
                        sweep.id,
                    ),
                )

    def update_sweep_status(self, sweep: SweepRecord) -> None:
        """Update only the sweep-level metadata."""

        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    update crawl_sweep
                    set
                        finished_at = %s,
                        status = %s,
                        success_count = %s,
                        failure_count = %s
                    where id = %s
                    """,
                    (
                        sweep.finished_at,
                        sweep.status,
                        sweep.success_count,
                        sweep.failure_count,
                        sweep.id,
                    ),
                )

    def _ensure_migration_table(self) -> None:
        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    create table if not exists schema_migration (
                        version text primary key,
                        name text not null,
                        applied_at timestamptz not null default now()
                    )
                    """
                )

    def _load_applied_migration_versions(self) -> set[str]:
        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.execute("select version from schema_migration")
                rows = cursor.fetchall()
        return {str(row[0]) for row in rows}


def _load_migrations() -> tuple[Migration, ...]:
    migrations_dir = resources.files("sevenma_crawler").joinpath("migrations")
    migrations = sorted(
        (
            _build_migration(resource.name, resource.read_text(encoding="utf-8"))
            for resource in migrations_dir.iterdir()
            if resource.is_file() and resource.name.endswith(".sql")
        ),
        key=lambda migration: migration.version,
    )
    if not migrations:
        raise RuntimeError("No SQL migrations were found in sevenma_crawler/migrations.")
    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise RuntimeError("Duplicate migration versions detected.")
    return tuple(migrations)


def _build_migration(filename: str, sql_text: str) -> Migration:
    version, separator, _name = filename.partition("_")
    if not separator or not version:
        raise RuntimeError(
            f"Invalid migration filename {filename!r}; expected '<version>_<name>.sql'."
        )
    return Migration(version=version, name=filename, sql=sql_text)

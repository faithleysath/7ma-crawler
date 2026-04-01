from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from .collector import run_forever, run_sweep
from .config import (
    CollectorSettings,
    DashboardSettings,
    build_default_collector_id,
    default_points_file,
    default_raw_fetch_log_dir,
)
from .dashboard import serve_dashboard
from .db import Database
from .points import load_points


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(description="7mate collector and database tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_db = subparsers.add_parser("prepare-db", help="create schema and seed points")
    _add_database_argument(prepare_db)
    prepare_db.add_argument(
        "--points-file",
        type=str,
        default=str(default_points_file()),
        help="JSON file containing the crawl points",
    )

    migrate_db = subparsers.add_parser("migrate-db", help="apply pending schema migrations")
    _add_database_argument(migrate_db)

    run_once = subparsers.add_parser("run-once", help="run one full sweep")
    _add_run_arguments(run_once)

    run_forever_parser = subparsers.add_parser(
        "run-forever",
        help="run aligned sweeps forever",
    )
    _add_run_arguments(run_forever_parser)

    serve_dashboard_parser = subparsers.add_parser(
        "serve-dashboard",
        help="serve the monitoring dashboard",
    )
    _add_dashboard_arguments(serve_dashboard_parser)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    command = cast(str, args.command)
    try:
        if command == "prepare-db":
            _prepare_db(args)
            return 0
        if command == "migrate-db":
            _migrate_db(args)
            return 0
        if command == "run-once":
            settings = _build_settings(args)
            asyncio.run(_run_once(settings))
            return 0
        if command == "run-forever":
            settings = _build_settings(args)
            asyncio.run(_run_forever(settings))
            return 0
        if command == "serve-dashboard":
            dashboard_settings = _build_dashboard_settings(args)
            serve_dashboard(dashboard_settings)
            return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        parser.exit(status=1, message=f"{type(exc).__name__}: {exc}\n")

    parser.exit(status=2, message=f"unsupported command: {command}\n")
    return 2


def _prepare_db(args: argparse.Namespace) -> None:
    database_url = _resolve_database_url(cast(str | None, args.database_url))
    points_file = Path(cast(str, args.points_file))
    points = load_points(points_file)
    with Database(database_url) as database:
        database.ensure_schema()
        database.upsert_points(points)


def _migrate_db(args: argparse.Namespace) -> None:
    database_url = _resolve_database_url(cast(str | None, args.database_url))
    with Database(database_url) as database:
        database.ensure_schema()


async def _run_once(settings: CollectorSettings) -> None:
    points = load_points(settings.points_file)
    with Database(settings.database_url) as database:
        database.ensure_schema()
        database.upsert_points(points)
        await run_sweep(settings=settings, database=database, points=points)


async def _run_forever(settings: CollectorSettings) -> None:
    points = load_points(settings.points_file)
    with Database(settings.database_url) as database:
        database.ensure_schema()
        database.upsert_points(points)
        await run_forever(settings=settings, database=database, points=points)


def _build_settings(args: argparse.Namespace) -> CollectorSettings:
    return CollectorSettings(
        database_url=_resolve_database_url(cast(str | None, args.database_url)),
        points_file=Path(cast(str, args.points_file)),
        raw_fetch_log_dir=Path(cast(str, args.raw_fetch_log_dir)),
        source_namespace=cast(str, args.source_namespace),
        collector_id=cast(str, args.collector_id),
        interval_seconds=cast(int, args.interval_seconds),
        concurrency=cast(int, args.concurrency),
        timeout_seconds=cast(float, args.timeout_seconds),
        request_jitter_seconds=cast(float, args.request_jitter_seconds),
        max_request_attempts=cast(int, args.max_request_attempts),
        retry_backoff_seconds=cast(float, args.retry_backoff_seconds),
    ).validate()


def _add_database_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--database-url",
        type=str,
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection string; defaults to DATABASE_URL",
    )


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    _add_database_argument(parser)
    parser.add_argument(
        "--points-file",
        type=str,
        default=str(default_points_file()),
        help="JSON file containing the crawl points",
    )
    parser.add_argument(
        "--source-namespace",
        type=str,
        default=os.getenv("SEVENMA_SOURCE_NAMESPACE", "local"),
        help="source namespace stored in crawl_sweep",
    )
    parser.add_argument(
        "--collector-id",
        type=str,
        default=os.getenv("SEVENMA_COLLECTOR_ID", build_default_collector_id()),
        help="collector identifier stored in crawl_sweep",
    )
    parser.add_argument(
        "--raw-fetch-log-dir",
        type=str,
        default=os.getenv("SEVENMA_RAW_FETCH_LOG_DIR", str(default_raw_fetch_log_dir())),
        help="directory for append-only raw fetch attempt JSONL logs",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("SEVENMA_INTERVAL_SECONDS", "60")),
        help="logical sweep interval in seconds",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("SEVENMA_CONCURRENCY", "8")),
        help="maximum concurrent point requests",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(os.getenv("SEVENMA_TIMEOUT_SECONDS", "10")),
        help="per-request timeout in seconds",
    )
    parser.add_argument(
        "--request-jitter-seconds",
        type=float,
        default=float(os.getenv("SEVENMA_REQUEST_JITTER_SECONDS", "0.35")),
        help="random delay added before each point request",
    )
    parser.add_argument(
        "--max-request-attempts",
        type=int,
        default=int(os.getenv("SEVENMA_MAX_REQUEST_ATTEMPTS", "3")),
        help="maximum attempts per point for retryable failures",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=float(os.getenv("SEVENMA_RETRY_BACKOFF_SECONDS", "0.5")),
        help="base delay before retrying a retryable point request",
    )


def _add_dashboard_arguments(parser: argparse.ArgumentParser) -> None:
    _add_database_argument(parser)
    parser.add_argument(
        "--source-namespace",
        type=str,
        default=os.getenv("SEVENMA_DASHBOARD_SOURCE_NAMESPACE", "local-dev"),
        help="source namespace displayed by the dashboard",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("SEVENMA_DASHBOARD_HOST", "127.0.0.1"),
        help="dashboard bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SEVENMA_DASHBOARD_PORT", "8000")),
        help="dashboard bind port",
    )
    parser.add_argument(
        "--refresh-interval-seconds",
        type=int,
        default=int(os.getenv("SEVENMA_DASHBOARD_REFRESH_SECONDS", "20")),
        help="frontend polling interval in seconds",
    )
    parser.add_argument(
        "--vehicle-limit",
        type=int,
        default=int(os.getenv("SEVENMA_DASHBOARD_VEHICLE_LIMIT", "1500")),
        help="maximum latest vehicles returned to the map",
    )
    parser.add_argument(
        "--amap-key",
        type=str,
        default=os.getenv("AMAP_WEB_KEY"),
        help="Amap Web JS API key",
    )
    parser.add_argument(
        "--amap-security-js-code",
        type=str,
        default=os.getenv("AMAP_SECURITY_JS_CODE"),
        help="Amap JS security code",
    )


def _build_dashboard_settings(args: argparse.Namespace) -> DashboardSettings:
    return DashboardSettings(
        database_url=_resolve_database_url(cast(str | None, args.database_url)),
        amap_key=_resolve_required_value(cast(str | None, args.amap_key), "amap_key"),
        amap_security_js_code=_resolve_required_value(
            cast(str | None, args.amap_security_js_code),
            "amap_security_js_code",
        ),
        source_namespace=cast(str, args.source_namespace),
        host=cast(str, args.host),
        port=cast(int, args.port),
        refresh_interval_seconds=cast(int, args.refresh_interval_seconds),
        vehicle_limit=cast(int, args.vehicle_limit),
    ).validate()


def _resolve_database_url(value: str | None) -> str:
    if value:
        return value
    raise ValueError("database_url is required. Pass --database-url or set DATABASE_URL.")


def _resolve_required_value(value: str | None, field_name: str) -> str:
    if value:
        return value
    raise ValueError(f"{field_name} is required.")

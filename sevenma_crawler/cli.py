from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from .collector import run_forever, run_sweep
from .config import CollectorSettings, build_default_collector_id, default_points_file
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

    run_once = subparsers.add_parser("run-once", help="run one full sweep")
    _add_run_arguments(run_once)

    run_forever_parser = subparsers.add_parser(
        "run-forever",
        help="run aligned sweeps forever",
    )
    _add_run_arguments(run_forever_parser)

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
        if command == "run-once":
            settings = _build_settings(args)
            asyncio.run(_run_once(settings))
            return 0
        if command == "run-forever":
            settings = _build_settings(args)
            asyncio.run(_run_forever(settings))
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
        source_namespace=cast(str, args.source_namespace),
        collector_id=cast(str, args.collector_id),
        interval_seconds=cast(int, args.interval_seconds),
        concurrency=cast(int, args.concurrency),
        timeout_seconds=cast(float, args.timeout_seconds),
        request_jitter_seconds=cast(float, args.request_jitter_seconds),
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


def _resolve_database_url(value: str | None) -> str:
    if value:
        return value
    raise ValueError("database_url is required. Pass --database-url or set DATABASE_URL.")

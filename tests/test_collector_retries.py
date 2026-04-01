import asyncio
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from curl_cffi import requests

from sevenma_crawler.api import (
    ListSurroundingCarData,
    SevenMateHTTPError,
    SurroundingCarResponse,
)
from sevenma_crawler.collector import _collect_point
from sevenma_crawler.config import CollectorSettings
from sevenma_crawler.points import CrawlPoint
from sevenma_crawler.records import PointFetchRecord, SweepRecord


def test_collect_point_retries_http_5xx_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleep_delays: list[float] = []

    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SevenMateHTTPError(503, '{"detail":"temporary outage"}')
        return _success_response()

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )
    monkeypatch.setattr("sevenma_crawler.collector.asyncio.sleep", fake_sleep)

    record = asyncio.run(
        _run_collect_point(
            max_request_attempts=3,
            retry_backoff_seconds=0.25,
        )
    )

    assert attempts == 2
    assert sleep_delays == [0.25]
    assert record.is_success is True


def test_collect_point_does_not_retry_http_4xx(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        nonlocal attempts
        attempts += 1
        raise SevenMateHTTPError(404, '{"detail":"not found"}')

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )

    record = asyncio.run(_run_collect_point(max_request_attempts=3))

    assert attempts == 1
    assert record.error_type == "SevenMateHTTPError"
    assert record.http_status == 404


def test_collect_point_retries_network_errors_until_attempt_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleep_delays: list[float] = []

    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        nonlocal attempts
        attempts += 1
        raise requests.RequestsError("connection reset")

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )
    monkeypatch.setattr("sevenma_crawler.collector.asyncio.sleep", fake_sleep)

    record = asyncio.run(
        _run_collect_point(
            max_request_attempts=3,
            retry_backoff_seconds=0.2,
        )
    )

    assert attempts == 3
    assert sleep_delays == [0.2, 0.4]
    assert record.error_type == "RequestException"
    assert record.error_message == "connection reset"


def _success_response() -> SurroundingCarResponse:
    return SurroundingCarResponse(
        http_status=200,
        status_code=200,
        message="ok",
        data=ListSurroundingCarData(items=()),
        extra="",
        trace_id="trace-ok",
        raw_body=json.dumps(
            {
                "status_code": 200,
                "message": "ok",
                "data": [],
                "extra": "",
            }
        ),
    )


async def _run_collect_point(
    *,
    max_request_attempts: int,
    retry_backoff_seconds: float = 0.5,
) -> PointFetchRecord:
    return await _collect_point(
        point=CrawlPoint(
            id=uuid.uuid4(),
            name="nuidt-test",
            latitude=32.202,
            longitude=118.715,
        ),
        settings=CollectorSettings(
            database_url="postgresql://example",
            points_file=Path("points.json"),
            raw_fetch_log_dir=Path("raw-fetch-logs"),
            source_namespace="test",
            collector_id="collector-test",
            request_jitter_seconds=0,
            max_request_attempts=max_request_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        ),
        sweep=SweepRecord(
            id=uuid.uuid4(),
            source_namespace="test",
            collector_id="collector-test",
            logical_slot=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
            started_at=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
            point_count=1,
        ),
        session=cast(Any, object()),
        semaphore=asyncio.Semaphore(1),
    )

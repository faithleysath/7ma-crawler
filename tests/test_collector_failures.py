import asyncio
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from sevenma_crawler.api import (
    ListSurroundingCarData,
    SevenMateHTTPError,
    SurroundingCarResponse,
)
from sevenma_crawler.collector import _collect_point
from sevenma_crawler.config import CollectorSettings
from sevenma_crawler.points import CrawlPoint
from sevenma_crawler.records import PointFetchRecord, SweepRecord


def test_collect_point_records_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        raise SevenMateHTTPError(503, '{"detail":"upstream unavailable"}')

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )

    record = asyncio.run(_run_collect_point())

    assert record.http_status == 503
    assert record.error_type == "SevenMateHTTPError"
    assert record.raw_json == {"detail": "upstream unavailable"}
    assert record.observations == ()


def test_collect_point_records_business_error_without_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        return SurroundingCarResponse(
            http_status=200,
            status_code=5001,
            message="busy",
            data=ListSurroundingCarData(items=()),
            extra="",
            trace_id="trace-1",
            raw_body=json.dumps(
                {
                    "status_code": 5001,
                    "message": "busy",
                    "data": [],
                    "extra": "",
                }
            ),
        )

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )

    record = asyncio.run(_run_collect_point())

    assert record.http_status == 200
    assert record.status_code == 5001
    assert record.error_type == "SevenMateBusinessError"
    assert record.error_message == "busy"
    assert record.observations == ()


def test_collect_point_records_unexpected_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_surrounding_cars(**_kwargs: object) -> SurroundingCarResponse:
        raise RuntimeError("socket closed")

    monkeypatch.setattr(
        "sevenma_crawler.collector.fetch_surrounding_cars",
        fake_fetch_surrounding_cars,
    )

    record = asyncio.run(_run_collect_point())

    assert record.http_status is None
    assert record.status_code is None
    assert record.error_type == "RuntimeError"
    assert record.error_message == "socket closed"
    assert record.raw_json is None


async def _run_collect_point() -> PointFetchRecord:
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
            source_namespace="test",
            collector_id="collector-test",
            request_jitter_seconds=0,
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

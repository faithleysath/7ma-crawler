import asyncio
import json

import pytest

from sevenma_crawler.api import (
    SevenMateBusinessError,
    SevenMateDecodeError,
    SevenMateHTTPError,
    StructuredSurroundingCarData,
    SurroundingCarResponse,
    fetch_surrounding_cars,
)


class FakeResponse:
    def __init__(self, *, status_code: int, text: str, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self._response = response
        self.closed = False

    async def get(self, _url: str, **_kwargs: object) -> FakeResponse:
        return self._response

    async def close(self) -> None:
        self.closed = True


def test_surrounding_car_response_parses_structured_payload() -> None:
    response = SurroundingCarResponse.from_payload(
        {
            "status_code": 200,
            "message": "ok",
            "extra": "",
            "data": {
                "danche": {
                    "total": 1,
                    "cars": [
                        {
                            "id": 101,
                            "number": "A-1",
                            "longitude": "118.715001",
                            "latitude": "32.202001",
                            "carmodel_id": 1,
                            "vendor_lock_id": "LOCK-101",
                            "api_type": 7,
                            "lock_id": 14,
                            "battery_name": "7500mAH 3.6V",
                            "distance": 23.5,
                        }
                    ],
                },
                "zhuli": {
                    "total": 0,
                    "cars": [],
                },
            },
        },
        http_status=200,
        trace_id="trace-123",
        raw_body="{}",
    )

    assert response.is_success is True
    assert response.has_structured_data is True
    assert isinstance(response.data, StructuredSurroundingCarData)
    assert response.data.danche.total == 1
    assert response.data.danche.cars[0].vendor_lock_id == "LOCK-101"
    assert response.data.danche.cars[0].lock_id == "14"
    assert response.trace_id == "trace-123"


def test_fetch_surrounding_cars_raises_http_error_for_non_200_response() -> None:
    session = FakeSession(FakeResponse(status_code=503, text="upstream unavailable"))

    with pytest.raises(SevenMateHTTPError, match="HTTP 503"):
        asyncio.run(fetch_surrounding_cars(32.2, 118.7, session=session))


def test_fetch_surrounding_cars_raises_decode_error_for_invalid_json() -> None:
    session = FakeSession(FakeResponse(status_code=200, text="not-json"))

    with pytest.raises(SevenMateDecodeError, match="not valid JSON"):
        asyncio.run(fetch_surrounding_cars(32.2, 118.7, session=session))


def test_fetch_surrounding_cars_decode_error_preserves_raw_body() -> None:
    payload = json.dumps(
        {
            "status_code": 200,
            "message": "ok",
            "extra": "",
            "data": {
                "danche": {
                    "total": 1,
                    "cars": [{"lock_id": {"unexpected": "object"}}],
                },
                "zhuli": {
                    "total": 0,
                    "cars": [],
                },
            },
        }
    )
    session = FakeSession(
        FakeResponse(
            status_code=200,
            text=payload,
            headers={"x-trace-id": "trace-decode"},
        )
    )

    with pytest.raises(SevenMateDecodeError, match="lock_id") as exc_info:
        asyncio.run(fetch_surrounding_cars(32.2, 118.7, session=session))

    assert exc_info.value.http_status == 200
    assert exc_info.value.trace_id == "trace-decode"
    assert exc_info.value.response_text == payload


def test_fetch_surrounding_cars_raises_business_error_when_requested() -> None:
    payload = json.dumps(
        {
            "status_code": 5001,
            "message": "busy",
            "extra": "",
            "data": [],
        }
    )
    session = FakeSession(FakeResponse(status_code=200, text=payload))

    with pytest.raises(SevenMateBusinessError, match="status_code=5001"):
        asyncio.run(
            fetch_surrounding_cars(
                32.2,
                118.7,
                session=session,
                raise_for_business_error=True,
            )
        )

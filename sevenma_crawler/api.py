import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, cast

from curl_cffi import requests

SURROUNDING_CAR_URL: Final = "https://newmapi.7mate.cn/api/v1/new/surrounding/car"

DEFAULT_SURROUNDING_CAR_HEADERS: Final[dict[str, str]] = {
    "phone-model": "Mac14,15",
    "xweb_xhr": "1",
    "phone-system": "Android",
    "phone-brand": "apple",
    "client": "Wechat_MiniAPP",
    "x-app-id": "default",
    "app-version": "1.3.165",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 "
        "Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) "
        "NetType/WIFI MiniProgramEnv/Mac MacWechat/WMPF "
        "MacWechat/3.8.7(0x13080712) UnifiedPCMacWechat(0xf2641721) XWEB/18788"
    ),
    "accept": "application/vnd.ws.v1+json",
    "phone-system-version": "Mac OS X 15.7.4 arm64",
    "referer": "https://servicewechat.com/wx9a6a1a8407b04c5d/344/page-frame.html",
}


class SevenMateError(RuntimeError):
    """Base class for 7mate API failures."""


class SevenMateHTTPError(SevenMateError):
    """Raised when the API returns a non-200 HTTP response."""

    def __init__(self, http_status: int, response_text: str) -> None:
        super().__init__(f"7mate API returned HTTP {http_status}.")
        self.http_status = http_status
        self.response_text = response_text


class SevenMateDecodeError(SevenMateError):
    """Raised when the response body cannot be parsed into the expected schema."""


class SevenMateBusinessError(SevenMateError):
    """Raised when the response JSON reports a non-success business status."""

    def __init__(self, response: SurroundingCarResponse) -> None:
        super().__init__(
            f"7mate API returned status_code={response.status_code}: {response.message}"
        )
        self.response = response


@dataclass(slots=True, frozen=True)
class SurroundingCar:
    """One vehicle entry from either the danche or zhuli bucket.

    Observed sample values:
    - danche: `carmodel_id=1`, `vendor_lock_id` like `"BL5300..."`,
      `lock_id="14"`, `battery_name="7500mAH 3.6V"`
    - zhuli: `carmodel_id=2`, `vendor_lock_id` often numeric,
      `lock_id="11"`, `battery_name="4824"`
    """

    id: int | None = None
    number: str | None = None
    longitude: str | None = None
    latitude: str | None = None
    carmodel_id: int | None = None
    vendor_lock_id: str | None = None
    api_type: int | None = None
    lock_id: str | None = None
    battery_name: str | None = None
    distance: float | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> SurroundingCar:
        """Parse a single vehicle entry from the response body."""

        return cls(
            id=_parse_optional_int(payload.get("id"), field_name="car.id"),
            number=_parse_optional_str(payload.get("number"), field_name="car.number"),
            longitude=_parse_optional_str(
                payload.get("longitude"), field_name="car.longitude"
            ),
            latitude=_parse_optional_str(
                payload.get("latitude"), field_name="car.latitude"
            ),
            carmodel_id=_parse_optional_int(
                payload.get("carmodel_id"), field_name="car.carmodel_id"
            ),
            vendor_lock_id=_parse_optional_str(
                payload.get("vendor_lock_id"), field_name="car.vendor_lock_id"
            ),
            api_type=_parse_optional_int(
                payload.get("api_type"), field_name="car.api_type"
            ),
            lock_id=_parse_optional_str(payload.get("lock_id"), field_name="car.lock_id"),
            battery_name=_parse_optional_str(
                payload.get("battery_name"), field_name="car.battery_name"
            ),
            distance=_parse_optional_float(
                payload.get("distance"), field_name="car.distance"
            ),
        )


@dataclass(slots=True, frozen=True)
class SurroundingCarGroup:
    total: int
    cars: tuple[SurroundingCar, ...]

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        field_name: str,
    ) -> SurroundingCarGroup:
        """Parse one vehicle bucket such as danche or zhuli."""

        cars_raw = _require_list(payload.get("cars"), field_name=f"{field_name}.cars")
        cars = tuple(
            SurroundingCar.from_payload(
                _require_mapping(item, field_name=f"{field_name}.cars[{index}]")
            )
            for index, item in enumerate(cars_raw)
        )
        return cls(
            total=_parse_int(payload.get("total"), field_name=f"{field_name}.total"),
            cars=cars,
        )


@dataclass(slots=True, frozen=True)
class StructuredSurroundingCarData:
    """Object-shaped `data` payload returned by the surrounding car endpoint.

    Attributes:
        danche: Pedal-bike bucket.
        zhuli: E-bike bucket.
    """

    danche: SurroundingCarGroup
    zhuli: SurroundingCarGroup

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, object]
    ) -> StructuredSurroundingCarData:
        """Parse the object-shaped data payload."""

        return cls(
            danche=SurroundingCarGroup.from_payload(
                _require_mapping(payload.get("danche"), field_name="data.danche"),
                field_name="data.danche",
            ),
            zhuli=SurroundingCarGroup.from_payload(
                _require_mapping(payload.get("zhuli"), field_name="data.zhuli"),
                field_name="data.zhuli",
            ),
        )


@dataclass(slots=True, frozen=True)
class ListSurroundingCarData:
    items: tuple[object, ...]

    @property
    def is_empty(self) -> bool:
        return not self.items


type SurroundingCarData = StructuredSurroundingCarData | ListSurroundingCarData


@dataclass(slots=True, frozen=True)
class SurroundingCarResponse:
    http_status: int
    status_code: int
    message: str
    data: SurroundingCarData
    extra: str
    trace_id: str | None = None

    @property
    def is_success(self) -> bool:
        return self.http_status == 200 and self.status_code == 200

    @property
    def has_structured_data(self) -> bool:
        return isinstance(self.data, StructuredSurroundingCarData)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object],
        *,
        http_status: int,
        trace_id: str | None = None,
    ) -> SurroundingCarResponse:
        """Parse the full response body plus a small amount of HTTP metadata."""

        return cls(
            http_status=http_status,
            status_code=_parse_int(payload.get("status_code"), field_name="status_code"),
            message=_parse_str(payload.get("message"), field_name="message"),
            data=_parse_surrounding_car_data(payload.get("data")),
            extra=_parse_optional_str(payload.get("extra"), field_name="extra") or "",
            trace_id=trace_id,
        )


def build_surrounding_car_headers(
    overrides: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the default header set used by the mini-program endpoint."""

    headers = dict(DEFAULT_SURROUNDING_CAR_HEADERS)
    if overrides:
        headers.update(overrides)
    return headers


async def fetch_surrounding_cars(
    latitude: float,
    longitude: float,
    *,
    session: requests.AsyncSession | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 10.0,
    raise_for_business_error: bool = False,
) -> SurroundingCarResponse:
    """Fetch nearby vehicles from the 7mate surrounding car endpoint.

    Args:
        latitude: Query latitude.
        longitude: Query longitude.
        session: Optional shared async session for connection reuse.
        headers: Optional header overrides merged on top of the default headers.
        timeout: Request timeout in seconds when creating an internal session.
        raise_for_business_error: Raise `SevenMateBusinessError` if the JSON
            payload reports `status_code != 200`.

    Returns:
        A parsed response object with typed vehicle buckets and HTTP metadata.

    Raises:
        SevenMateHTTPError: The server returned a non-200 HTTP status.
        SevenMateDecodeError: The response body was not valid JSON or did not
            match the expected schema.
        SevenMateBusinessError: `raise_for_business_error=True` and the JSON
            body did not report success.
    """

    request_headers = build_surrounding_car_headers(headers)
    owns_session = session is None
    client = session or requests.AsyncSession(timeout=timeout)

    try:
        response = await client.get(
            SURROUNDING_CAR_URL,
            params={"latitude": latitude, "longitude": longitude},
            headers=request_headers,
        )
    finally:
        if owns_session:
            await client.close()

    if response.status_code != 200:
        raise SevenMateHTTPError(response.status_code, response.text)

    try:
        payload: object = json.loads(response.text)
    except json.JSONDecodeError as exc:
        raise SevenMateDecodeError("Response body is not valid JSON.") from exc

    parsed = SurroundingCarResponse.from_payload(
        _require_mapping(payload, field_name="response"),
        http_status=response.status_code,
        trace_id=response.headers.get("x-trace-id"),
    )
    if raise_for_business_error and parsed.status_code != 200:
        raise SevenMateBusinessError(parsed)
    return parsed


def _parse_surrounding_car_data(value: object) -> SurroundingCarData:
    if isinstance(value, Mapping):
        return StructuredSurroundingCarData.from_payload(
            cast(Mapping[str, object], value)
        )
    if isinstance(value, list):
        return ListSurroundingCarData(items=tuple(cast(list[object], value)))
    raise SevenMateDecodeError(
        f"data must be an object or list, got {type(value).__name__}."
    )


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SevenMateDecodeError(
            f"{field_name} must be an object, got {type(value).__name__}."
        )
    return cast(Mapping[str, object], value)


def _require_list(value: object, *, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise SevenMateDecodeError(
            f"{field_name} must be a list, got {type(value).__name__}."
        )
    return cast(list[object], value)


def _parse_str(value: object, *, field_name: str) -> str:
    if isinstance(value, str):
        return value
    raise SevenMateDecodeError(
        f"{field_name} must be a string, got {type(value).__name__}."
    )


def _parse_optional_str(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _parse_str(value, field_name=field_name)


def _parse_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise SevenMateDecodeError(f"{field_name} must be an int, got bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise SevenMateDecodeError(
                f"{field_name} must be an int-like string, got {value!r}."
            ) from exc
    raise SevenMateDecodeError(
        f"{field_name} must be an int, got {type(value).__name__}."
    )


def _parse_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    return _parse_int(value, field_name=field_name)


def _parse_optional_float(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise SevenMateDecodeError(f"{field_name} must be a float, got bool.")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise SevenMateDecodeError(
                f"{field_name} must be a float-like string, got {value!r}."
            ) from exc
    raise SevenMateDecodeError(
        f"{field_name} must be a float, got {type(value).__name__}."
    )

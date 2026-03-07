from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

POINT_NAMESPACE: Final = uuid.UUID("d9a57d48-8389-4f17-a5ca-b8144d550f5c")


@dataclass(slots=True, frozen=True)
class CrawlPoint:
    """One configured geo point used to query nearby vehicles."""

    id: uuid.UUID
    name: str
    latitude: float
    longitude: float
    radius_m: int = 100


def build_point_id(*, latitude: float, longitude: float) -> uuid.UUID:
    """Build a deterministic UUID for a point coordinate."""

    point_key = f"{latitude:.6f},{longitude:.6f}"
    return uuid.uuid5(POINT_NAMESPACE, point_key)


def load_points(path: Path) -> tuple[CrawlPoint, ...]:
    """Load point definitions from the project's JSON file."""

    payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON array.")

    points: list[CrawlPoint] = []
    entries = cast(list[object], payload)
    for index, item in enumerate(entries, start=1):
        point_payload = _require_mapping(item, field_name=f"points[{index - 1}]")
        latitude = _parse_float(
            point_payload.get("latitude"), field_name=f"points[{index - 1}].latitude"
        )
        longitude = _parse_float(
            point_payload.get("longitude"), field_name=f"points[{index - 1}].longitude"
        )
        points.append(
            CrawlPoint(
                id=build_point_id(latitude=latitude, longitude=longitude),
                name=f"nuidt-{index:03d}",
                latitude=latitude,
                longitude=longitude,
            )
        )
    return tuple(points)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object.")
    return cast(Mapping[str, object], value)


def _parse_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a float, got bool.")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a float-like string.") from exc
    raise ValueError(f"{field_name} must be a float.")

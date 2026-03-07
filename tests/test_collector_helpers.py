from datetime import UTC, datetime

from sevenma_crawler.api import SurroundingCar
from sevenma_crawler.collector import build_vehicle_uid, floor_to_logical_slot


def test_build_vehicle_uid_prefers_vendor_lock_id() -> None:
    car = SurroundingCar(vendor_lock_id="LOCK-001", id=42, number="car-7")

    vehicle_uid = build_vehicle_uid("zhuli", car)

    assert vehicle_uid == "zhuli:vendor_lock:LOCK-001"


def test_floor_to_logical_slot_rounds_down_to_interval_boundary() -> None:
    timestamp = datetime(2026, 3, 7, 6, 30, 29, tzinfo=UTC)

    logical_slot = floor_to_logical_slot(timestamp, 60)

    assert logical_slot == datetime(2026, 3, 7, 6, 30, 0, tzinfo=UTC)

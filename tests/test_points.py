import json
from pathlib import Path

from sevenma_crawler.points import build_point_id, load_points


def test_load_points_returns_deterministic_ids(tmp_path: Path) -> None:
    path = tmp_path / "points.json"
    path.write_text(
        json.dumps(
            [
                {"latitude": 32.1, "longitude": 118.7},
                {"latitude": 32.2, "longitude": 118.8},
            ]
        ),
        encoding="utf-8",
    )

    points = load_points(path)

    assert [point.name for point in points] == ["nuidt-001", "nuidt-002"]
    assert points[0].id == build_point_id(latitude=32.1, longitude=118.7)

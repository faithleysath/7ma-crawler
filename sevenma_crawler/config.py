from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path


def default_points_file() -> Path:
    """Return the repository's default point-definition file."""

    return Path(__file__).resolve().parent.parent / "南信大选点.json"


def build_default_collector_id() -> str:
    """Build a readable default collector identifier for the current process."""

    return f"{socket.gethostname()}:{os.getpid()}"


@dataclass(slots=True, frozen=True)
class CollectorSettings:
    """Runtime settings for the long-lived collector process."""

    database_url: str
    points_file: Path
    source_namespace: str
    collector_id: str
    interval_seconds: int = 60
    concurrency: int = 8
    timeout_seconds: float = 10.0
    request_jitter_seconds: float = 0.35

    def validate(self) -> CollectorSettings:
        """Return a validated copy of the settings."""

        if not self.database_url:
            raise ValueError("database_url must not be empty.")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than 0.")
        if self.concurrency <= 0:
            raise ValueError("concurrency must be greater than 0.")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0.")
        if self.request_jitter_seconds < 0:
            raise ValueError("request_jitter_seconds must be >= 0.")
        return self

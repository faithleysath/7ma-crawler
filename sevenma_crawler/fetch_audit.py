from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import Database

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class FetchAttemptLogRecord:
    """Raw per-attempt collector journal entry written before final DB shaping."""

    id: uuid.UUID
    fetch_id: uuid.UUID
    sweep_id: uuid.UUID
    point_id: uuid.UUID
    point_name: str
    source_namespace: str
    collector_id: str
    attempt: int
    requested_at: datetime
    finished_at: datetime
    request_latitude: float
    request_longitude: float
    http_status: int | None
    status_code: int | None
    trace_id: str | None
    error_type: str | None
    error_message: str | None
    response_body: str | None

    def as_json_dict(self) -> dict[str, object]:
        return {
            "id": str(self.id),
            "fetch_id": str(self.fetch_id),
            "sweep_id": str(self.sweep_id),
            "point_id": str(self.point_id),
            "point_name": self.point_name,
            "source_namespace": self.source_namespace,
            "collector_id": self.collector_id,
            "attempt": self.attempt,
            "requested_at": self.requested_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "request_latitude": self.request_latitude,
            "request_longitude": self.request_longitude,
            "http_status": self.http_status,
            "status_code": self.status_code,
            "trace_id": self.trace_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "response_body": self.response_body,
        }


class RawFetchAuditLogger:
    """Best-effort sink that journals raw fetch attempts to file and PostgreSQL."""

    def __init__(self, *, log_dir: Path, database: Database | None) -> None:
        self._log_dir = log_dir
        self._database = database
        self._lock = threading.Lock()

    def write(self, record: FetchAttemptLogRecord) -> None:
        with self._lock:
            self._write_file(record)
            self._write_database(record)

    def _write_file(self, record: FetchAttemptLogRecord) -> None:
        file_path = self._build_file_path(record)
        line = json.dumps(record.as_json_dict(), ensure_ascii=False, sort_keys=True)
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            LOGGER.exception("failed to append raw fetch audit log file=%s", file_path)

    def _write_database(self, record: FetchAttemptLogRecord) -> None:
        if self._database is None:
            return
        try:
            self._database.insert_fetch_attempt_log(record)
        except Exception:
            LOGGER.exception(
                "failed to persist raw fetch audit log fetch_id=%s attempt=%s",
                record.fetch_id,
                record.attempt,
            )

    def _build_file_path(self, record: FetchAttemptLogRecord) -> Path:
        day = record.requested_at.astimezone(UTC).strftime("%Y-%m-%d")
        return self._log_dir / record.source_namespace / f"{day}.jsonl"

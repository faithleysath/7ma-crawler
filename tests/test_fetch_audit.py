import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sevenma_crawler.fetch_audit import FetchAttemptLogRecord, RawFetchAuditLogger


class FakeDatabase:
    def __init__(self) -> None:
        self.records: list[FetchAttemptLogRecord] = []

    def insert_fetch_attempt_log(self, record: FetchAttemptLogRecord) -> None:
        self.records.append(record)


class FailingDatabase:
    def insert_fetch_attempt_log(self, _record: FetchAttemptLogRecord) -> None:
        raise RuntimeError("database unavailable")


def test_raw_fetch_audit_logger_writes_file_and_database(tmp_path: Path) -> None:
    database = FakeDatabase()
    logger = RawFetchAuditLogger(log_dir=tmp_path, database=database)
    record = _build_record()

    logger.write(record)

    log_path = tmp_path / "prod" / "2026-04-01.jsonl"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]

    assert len(entries) == 1
    assert entries[0]["fetch_id"] == str(record.fetch_id)
    assert entries[0]["response_body"] == record.response_body
    assert database.records == [record]


def test_raw_fetch_audit_logger_keeps_file_write_when_database_fails(tmp_path: Path) -> None:
    logger = RawFetchAuditLogger(log_dir=tmp_path, database=FailingDatabase())

    logger.write(_build_record())

    log_path = tmp_path / "prod" / "2026-04-01.jsonl"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]

    assert len(entries) == 1
    assert entries[0]["point_name"] == "nuidt-test"


def _build_record() -> FetchAttemptLogRecord:
    return FetchAttemptLogRecord(
        id=uuid.uuid4(),
        fetch_id=uuid.uuid4(),
        sweep_id=uuid.uuid4(),
        point_id=uuid.uuid4(),
        point_name="nuidt-test",
        source_namespace="prod",
        collector_id="collector-1",
        attempt=1,
        requested_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 4, 1, 12, 0, 2, tzinfo=UTC),
        request_latitude=32.202,
        request_longitude=118.715,
        http_status=200,
        status_code=200,
        trace_id="trace-1",
        error_type=None,
        error_message=None,
        response_body='{"status_code":200}',
    )

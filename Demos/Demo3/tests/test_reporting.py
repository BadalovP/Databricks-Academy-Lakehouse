from __future__ import annotations

import json

from urbanflow.reporting import (
    build_bronze_execution_report,
    reconcile_producer_and_bronze,
    write_json_report,
)


def _record(event_id: str, offset: int) -> dict:
    return {
        "execution_id": "run-1",
        "event_id": event_id,
        "source_last_updated": 100,
        "collected_at": "2026-09-28T21:00:00Z",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": "2026-09-28T21:00:01Z",
        "parse_error": False,
    }


def test_bronze_report_reconciles_counts_ids_offsets_and_timestamps() -> None:
    report = build_bronze_execution_report(
        [_record("event-1", 10), _record("event-2", 11)],
        execution_id="run-1",
        expected_published_events=2,
        expected_source_last_updated=100,
        query_terminated=True,
        query_progress={"numInputRows": 2},
    )
    assert report["status"] == "PASS"
    assert report["distinct_event_ids"] == 2
    assert report["offset_ranges"] == [
        {"partition": 0, "minimum_offset": 10, "maximum_offset": 11, "row_count": 2}
    ]


def test_bronze_report_exposes_duplicates_and_fails_reconciliation() -> None:
    report = build_bronze_execution_report(
        [_record("event-1", 10), _record("event-1", 11)],
        execution_id="run-1",
        expected_published_events=2,
        expected_source_last_updated=100,
        query_terminated=True,
    )
    assert report["status"] == "FAIL"
    assert report["duplicate_rows"] == 1


def test_producer_and_bronze_reports_compare_exact_event_ids(tmp_path) -> None:
    producer = {"execution_id": "run-1", "published_events": 2, "event_ids": ["a", "b"]}
    bronze = {
        "execution_id": "run-1",
        "status": "PASS",
        "consumed_rows": 2,
        "event_ids": ["b", "a"],
    }
    report = reconcile_producer_and_bronze(producer, bronze)
    assert report["status"] == "PASS"

    destination = write_json_report(report, tmp_path / "report.json")
    assert json.loads(destination.read_text())["status"] == "PASS"

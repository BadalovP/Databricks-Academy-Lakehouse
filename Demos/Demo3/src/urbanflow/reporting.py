"""Secret-free execution evidence for the bounded UrbanFlow streaming test."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _is_rejected(row: Mapping[str, Any]) -> bool:
    """A Bronze row is unusable when the payload failed to parse or carries no event ID."""
    return bool(row.get("parse_error")) or not str(row.get("event_id") or "").strip()


def build_bronze_execution_report(
    records: Iterable[Mapping[str, Any]],
    *,
    execution_id: str,
    expected_published_events: int,
    expected_source_last_updated: int,
    query_terminated: bool,
    query_progress: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Reconcile one execution ID without including credentials or raw payloads."""
    rows = [dict(record) for record in records]
    # Safety-critical filter: the shared Event Hub also holds messages from other producers and
    # from earlier runs, and the consumer starts at the earliest offset, so this single line is
    # what makes every count below belong to this execution only. Never remove it.
    matching_rows = [row for row in rows if str(row.get("execution_id")) == execution_id]
    rejected_rows = [row for row in matching_rows if _is_rejected(row)]
    accepted_rows = [row for row in matching_rows if not _is_rejected(row)]
    event_ids = [str(row["event_id"]) for row in accepted_rows]
    distinct_event_ids = sorted(set(event_ids))
    source_timestamps = {
        int(row["source_last_updated"])
        for row in accepted_rows
        if row.get("source_last_updated") is not None
    }

    partition_rows: dict[int, list[int]] = defaultdict(list)
    for row in matching_rows:
        partition = row.get("kafka_partition")
        offset = row.get("kafka_offset")
        if partition is not None and offset is not None:
            partition_rows[int(partition)].append(int(offset))
    offset_ranges = [
        {
            "partition": partition,
            "minimum_offset": min(offsets),
            "maximum_offset": max(offsets),
            "row_count": len(offsets),
        }
        for partition, offsets in sorted(partition_rows.items())
    ]

    duplicate_rows = len(event_ids) - len(distinct_event_ids)
    collection_timestamp_rows = sum(
        1 for row in accepted_rows if str(row.get("collected_at") or "").strip()
    )
    broker_timestamp_rows = sum(
        1 for row in accepted_rows if row.get("kafka_timestamp") is not None
    )
    source_timestamp_rows = sum(
        1 for row in accepted_rows if row.get("source_last_updated") is not None
    )
    passed = all(
        (
            query_terminated,
            len(matching_rows) == expected_published_events,
            len(distinct_event_ids) == expected_published_events,
            duplicate_rows == 0,
            len(rejected_rows) == 0,
            source_timestamps == {expected_source_last_updated},
            collection_timestamp_rows == expected_published_events,
            broker_timestamp_rows == expected_published_events,
            source_timestamp_rows == expected_published_events,
        )
    )
    return {
        "report_type": "urbanflow_bronze_execution",
        "execution_id": execution_id,
        "status": "PASS" if passed else "FAIL",
        "query_terminated": query_terminated,
        "expected_published_events": expected_published_events,
        "consumed_rows": len(matching_rows),
        "accepted_rows": len(accepted_rows),
        "rejected_rows": len(rejected_rows),
        "distinct_event_ids": len(distinct_event_ids),
        "duplicate_rows": duplicate_rows,
        "expected_source_last_updated": expected_source_last_updated,
        "observed_source_last_updated": sorted(source_timestamps),
        "collection_timestamp_rows": collection_timestamp_rows,
        "source_timestamp_rows": source_timestamp_rows,
        "broker_timestamp_rows": broker_timestamp_rows,
        "event_ids": distinct_event_ids,
        "offset_ranges": offset_ranges,
        "query_progress": _json_safe(dict(query_progress or {})),
    }


def reconcile_producer_and_bronze(
    producer_report: Mapping[str, Any], bronze_report: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare producer and Bronze reports after a live run, entirely offline."""
    producer_ids = {str(value) for value in producer_report.get("event_ids", [])}
    bronze_ids = {str(value) for value in bronze_report.get("event_ids", [])}
    execution_ids_match = producer_report.get("execution_id") == bronze_report.get("execution_id")
    # A missing count stays negative so an incomplete report can never look reconciled.
    published = int(producer_report.get("published_events", -1))
    consumed = int(bronze_report.get("consumed_rows", -1))
    duplicate_bronze_rows = int(bronze_report.get("duplicate_rows", 0))
    bronze_status = str(bronze_report.get("status", "MISSING"))
    missing = sorted(producer_ids - bronze_ids)
    unexpected = sorted(bronze_ids - producer_ids)
    passed = all(
        (
            execution_ids_match,
            published >= 0,
            published == consumed,
            not missing,
            not unexpected,
            duplicate_bronze_rows == 0,
            bronze_status == "PASS",
        )
    )
    return {
        "report_type": "urbanflow_end_to_end_reconciliation",
        "execution_id": producer_report.get("execution_id"),
        "status": "PASS" if passed else "FAIL",
        "execution_ids_match": execution_ids_match,
        "published_events": published,
        "consumed_rows": consumed,
        "duplicate_bronze_rows": duplicate_bronze_rows,
        "bronze_status": bronze_status,
        "missing_event_ids": missing,
        "unexpected_event_ids": unexpected,
    }


def write_json_report(report: Mapping[str, Any], path: str | Path) -> Path:
    """Write deterministic JSON evidence to an already authorized path."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(_json_safe(dict(report)), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination

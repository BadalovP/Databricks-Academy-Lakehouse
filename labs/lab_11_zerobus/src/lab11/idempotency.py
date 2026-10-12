"""Downstream idempotency: duplicate statistics and the Bronze-to-Silver MERGE, in Python.

This module is the executable specification of ``sql/01_deduplication.sql``. The tests run both
on the same rows (the SQL through DuckDB) and require identical results.

Deduplication policy, per ``event_id``:

1. Canonical delivery = earliest ``produced_at``; ties broken by the smallest ``batch_id``.
   Duplicates of one event carry identical business fields, so the choice only matters for the
   delivery metadata, and it is deterministic either way.
2. ``delivery_count`` = number of Bronze rows for the event.
3. A Silver row is updated only when one of its values would change; otherwise it is left alone,
   so re-running the MERGE on unchanged Bronze writes nothing.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from lab11.events import BUSINESS_FIELDS

SILVER_COLUMNS = (
    "event_id",
    *BUSINESS_FIELDS,
    "first_batch_id",
    "first_produced_at",
    "delivery_count",
)

Row = Mapping[str, object]


@dataclass(frozen=True)
class DuplicateStats:
    raw_rows: int
    unique_events: int
    duplicate_deliveries: int
    events_with_duplicates: int
    conflicting_events: int


@dataclass(frozen=True)
class MergeResult:
    inserted: int
    updated: int
    unchanged: int
    silver: dict[str, dict[str, object]]


def _group(rows: Iterable[Row]) -> dict[str, list[Row]]:
    groups: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        groups[str(row["event_id"])].append(row)
    return groups


def duplicate_stats(rows: Iterable[Row]) -> DuplicateStats:
    groups = _group(rows)
    raw = sum(len(g) for g in groups.values())
    conflicting = sum(
        1 for g in groups.values() if len({tuple(r[f] for f in BUSINESS_FIELDS) for r in g}) > 1
    )
    return DuplicateStats(
        raw_rows=raw,
        unique_events=len(groups),
        duplicate_deliveries=raw - len(groups),
        events_with_duplicates=sum(1 for g in groups.values() if len(g) > 1),
        conflicting_events=conflicting,
    )


def canonical_rows(rows: Iterable[Row]) -> dict[str, dict[str, object]]:
    """The Silver image of Bronze: one row per event, chosen by the documented policy."""
    silver = {}
    for event_id, group in _group(rows).items():
        first = min(group, key=lambda r: (r["produced_at"], r["batch_id"]))
        silver[event_id] = {
            "event_id": event_id,
            **{f: first[f] for f in BUSINESS_FIELDS},
            "first_batch_id": first["batch_id"],
            "first_produced_at": first["produced_at"],
            "delivery_count": len(group),
        }
    return silver


def merge(silver: Mapping[str, Mapping[str, object]], bronze_rows: Iterable[Row]) -> MergeResult:
    """Apply one MERGE of Bronze into ``silver`` without mutating the input."""
    result = {key: dict(value) for key, value in silver.items()}
    inserted = updated = unchanged = 0
    for event_id, row in canonical_rows(bronze_rows).items():
        current = result.get(event_id)
        if current is None:
            result[event_id] = row
            inserted += 1
        elif any(current.get(column) != row[column] for column in SILVER_COLUMNS):
            result[event_id] = row
            updated += 1
        else:
            unchanged += 1
    return MergeResult(inserted, updated, unchanged, result)

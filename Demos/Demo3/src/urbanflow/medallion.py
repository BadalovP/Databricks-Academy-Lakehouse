"""Pure local preparation for Silver, Quarantine, and shortage reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from urbanflow.quality import QualityResult, split_quality
from urbanflow.transformations import classify_availability


@dataclass(frozen=True)
class PreparedStationBatch:
    bronze_count: int
    silver: list[dict[str, Any]]
    quarantine: list[dict[str, Any]]
    duplicates: list[dict[str, Any]]
    shortages: list[dict[str, Any]]
    rule_counts: dict[str, int]

    @property
    def reconciles(self) -> bool:
        return len(self.silver) + len(self.quarantine) + len(self.duplicates) == self.bronze_count


def _deduplicate_event_ids(
    rows: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    unique: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        identifier = str(row.get("event_id") or "")
        if identifier in seen:
            duplicate = dict(row)
            duplicate["failed_rules"] = ["DUPLICATE_EVENT_ID"]
            duplicates.append(duplicate)
        else:
            seen.add(identifier)
            unique.append(dict(row))
    return unique, duplicates


def prepare_station_batch(
    rows: Iterable[dict[str, Any]],
    *,
    known_station_ids: set[str] | None = None,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> PreparedStationBatch:
    """Route invalid rows, deduplicate valid events, and calculate shortages."""
    materialized = [dict(row) for row in rows]
    quality: QualityResult = split_quality(materialized, known_station_ids=known_station_ids)
    unique, duplicates = _deduplicate_event_ids(quality.valid)
    silver = [
        classify_availability(
            row,
            low_bike_threshold=low_bike_threshold,
            low_dock_threshold=low_dock_threshold,
        )
        for row in unique
    ]
    shortages = [
        row
        for row in silver
        if row["availability_status"] in {"LOW_BIKES", "LOW_DOCKS", "LOW_BIKES_AND_DOCKS"}
    ]
    rule_counts = dict(quality.rule_counts)
    if duplicates:
        rule_counts["DUPLICATE_EVENT_ID"] = len(duplicates)
        rule_counts["PASS"] = rule_counts.get("PASS", 0) - len(duplicates)
    prepared = PreparedStationBatch(
        bronze_count=len(materialized),
        silver=silver,
        quarantine=quality.quarantine,
        duplicates=duplicates,
        shortages=shortages,
        rule_counts=dict(sorted(rule_counts.items())),
    )
    if not prepared.reconciles:
        raise AssertionError("Bronze rows did not reconcile to Silver, Quarantine, and duplicates.")
    return prepared

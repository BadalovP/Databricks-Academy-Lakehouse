"""Data-contract and data-quality checks for station observations."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable

REQUIRED_STATUS_FIELDS = (
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
    "collected_at",
)


@dataclass(frozen=True)
class QualityResult:
    valid: list[dict[str, Any]]
    quarantine: list[dict[str, Any]]
    rule_counts: dict[str, int]
    input_count: int

    @property
    def reconciles(self) -> bool:
        return len(self.valid) + len(self.quarantine) == self.input_count


def validate_observation(
    row: dict[str, Any],
    *,
    known_station_ids: set[str] | None = None,
    now_epoch_seconds: int | None = None,
    freshness_seconds: int = 300,
) -> list[str]:
    """Return all failed rules instead of dropping the row after the first failure."""
    failures: list[str] = []
    missing = [field for field in REQUIRED_STATUS_FIELDS if row.get(field) is None]
    if missing:
        failures.append("MISSING_REQUIRED_FIELD")
        return failures

    station_id = str(row["station_id"]).strip()
    if not station_id:
        failures.append("EMPTY_STATION_ID")
    if known_station_ids is not None and station_id not in known_station_ids:
        failures.append("UNKNOWN_STATION_ID")

    try:
        bikes = int(row["num_bikes_available"])
        docks = int(row["num_docks_available"])
        last_reported = int(row["last_reported"])
    except (TypeError, ValueError):
        failures.append("INVALID_NUMERIC_TYPE")
        return failures

    if bikes < 0 or docks < 0:
        failures.append("NEGATIVE_AVAILABILITY")
    capacity = row.get("capacity")
    if capacity is not None:
        try:
            parsed_capacity = int(capacity)
        except (TypeError, ValueError):
            failures.append("INVALID_CAPACITY_TYPE")
        else:
            if parsed_capacity < 0:
                failures.append("NEGATIVE_CAPACITY")
            elif bikes + docks > parsed_capacity:
                failures.append("AVAILABILITY_EXCEEDS_CAPACITY")
    if now_epoch_seconds is not None and now_epoch_seconds - last_reported > freshness_seconds:
        failures.append("STALE_OBSERVATION")
    if last_reported <= 0:
        failures.append("INVALID_SOURCE_TIMESTAMP")
    return failures


def split_quality(
    rows: Iterable[dict[str, Any]],
    *,
    known_station_ids: set[str] | None = None,
    now_epoch_seconds: int | None = None,
    freshness_seconds: int = 300,
) -> QualityResult:
    valid: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    rule_counts: Counter[str] = Counter()
    total = 0
    for row in rows:
        total += 1
        failures = validate_observation(
            row,
            known_station_ids=known_station_ids,
            now_epoch_seconds=now_epoch_seconds,
            freshness_seconds=freshness_seconds,
        )
        enriched = dict(row)
        enriched["failed_rules"] = failures
        if failures:
            quarantine.append(enriched)
            for failure in failures:
                rule_counts[failure] += 1
        else:
            valid.append(enriched)
            rule_counts["PASS"] += 1
    if len(valid) + len(quarantine) != total:
        raise AssertionError("Quality routing did not reconcile to the input count.")
    return QualityResult(valid, quarantine, dict(sorted(rule_counts.items())), total)


def current_epoch_seconds() -> int:
    return int(datetime.now(UTC).timestamp())

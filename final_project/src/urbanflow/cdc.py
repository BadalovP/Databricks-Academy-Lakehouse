"""Deterministic Delta Change Data Feed semantics for station-maintenance state."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Iterable, Mapping


class ChangeType(StrEnum):
    INSERT = "insert"
    UPDATE_PREIMAGE = "update_preimage"
    UPDATE_POSTIMAGE = "update_postimage"
    DELETE = "delete"


@dataclass(frozen=True)
class ChangeEvent:
    station_id: str
    maintenance_id: str
    change_type: ChangeType
    commit_version: int
    commit_timestamp: str
    values: Mapping[str, Any]

    @property
    def change_id(self) -> str:
        raw = (
            f"{self.maintenance_id}|{self.station_id}|{self.commit_version}|"
            f"{self.change_type.value}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CdcResult:
    target: dict[str, dict[str, Any]]
    history: tuple[ChangeEvent, ...]
    checkpoint: int
    inserted: int
    updated: int
    deleted: int
    ignored_preimages: int

    @property
    def changed(self) -> int:
        return self.inserted + self.updated + self.deleted


@dataclass(frozen=True)
class Reconciliation:
    source_count: int
    target_count: int
    missing_in_target: tuple[str, ...]
    unexpected_in_target: tuple[str, ...]
    mismatched: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return not (self.missing_in_target or self.unexpected_in_target or self.mismatched)


def apply_cdf(
    current: Mapping[str, Mapping[str, Any]],
    events: Iterable[ChangeEvent],
    *,
    checkpoint: int,
) -> CdcResult:
    """Apply CDF commits strictly after ``checkpoint`` without mutating the input.

    Delta update preimages are audit evidence, not target state. Inserts and update postimages
    upsert by maintenance_id; deletes remove the key. A repeat with the returned checkpoint is a
    no-op, which is the contract used by the notebook MERGE implementation.
    """
    target = {str(key): dict(value) for key, value in current.items()}
    ordered = sorted(
        (event for event in events if event.commit_version > checkpoint),
        key=lambda event: (event.commit_version, _change_order(event.change_type), event.change_id),
    )
    inserted = updated = deleted = ignored = 0
    for event in ordered:
        key = event.maintenance_id.strip()
        if not key or not event.station_id.strip() or event.commit_version < 0:
            raise ValueError("CDC events require non-empty IDs and a non-negative commit version.")
        if event.change_type == ChangeType.UPDATE_PREIMAGE:
            ignored += 1
            continue
        if event.change_type == ChangeType.DELETE:
            deleted += int(key in target)
            target.pop(key, None)
            continue
        row = dict(event.values)
        row.update(
            maintenance_id=key,
            station_id=event.station_id,
            source_commit_version=event.commit_version,
            source_commit_timestamp=event.commit_timestamp,
        )
        if key in target:
            updated += int(target[key] != row)
        else:
            inserted += 1
        target[key] = row
    next_checkpoint = max((event.commit_version for event in ordered), default=checkpoint)
    return CdcResult(
        target=target,
        history=tuple(ordered),
        checkpoint=next_checkpoint,
        inserted=inserted,
        updated=updated,
        deleted=deleted,
        ignored_preimages=ignored,
    )


def reconcile(
    source: Mapping[str, Mapping[str, Any]],
    target: Mapping[str, Mapping[str, Any]],
    *,
    compared_fields: tuple[str, ...] = ("station_id", "status", "severity", "updated_at"),
) -> Reconciliation:
    source_keys = set(source)
    target_keys = set(target)
    mismatched = tuple(
        sorted(
            key
            for key in source_keys & target_keys
            if any(source[key].get(field) != target[key].get(field) for field in compared_fields)
        )
    )
    return Reconciliation(
        source_count=len(source),
        target_count=len(target),
        missing_in_target=tuple(sorted(source_keys - target_keys)),
        unexpected_in_target=tuple(sorted(target_keys - source_keys)),
        mismatched=mismatched,
    )


def _change_order(change_type: ChangeType) -> int:
    return {
        ChangeType.UPDATE_PREIMAGE: 0,
        ChangeType.DELETE: 1,
        ChangeType.INSERT: 2,
        ChangeType.UPDATE_POSTIMAGE: 3,
    }[change_type]


SNAPSHOT_FIELDS = ("station_id", "status", "severity", "updated_at")


def load_snapshot_csv(path: Any) -> dict[str, dict[str, Any]]:
    """A maintenance snapshot keyed by maintenance_id; duplicate keys are refused."""
    import csv
    from pathlib import Path

    rows: dict[str, dict[str, Any]] = {}
    with Path(path).open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row.get("maintenance_id") or "").strip()
            if not key:
                raise ValueError(f"{path}: a row has no maintenance_id.")
            if key in rows:
                raise ValueError(f"{path}: duplicate maintenance_id {key}.")
            rows[key] = {field: row[field] for field in SNAPSHOT_FIELDS}
    return rows


def snapshot_change_events(
    previous: Mapping[str, Mapping[str, Any]],
    current: Mapping[str, Mapping[str, Any]],
    *,
    commit_version: int,
    commit_timestamp: str,
) -> list[ChangeEvent]:
    """The Change Data Feed rows one snapshot MERGE produces (insert/update/delete).

    This models the notebook's MERGE: changed rows emit an update pre- and post-image, new keys
    an insert, vanished keys a delete, and unchanged rows nothing at all.
    """

    def event(key: str, values: Mapping[str, Any], change_type: ChangeType) -> ChangeEvent:
        return ChangeEvent(
            station_id=str(values["station_id"]),
            maintenance_id=key,
            change_type=change_type,
            commit_version=commit_version,
            commit_timestamp=commit_timestamp,
            values={field: values[field] for field in SNAPSHOT_FIELDS if field != "station_id"},
        )

    events: list[ChangeEvent] = []
    for key in sorted(set(previous) | set(current)):
        before, after = previous.get(key), current.get(key)
        if before is None and after is not None:
            events.append(event(key, after, ChangeType.INSERT))
        elif after is None and before is not None:
            events.append(event(key, before, ChangeType.DELETE))
        elif before is not None and after is not None and dict(before) != dict(after):
            events.append(event(key, before, ChangeType.UPDATE_PREIMAGE))
            events.append(event(key, after, ChangeType.UPDATE_POSTIMAGE))
    return events

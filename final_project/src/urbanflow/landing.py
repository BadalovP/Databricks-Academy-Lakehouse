"""Bounded landing of the committed samples so a fresh environment can run end to end.

A new UrbanFlow 2.0 environment has no Bronze rows. In Azure, station events can arrive through
Event Hubs (``notebooks/03_eventhubs_to_bronze.py``). Everywhere, the platform Job can instead land
the committed GBFS snapshot as JSON-lines files in the target's Volume and ingest them with Auto
Loader. Both paths use the same event construction (``producer.build_events``) and the same Bronze
contract (``streaming.station_status_schema``), so downstream code cannot tell them apart except
through the recorded lineage columns.

Everything here is pure Python and unit-tested; the notebook only performs the file writes.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any

from urbanflow.gbfs_client import (
    REQUIRED_INFORMATION_FIELDS,
    REQUIRED_STATUS_FIELDS,
    parse_station_feed,
)
from urbanflow.historical import historical_landing_paths
from urbanflow.producer import build_events
from urbanflow.transformations import classify_availability

STATION_SOURCES = ("sample_autoloader", "existing_bronze")
# Fixed, Final-Project-owned sample identities. The historical Bronze table has no execution
# column, so the sample namespace is owned by exactly one execution and repeats are idempotent.
SAMPLE_HISTORICAL_ID = "urbanflow-final-hist-devsample40"
SAMPLE_WEATHER_ID = "urbanflow-final-weather-devsample48h"
SAMPLE_STATUS_URL = "fixtures/samples/station_status.sample.json"
_SAFE_SEGMENT = re.compile(r"[A-Za-z0-9._-]+")
ACTIONABLE_STATUSES = frozenset({"LOW_BIKES", "LOW_DOCKS", "LOW_BIKES_AND_DOCKS"})


@dataclass(frozen=True)
class LandingPaths:
    station_events_dir: str
    station_checkpoint: str
    historical_dir: str


def safe_segment(value: str, label: str) -> str:
    """A path segment that cannot traverse directories or inject anything."""
    if not _SAFE_SEGMENT.fullmatch(value or "") or value in {".", ".."}:
        raise ValueError(f"{label} may contain only letters, digits, '.', '_' and '-': {value!r}")
    return value


def landing_paths(
    volume_root: str, *, source_execution_id: str, historical_execution_id: str
) -> LandingPaths:
    """Per-execution station landing and checkpoint, plus the sample's historical landing."""
    root = PurePosixPath(volume_root)
    if not str(root).startswith("/Volumes/") or len(root.parts) != 5:
        raise ValueError("volume_root must be /Volumes/<catalog>/<schema>/<volume>.")
    execution = safe_segment(source_execution_id, "source_execution_id")
    historical = historical_landing_paths(
        str(root), execution_id=safe_segment(historical_execution_id, "historical_execution_id")
    )
    return LandingPaths(
        station_events_dir=str(root / "landing" / "station_events" / execution),
        station_checkpoint=str(root / "checkpoints" / "urbanflow_final" / "autoloader" / execution),
        historical_dir=historical["landing"],
    )


def sample_station_events(
    information_payload: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    execution_id: str,
    collected_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Station events for the committed snapshot, shaped exactly like Event Hubs messages."""
    safe_segment(execution_id, "execution_id")
    moment = collected_at or datetime.now(UTC)
    parse_station_feed(
        information_payload,
        name="station_information",
        url="fixtures/samples/station_information.sample.json",
        required_fields=REQUIRED_INFORMATION_FIELDS,
        collected_at=moment,
    )  # validates the reference half of the snapshot
    feed = parse_station_feed(
        status_payload,
        name="station_status",
        url=SAMPLE_STATUS_URL,
        required_fields=REQUIRED_STATUS_FIELDS,
        collected_at=moment,
    )
    return build_events(feed, execution_id=execution_id)


def to_json_lines(events: list[dict[str, Any]]) -> str:
    """One compact JSON object per line - the format Auto Loader reads as text."""
    return "".join(
        json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n" for event in events
    )


def expected_sample_profile(
    status_payload: dict[str, Any],
    *,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> dict[str, Any]:
    """Independent pure-Python expectation for a sample run, used by the final validation."""
    stations = status_payload["data"]["stations"]
    classified = [
        classify_availability(
            station,
            low_bike_threshold=low_bike_threshold,
            low_dock_threshold=low_dock_threshold,
        )
        for station in stations
    ]
    composition = Counter(row["availability_status"] for row in classified)
    actionable = sum(composition[status] for status in ACTIONABLE_STATUSES)
    return {
        "events": len(stations),
        "composition": dict(sorted(composition.items())),
        "actionable": actionable,
    }


def validate_station_source(station_source: str, source_execution_id: str) -> str:
    if station_source not in STATION_SOURCES:
        raise ValueError(
            f"station_source must be one of {STATION_SOURCES}, got {station_source!r}."
        )
    safe_segment(source_execution_id, "source_execution_id")
    return station_source


def station_expectations(
    station_source: str,
    station_counts: dict[str, int],
    sample_profile: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Expected per-layer counts for one station execution.

    A sample run has exact expectations from the pure-Python profile of the same snapshot. An
    Event Hubs run has no fixed size, so each layer must reconcile with the one before it.
    """
    if station_source == "sample_autoloader":
        if sample_profile is None:
            raise ValueError("A sample run needs the expected sample profile.")
        events, actionable = sample_profile["events"], sample_profile["actionable"]
        return {
            "bronze": events,
            "silver": events,
            "fact": events,
            "shortage": actionable,
            "priority": actionable,
        }
    if station_source == "existing_bronze":
        return {
            "silver": station_counts["bronze"],
            "fact": station_counts["silver"],
            "priority": station_counts["shortage"],
        }
    raise ValueError(f"Unknown station_source {station_source!r}.")


LANDING_PARAMETERS = (
    "run_station_pipeline",
    "station_source",
    "run_historical",
    "run_full_month",
    "source_execution_id",
    "historical_execution_id",
    "historical_landing_subdir",
    "catalog",
    "schema",
    "volume",
    "stream_timeout_seconds",
)


def landing_plan(values: dict[str, str]) -> tuple[bool, bool]:
    """(land the station sample, land the trip sample) for one run's parameters.

    Only the development sample is landed by the platform Job. Station events come from Event
    Hubs when ``station_source=existing_bronze``; the full month uses its own approved namespace.
    """
    flag = {name: values.get(name, "").strip().lower() == "true" for name in LANDING_PARAMETERS}
    land_station = (
        flag["run_station_pipeline"] and values.get("station_source") == "sample_autoloader"
    )
    land_trips = (
        flag["run_historical"]
        and not flag["run_full_month"]
        and not values.get("historical_landing_subdir", "").strip()
    )
    return land_station, land_trips


# Execution-scoped station tables, from Bronze to the actionable Gold outputs.
STATION_LAYER_TABLES = {
    "bronze": "bronze_station_status",
    "silver": "silver_station_status",
    "fact": "fact_station_availability",
    "shortage": "gold_station_shortage",
    "priority": "gold_rebalancing_priority",
}

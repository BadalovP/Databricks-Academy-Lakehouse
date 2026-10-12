"""Optional Zerobus station-health contract with at-least-once replay semantics."""

from __future__ import annotations

import math
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Callable, Iterable, Mapping

EVENT_NAMESPACE = uuid.UUID("e365b2b4-5f24-5d83-bb24-8229b83998d1")


@dataclass(frozen=True)
class StationHealthEvent:
    event_id: str
    station_id: str
    device_id: str
    sequence_number: int
    event_timestamp_micros: int
    battery_pct: float
    vibration_rms: float
    temperature_c: float
    run_id: str

    def record(self, *, batch_id: str, produced_at_micros: int) -> dict[str, object]:
        row = {
            **self.__dict__,
            "batch_id": batch_id,
            "produced_at_micros": produced_at_micros,
            "synthetic": True,
        }
        validate_health_record(row)
        return row


def event_id_for(run_id: str, station_id: str, device_id: str, sequence_number: int) -> str:
    value = f"{run_id}|{station_id}|{device_id}|{sequence_number}"
    return str(uuid.uuid5(EVENT_NAMESPACE, value))


def generate_health_events(
    *,
    run_id: str,
    station_ids: Iterable[str],
    readings_per_station: int = 3,
    base_time: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> list[StationHealthEvent]:
    if not run_id.strip() or readings_per_station < 1:
        raise ValueError("run_id and a positive readings_per_station are required.")
    events: list[StationHealthEvent] = []
    for station_index, station_id in enumerate(station_ids):
        if not station_id.strip():
            raise ValueError("station_id must not be blank.")
        device_id = f"health-{station_index + 1:03d}"
        for sequence in range(1, readings_per_station + 1):
            moment = base_time + timedelta(minutes=sequence - 1, seconds=station_index)
            events.append(
                StationHealthEvent(
                    event_id=event_id_for(run_id, station_id, device_id, sequence),
                    station_id=station_id,
                    device_id=device_id,
                    sequence_number=sequence,
                    event_timestamp_micros=int(moment.timestamp() * 1_000_000),
                    battery_pct=round(100.0 - station_index - sequence * 0.5, 2),
                    vibration_rms=round(0.1 + sequence * 0.03, 3),
                    temperature_c=round(20.0 + station_index * 0.2 + sequence * 0.1, 2),
                    run_id=run_id,
                )
            )
    return events


def validate_health_record(row: Mapping[str, object]) -> None:
    required = {
        "event_id",
        "station_id",
        "device_id",
        "sequence_number",
        "event_timestamp_micros",
        "battery_pct",
        "vibration_rms",
        "temperature_c",
        "run_id",
        "batch_id",
        "produced_at_micros",
        "synthetic",
    }
    if set(row) != required:
        raise ValueError(f"Station-health record fields differ: {sorted(set(row) ^ required)}")
    uuid.UUID(str(row["event_id"]))
    for field in ("station_id", "device_id", "run_id", "batch_id"):
        if not str(row[field]).strip():
            raise ValueError(f"{field} must not be blank.")
    battery = float(row["battery_pct"])
    vibration = float(row["vibration_rms"])
    temperature = float(row["temperature_c"])
    if not all(math.isfinite(value) for value in (battery, vibration, temperature)):
        raise ValueError("Health measurements must be finite.")
    if not 0 <= battery <= 100 or vibration < 0 or not -50 <= temperature <= 150:
        raise ValueError("Health measurement is outside the contract range.")
    if row["synthetic"] is not True:
        raise ValueError("Final Project station-health telemetry must be labelled synthetic.")


def replay_plan(events: list[StationHealthEvent], replay_count: int) -> list[StationHealthEvent]:
    if not 0 <= replay_count <= len(events):
        raise ValueError("replay_count must be between zero and the event count.")
    if replay_count == 0:
        return []
    step = len(events) / replay_count
    return [events[int(index * step)] for index in range(replay_count)]


def canonical_deliveries(rows: Iterable[Mapping[str, object]]) -> dict[str, dict[str, object]]:
    """Deduplicate at-least-once deliveries by stable event_id without losing audit counts."""
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        validate_health_record(row)
        grouped[str(row["event_id"])].append(row)
    result: dict[str, dict[str, object]] = {}
    for event_id, deliveries in grouped.items():
        first = min(deliveries, key=lambda row: (row["produced_at_micros"], row["batch_id"]))
        result[event_id] = {**first, "delivery_count": len(deliveries)}
    return result


# --- isolation from Lab 11 -------------------------------------------------------------------
# The Final Project never reuses Lab 11's schemas, tables, service principal or credentials. Its
# identity is read only from its own environment variables, and a value equal to Lab 11's is
# refused, so a copied Lab 11 secret cannot be used here by mistake.
BRONZE_TABLE = "zerobus_station_health_bronze"
SILVER_TABLE = "silver_station_health"
CREDENTIAL_ENV = ("URBANFLOW_FINAL_ZEROBUS_CLIENT_ID", "URBANFLOW_FINAL_ZEROBUS_CLIENT_SECRET")
LAB11_CREDENTIAL_ENV = ("ZEROBUS_CLIENT_ID", "ZEROBUS_CLIENT_SECRET")
LAB11_MARKERS = ("lab_11", "lab11", "iot_events")
MAX_RECORDS_PER_RUN = 30
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class IsolationError(ValueError):
    """The request would touch a Lab 11 object or identity."""


def assert_final_project_target(catalog: str, schema: str, table: str = BRONZE_TABLE) -> str:
    """Full name of a Final-Project-owned Zerobus table; anything else is refused."""
    for label, value in (("catalog", catalog), ("schema", schema), ("table", table)):
        if not _IDENTIFIER.fullmatch(value or ""):
            raise ValueError(f"{label} must be a plain identifier: {value!r}")
    name = f"{catalog}.{schema}.{table}"
    if any(marker in name.lower() for marker in LAB11_MARKERS):
        raise IsolationError(f"{name} belongs to Lab 11; the Final Project never writes there.")
    if "urbanflow_final" not in schema or table not in (BRONZE_TABLE, SILVER_TABLE):
        raise IsolationError(f"{name} is not a Final Project Zerobus table.")
    return name


def final_project_credentials(environ: Mapping[str, str]) -> tuple[str, str]:
    """The Final Project's own Zerobus identity, kept in memory only."""
    values = tuple(environ.get(name, "").strip() for name in CREDENTIAL_ENV)
    if not all(values):
        raise IsolationError(f"Set {' and '.join(CREDENTIAL_ENV)} for the Final Project identity.")
    lab11 = tuple(environ.get(name, "").strip() for name in LAB11_CREDENTIAL_ENV)
    if any(value and value in lab11 for value in values):
        raise IsolationError("These are Lab 11 credentials; the Final Project needs its own.")
    return values[0], values[1]


# --- delivery ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class HealthBatch:
    batch_id: str
    delivery: str  # "primary" or "replay"
    records: tuple[dict[str, object], ...]


def build_batches(
    events: list[StationHealthEvent],
    *,
    replay_count: int,
    batch_size: int,
    clock: Callable[[], int],
) -> list[HealthBatch]:
    """Primary batches, then one replay batch that re-sends events with their original IDs."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")
    replay = replay_plan(events, replay_count)
    if len(events) + len(replay) > MAX_RECORDS_PER_RUN:
        raise ValueError(f"A run may send at most {MAX_RECORDS_PER_RUN} records.")
    run_id = events[0].run_id if events else "empty"
    batches = []
    for number, start in enumerate(range(0, len(events), batch_size), start=1):
        batch_id = f"{run_id}-p{number:02d}"
        records = tuple(
            event.record(batch_id=batch_id, produced_at_micros=clock())
            for event in events[start : start + batch_size]
        )
        batches.append(HealthBatch(batch_id, "primary", records))
    if replay:
        batch_id = f"{run_id}-replay"
        records = tuple(e.record(batch_id=batch_id, produced_at_micros=clock()) for e in replay)
        batches.append(HealthBatch(batch_id, "replay", records))
    return batches


def deliver(open_stream: Callable[[], object], batches: list[HealthBatch]) -> list[dict]:
    """Send every batch and wait for its durability acknowledgment before the next one.

    A failure stops the run without retrying. Rerunning with the same ``run_id`` re-sends the
    same event IDs, which Silver deduplicates - at-least-once delivery, exactly-once Silver.
    """
    stream = open_stream()
    acknowledged = []
    try:
        for batch in batches:
            offset = stream.ingest_records_offset(list(batch.records))
            if offset is None:
                raise RuntimeError(f"batch {batch.batch_id}: the SDK returned no offset")
            stream.wait_for_offset(offset)
            acknowledged.append(
                {"batch_id": batch.batch_id, "delivery": batch.delivery,
                 "records": len(batch.records), "offset": offset}
            )  # fmt: skip
    finally:
        stream.close()
    return acknowledged

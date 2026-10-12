"""Optional Zerobus station-health contract with at-least-once replay semantics."""

from __future__ import annotations

import math
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Iterable, Mapping

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

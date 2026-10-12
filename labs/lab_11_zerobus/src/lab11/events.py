"""IoT telemetry events: deterministic generation, stable identifiers and schema validation.

The Bronze table schema is the contract Zerobus enforces. ``BRONZE_COLUMNS`` mirrors
``sql/00_create_tables.sql`` exactly (a test keeps them in sync), and ``validate_record`` rejects
anything Zerobus would reject before a byte leaves the producer.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from lab11.config import ProducerSettings, run_identifier

# Column name -> Delta type, in table order. TIMESTAMP travels as epoch microseconds (int64),
# as the Zerobus type mapping requires for JSON and Protobuf records.
BRONZE_COLUMNS: dict[str, str] = {
    "event_id": "STRING",
    "device_id": "STRING",
    "producer_id": "STRING",
    "sequence_number": "BIGINT",
    "event_timestamp": "TIMESTAMP",
    "temperature": "DOUBLE",
    "humidity": "DOUBLE",
    "run_id": "STRING",
    "batch_id": "STRING",
    "produced_at": "TIMESTAMP",
}
# Fields that describe the reading itself; two deliveries of one event must agree on these.
BUSINESS_FIELDS = (
    "device_id",
    "producer_id",
    "sequence_number",
    "event_timestamp",
    "temperature",
    "humidity",
    "run_id",
)
# Fields that describe one delivery attempt and may differ between duplicates.
DELIVERY_FIELDS = ("batch_id", "produced_at")

# Fixed namespace so the same (producer, run, device, sequence) always yields the same event_id.
EVENT_NAMESPACE = uuid.UUID("6f1c7d64-4d55-5b3a-9c1e-0b11ab0c1d11")

_MIN_MICROS = int(datetime(2000, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
_MAX_MICROS = int(datetime(2100, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
_INT64 = (-(2**63), 2**63 - 1)
RANGES = {"temperature": (-50.0, 150.0), "humidity": (0.0, 100.0)}


class SchemaValidationError(ValueError):
    """A record does not match the Bronze table contract."""


def to_micros(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError("timestamps must be time-zone aware")
    delta = moment.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


@dataclass(frozen=True)
class IoTEvent:
    """One sensor reading. Identity is ``event_id``; it never changes when the event is re-sent."""

    event_id: str
    device_id: str
    producer_id: str
    sequence_number: int
    event_timestamp: int
    temperature: float
    humidity: float
    run_id: str

    def to_record(self, batch_id: str, produced_at: int) -> dict[str, object]:
        """Zerobus JSON record for one delivery of this event."""
        return {
            "event_id": self.event_id,
            "device_id": self.device_id,
            "producer_id": self.producer_id,
            "sequence_number": self.sequence_number,
            "event_timestamp": self.event_timestamp,
            "temperature": self.temperature,
            "humidity": self.humidity,
            "run_id": self.run_id,
            "batch_id": batch_id,
            "produced_at": produced_at,
        }


def event_id_for(producer_id: str, run_id: str, device_id: str, sequence_number: int) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{producer_id}|{run_id}|{device_id}|{sequence_number}"))


def generate_events(settings: ProducerSettings, run_id: str) -> list[IoTEvent]:
    """A finite, deterministic set of readings: same settings and run id, same events."""
    run_identifier(run_id)
    events = []
    for device_index in range(settings.devices):
        device_id = f"device-{device_index + 1:02d}"
        for sequence_number in range(1, settings.readings_per_device + 1):
            # Seeded per reading, so a reading does not depend on generation order.
            rng = random.Random(f"{settings.seed}|{device_id}|{sequence_number}")
            moment = settings.base_timestamp + timedelta(
                seconds=(sequence_number - 1) * settings.interval_seconds + device_index
            )
            events.append(
                IoTEvent(
                    event_id=event_id_for(settings.producer_id, run_id, device_id, sequence_number),
                    device_id=device_id,
                    producer_id=settings.producer_id,
                    sequence_number=sequence_number,
                    event_timestamp=to_micros(moment),
                    temperature=round(18.0 + rng.uniform(0.0, 10.0), 2),
                    humidity=round(35.0 + rng.uniform(0.0, 30.0), 2),
                    run_id=run_id,
                )
            )
    return events


def validation_errors(record: dict[str, object]) -> list[str]:
    """Every way ``record`` violates the Bronze contract (empty list when valid)."""
    errors = []
    unknown = sorted(set(record) - set(BRONZE_COLUMNS))
    if unknown:
        errors.append(f"unknown fields {unknown} (Zerobus rejects fields missing from the table)")
    for column, delta_type in BRONZE_COLUMNS.items():
        if column not in record or record[column] is None:
            errors.append(f"{column} is missing or null")
            continue
        value = record[column]
        if delta_type == "STRING":
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{column} must be a non-empty string")
        elif delta_type == "BIGINT":
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not _INT64[0] <= value <= _INT64[1]
            ):
                errors.append(f"{column} must be a 64-bit integer")
        elif delta_type == "TIMESTAMP":
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not _MIN_MICROS <= value < _MAX_MICROS
            ):
                errors.append(f"{column} must be epoch microseconds between 2000 and 2100")
        elif delta_type == "DOUBLE":
            if (
                isinstance(value, bool)
                or not isinstance(value, int | float)
                or not math.isfinite(value)
            ):
                errors.append(f"{column} must be a finite number")
            elif column in RANGES and not RANGES[column][0] <= value <= RANGES[column][1]:
                errors.append(f"{column}={value} is outside {RANGES[column]}")
    event_id = record.get("event_id")
    if isinstance(event_id, str) and event_id.strip():
        try:
            uuid.UUID(event_id)
        except ValueError:
            errors.append("event_id must be a UUID")
    return errors


def validate_record(record: dict[str, object]) -> dict[str, object]:
    errors = validation_errors(record)
    if errors:
        raise SchemaValidationError(f"event {record.get('event_id')!r}: " + "; ".join(errors))
    return record

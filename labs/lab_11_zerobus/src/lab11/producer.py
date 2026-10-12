"""Delivery plan: which records the producer sends, in which batches.

A run sends every generated event once in the ``primary`` delivery, then re-sends a fixed subset
with the *same* ``event_id`` in a ``replay`` delivery. The replay reproduces, on purpose, the
duplicate a producer creates when it retries after losing an acknowledgment. Zerobus is
at-least-once, so Bronze keeps both deliveries; Silver must still hold each event once.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from lab11.config import ProducerSettings
from lab11.events import IoTEvent, generate_events, to_micros, validate_record

PRIMARY = "primary"
REPLAY = "replay"


def wall_clock_micros() -> int:
    from datetime import UTC, datetime

    return to_micros(datetime.now(UTC))


@dataclass(frozen=True)
class Batch:
    """Records sent together and acknowledged together."""

    batch_id: str
    delivery: str
    records: tuple[dict[str, object], ...]

    @property
    def event_ids(self) -> list[str]:
        return [str(record["event_id"]) for record in self.records]


@dataclass(frozen=True)
class DeliveryPlan:
    run_id: str
    events: tuple[IoTEvent, ...]
    batches: tuple[Batch, ...]

    @property
    def records(self) -> list[dict[str, object]]:
        return [record for batch in self.batches for record in batch.records]

    @property
    def replayed_event_ids(self) -> list[str]:
        return [
            eid for batch in self.batches if batch.delivery == REPLAY for eid in batch.event_ids
        ]

    def summary(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "unique_events": len(self.events),
            "deliveries": len(self.records),
            "replayed_events": len(self.replayed_event_ids),
            "batches": [
                {"batch_id": b.batch_id, "delivery": b.delivery, "records": len(b.records)}
                for b in self.batches
            ],
        }


def replay_selection(events: list[IoTEvent], replay_count: int) -> list[IoTEvent]:
    """Spread the replayed events evenly across the run, deterministically."""
    if replay_count == 0:
        return []
    step = len(events) / replay_count
    return [events[int(i * step)] for i in range(replay_count)]


def _chunks(items: list[IoTEvent], size: int) -> list[list[IoTEvent]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_plan(
    settings: ProducerSettings,
    run_id: str,
    clock: Callable[[], int] = wall_clock_micros,
) -> DeliveryPlan:
    """Generate, validate and batch the run. ``clock`` stamps ``produced_at`` per batch."""
    events = generate_events(settings, run_id)
    batches = []
    for delivery, selected in (
        (PRIMARY, events),
        (REPLAY, replay_selection(events, settings.replay_count)),
    ):
        for number, chunk in enumerate(_chunks(selected, settings.batch_size), start=1):
            batch_id = f"{run_id}-{delivery}-{number:02d}"
            produced_at = clock()
            records = tuple(
                validate_record(event.to_record(batch_id, produced_at)) for event in chunk
            )
            batches.append(Batch(batch_id, delivery, records))
    return DeliveryPlan(run_id, tuple(events), tuple(batches))


def fixed_clock(start: int, step: int = 1_000_000) -> Callable[[], int]:
    """Deterministic clock for dry runs and tests."""
    state = {"now": start - step}

    def tick() -> int:
        state["now"] += step
        return state["now"]

    return tick


def monotonic_seconds() -> float:
    return time.monotonic()

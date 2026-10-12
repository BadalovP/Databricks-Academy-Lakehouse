from datetime import UTC, datetime

import pytest

from urbanflow.zerobus import (
    canonical_deliveries,
    generate_health_events,
    replay_plan,
)


def test_health_generation_is_stable_and_clearly_synthetic() -> None:
    kwargs = {
        "run_id": "offline-001",
        "station_ids": ["station-a", "station-b"],
        "readings_per_station": 2,
        "base_time": datetime(2026, 10, 12, tzinfo=UTC),
    }
    first = generate_health_events(**kwargs)
    second = generate_health_events(**kwargs)
    assert first == second
    assert len(first) == 4
    assert all(event.record(batch_id="b1", produced_at_micros=1)["synthetic"] for event in first)


def test_replay_has_same_ids_and_silver_is_idempotent() -> None:
    events = generate_health_events(run_id="offline-002", station_ids=["station-a"])
    replay = replay_plan(events, 2)
    primary_rows = [event.record(batch_id="primary", produced_at_micros=10) for event in events]
    replay_rows = [event.record(batch_id="replay", produced_at_micros=20) for event in replay]
    silver = canonical_deliveries([*primary_rows, *replay_rows])
    assert set(silver) == {event.event_id for event in events}
    assert sum(row["delivery_count"] for row in silver.values()) == len(primary_rows) + len(
        replay_rows
    )


def test_replay_count_cannot_exceed_event_count() -> None:
    with pytest.raises(ValueError):
        replay_plan([], 1)

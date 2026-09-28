from __future__ import annotations

from urbanflow.medallion import prepare_station_batch


def _row(event_id: str, *, bikes: int = 1, docks: int = 8) -> dict:
    return {
        "event_id": event_id,
        "execution_id": "run-1",
        "station_id": "station-1",
        "num_bikes_available": bikes,
        "num_docks_available": docks,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 100,
        "source_last_updated": 100,
        "collected_at": "2026-09-28T21:00:00Z",
        "source_url": "https://example.test/station_status.json",
    }


def test_medallion_preparation_routes_deduplicates_and_calculates_shortages() -> None:
    rows = [
        _row("event-1"),
        _row("event-1"),
        {**_row("event-2"), "num_docks_available": -1},
        _row("event-3", bikes=10),
    ]

    prepared = prepare_station_batch(rows, known_station_ids={"station-1"})

    assert prepared.bronze_count == 4
    assert len(prepared.silver) == 2
    assert len(prepared.quarantine) == 1
    assert len(prepared.duplicates) == 1
    assert len(prepared.shortages) == 1
    assert prepared.reconciles is True
    assert prepared.rule_counts["DUPLICATE_EVENT_ID"] == 1

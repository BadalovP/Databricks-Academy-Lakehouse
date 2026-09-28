from __future__ import annotations

from urbanflow.transformations import (
    apply_scd2_station_change,
    classify_availability,
    deduplicate_latest,
    join_station_information,
    summarize_availability,
)

BASE = {
    "station_id": "station-1",
    "num_bikes_available": 2,
    "num_docks_available": 5,
    "is_installed": 1,
    "is_renting": 1,
    "is_returning": 1,
    "last_reported": 10,
    "collected_at": "2026-09-28T10:00:00Z",
}


def test_classifies_low_bikes_at_configurable_threshold() -> None:
    result = classify_availability(BASE, low_bike_threshold=2, low_dock_threshold=2)
    assert result["is_low_bikes"] is True
    assert result["is_low_docks"] is False
    assert result["availability_status"] == "LOW_BIKES"


def test_out_of_service_station_is_not_an_operational_shortage() -> None:
    result = classify_availability({**BASE, "is_renting": 0, "num_bikes_available": 0})
    assert result["availability_status"] == "OUT_OF_SERVICE"
    assert result["is_low_bikes"] is False


def test_latest_deduplication_uses_source_then_collection_time() -> None:
    rows = [
        BASE,
        {**BASE, "num_bikes_available": 1, "last_reported": 11},
        {**BASE, "station_id": "station-2"},
    ]
    result = deduplicate_latest(rows)
    assert len(result) == 2
    assert (
        next(row for row in result if row["station_id"] == "station-1")["num_bikes_available"] == 1
    )


def test_left_join_marks_unknown_station() -> None:
    joined = join_station_information([BASE], [{"station_id": "different", "name": "Other"}])
    assert joined[0]["station_reference_matched"] is False


def test_summary_counts_each_status() -> None:
    rows = [
        classify_availability(BASE),
        classify_availability({**BASE, "station_id": "2", "num_bikes_available": 10}),
    ]
    assert summarize_availability(rows) == {"AVAILABLE": 1, "LOW_BIKES": 1}


def test_scd2_capacity_change_closes_old_and_adds_new_version() -> None:
    history = [
        {
            "station_id": "station-1",
            "name": "Example",
            "capacity": 20,
            "lat": 1.0,
            "lon": 2.0,
            "valid_from": "2026-09-01T00:00:00Z",
            "valid_to": None,
            "is_current": True,
        }
    ]
    changed = {"station_id": "station-1", "name": "Example", "capacity": 24, "lat": 1.0, "lon": 2.0}
    result = apply_scd2_station_change(history, changed, effective_at="2026-09-28T00:00:00Z")
    assert len(result) == 2
    assert result[0]["is_current"] is False
    assert result[0]["valid_to"] == "2026-09-28T00:00:00Z"
    assert result[1]["capacity"] == 24
    assert result[1]["is_current"] is True

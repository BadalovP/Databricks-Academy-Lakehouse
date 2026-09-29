from __future__ import annotations

from urbanflow.quality import split_quality, validate_observation

VALID = {
    "event_id": "event-1",
    "execution_id": "run-1",
    "station_id": "station-1",
    "num_bikes_available": 3,
    "num_docks_available": 7,
    "capacity": 12,
    "is_installed": 1,
    "is_renting": 1,
    "is_returning": 1,
    "last_reported": 1000,
    "source_last_updated": 1000,
    "collected_at": "2026-09-28T00:00:00Z",
    "source_url": "https://example.test/station_status.json",
}


def test_valid_observation_passes_all_rules() -> None:
    assert validate_observation(VALID, known_station_ids={"station-1"}) == []


def test_invalid_observation_reports_all_relevant_rules() -> None:
    failures = validate_observation(
        {**VALID, "station_id": "unknown", "num_bikes_available": -1},
        known_station_ids={"station-1"},
    )
    assert failures == ["UNKNOWN_STATION_ID", "NEGATIVE_AVAILABILITY"]


def test_invalid_capacity_type_is_quarantined() -> None:
    assert validate_observation({**VALID, "capacity": "unknown"}) == ["INVALID_CAPACITY_TYPE"]


def test_quality_routing_preserves_reconciliation() -> None:
    result = split_quality(
        [VALID, {**VALID, "station_id": "station-2", "num_docks_available": -1}],
        known_station_ids={"station-1", "station-2"},
    )
    assert len(result.valid) == 1
    assert len(result.quarantine) == 1
    assert result.reconciles is True
    assert result.rule_counts == {"NEGATIVE_AVAILABILITY": 1, "PASS": 1}


def test_stale_record_is_detected() -> None:
    assert validate_observation(VALID, now_epoch_seconds=1401, freshness_seconds=300) == [
        "STALE_OBSERVATION"
    ]

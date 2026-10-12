import math
import uuid
from datetime import UTC, datetime

import pytest

from lab11.events import (
    BRONZE_COLUMNS,
    SchemaValidationError,
    generate_events,
    to_micros,
    validate_record,
    validation_errors,
)


def test_generation_is_deterministic(config):
    assert generate_events(config.producer, "run-a") == generate_events(config.producer, "run-a")


def test_event_ids_are_stable_uuids_and_unique_within_a_run(config):
    events = generate_events(config.producer, "run-a")
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids)) == config.producer.unique_events
    assert all(uuid.UUID(i).version == 5 for i in ids)


def test_a_different_run_gets_different_event_ids(config):
    first = {e.event_id for e in generate_events(config.producer, "run-a")}
    second = {e.event_id for e in generate_events(config.producer, "run-b")}
    assert first.isdisjoint(second)


def test_every_generated_record_matches_the_contract(plan):
    for record in plan.records:
        assert list(record) == list(BRONZE_COLUMNS)
        assert validation_errors(record) == []


def test_timestamps_are_epoch_microseconds():
    assert to_micros(datetime(1970, 1, 1, 0, 0, 1, 5, tzinfo=UTC)) == 1_000_005
    with pytest.raises(ValueError):
        to_micros(datetime(2026, 1, 1))  # naive timestamps are ambiguous


@pytest.fixture
def record(plan):
    return dict(plan.records[0])


@pytest.mark.parametrize(
    ("column", "value", "fragment"),
    [
        ("event_id", None, "missing or null"),
        ("event_id", "not-a-uuid", "UUID"),
        ("device_id", "", "non-empty string"),
        ("sequence_number", "7", "64-bit integer"),
        ("sequence_number", True, "64-bit integer"),
        ("event_timestamp", 1.5, "epoch microseconds"),
        ("event_timestamp", 10, "between 2000 and 2100"),
        ("temperature", math.nan, "finite number"),
        ("temperature", "21.5", "finite number"),
        ("humidity", 140.0, "outside"),
    ],
)
def test_invalid_values_are_rejected_before_sending(record, column, value, fragment):
    record[column] = value
    with pytest.raises(SchemaValidationError, match=fragment):
        validate_record(record)


def test_missing_and_unknown_fields_are_rejected(record):
    del record["humidity"]
    record["firmware"] = "1.2"
    errors = validation_errors(record)
    assert any("humidity is missing" in e for e in errors)
    assert any("unknown fields ['firmware']" in e for e in errors)

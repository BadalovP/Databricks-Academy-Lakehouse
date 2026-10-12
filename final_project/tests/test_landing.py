"""Bounded sample landing: the Auto Loader path must produce the same Bronze rows as Event Hubs."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from tests.conftest_spark import BRONZE_DDL
from urbanflow.landing import (
    SAMPLE_HISTORICAL_ID,
    expected_sample_profile,
    landing_paths,
    sample_station_events,
    to_json_lines,
    validate_station_source,
)
from urbanflow.producer import event_id
from urbanflow.streaming import REQUIRED_EVENT_FIELDS

MOMENT = datetime(2026, 10, 12, 8, 0, tzinfo=UTC)
VOLUME = "/Volumes/workspace/urbanflow_final_dev/urbanflow_final_landing"


def _events(information_payload: dict, status_payload: dict, execution_id: str = "run-1") -> list:
    return sample_station_events(
        information_payload, status_payload, execution_id=execution_id, collected_at=MOMENT
    )


def test_sample_events_have_the_event_hubs_contract(information_payload, status_payload) -> None:
    events = _events(information_payload, status_payload)

    assert len(events) == len(status_payload["data"]["stations"]) == 40
    for event in events:
        assert all(event.get(field) is not None for field in REQUIRED_EVENT_FIELDS)
        assert event["execution_id"] == "run-1"
        assert event["event_id"] == event_id(event)
        assert event["source_url"] == "fixtures/samples/station_status.sample.json"
    assert len({event["event_id"] for event in events}) == 40


def test_sample_events_are_deterministic_for_one_collection_time(
    information_payload, status_payload
) -> None:
    assert _events(information_payload, status_payload) == _events(
        information_payload, status_payload
    )


def test_json_lines_round_trip(information_payload, status_payload) -> None:
    events = _events(information_payload, status_payload)
    text = to_json_lines(events)

    assert text.endswith("\n") and text.count("\n") == 40
    assert [json.loads(line) for line in text.splitlines()] == events


def test_landing_paths_are_scoped_to_one_volume_and_execution() -> None:
    paths = landing_paths(
        VOLUME,
        source_execution_id="urbanflow-final-123",
        historical_execution_id=SAMPLE_HISTORICAL_ID,
    )

    assert paths.station_events_dir == f"{VOLUME}/landing/station_events/urbanflow-final-123"
    assert paths.station_checkpoint.endswith(
        "/checkpoints/urbanflow_final/autoloader/urbanflow-final-123"
    )
    # The sample keeps the historical module's own namespace (one owner execution).
    assert paths.historical_dir == f"{VOLUME}/landing/historical_trips"


@pytest.mark.parametrize(
    ("volume", "execution", "historical"),
    [
        ("/tmp/landing", "run-1", SAMPLE_HISTORICAL_ID),
        (VOLUME + "/extra", "run-1", SAMPLE_HISTORICAL_ID),
        (VOLUME, "../other", SAMPLE_HISTORICAL_ID),
        (VOLUME, "run-1", "a/b"),
        (VOLUME, "run 1", SAMPLE_HISTORICAL_ID),
        (VOLUME, "run-1", ".."),
    ],
)
def test_landing_paths_refuse_anything_outside_the_volume(volume, execution, historical) -> None:
    with pytest.raises(ValueError):
        landing_paths(volume, source_execution_id=execution, historical_execution_id=historical)


def test_expected_profile_accounts_for_every_sample_station(status_payload) -> None:
    profile = expected_sample_profile(status_payload)

    assert profile["events"] == 40
    assert sum(profile["composition"].values()) == 40
    actionable = sum(
        count
        for status, count in profile["composition"].items()
        if status in {"LOW_BIKES", "LOW_DOCKS", "LOW_BIKES_AND_DOCKS"}
    )
    assert profile["actionable"] == actionable


def test_station_source_is_one_of_two_explicit_modes() -> None:
    assert validate_station_source("sample_autoloader", "run-1") == "sample_autoloader"
    assert validate_station_source("existing_bronze", "run-1") == "existing_bronze"
    with pytest.raises(ValueError, match="station_source"):
        validate_station_source("republish_event_hubs", "run-1")
    with pytest.raises(ValueError):
        validate_station_source("sample_autoloader", "")


@pytest.mark.spark
def test_landed_lines_parse_to_exactly_the_event_hubs_bronze_columns(
    tmp_path, information_payload, status_payload
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from urbanflow.streaming import (
        AUTOLOADER_LINEAGE_TOPIC,
        landed_text_as_kafka_records,
        parse_station_events,
    )

    landed = tmp_path / "events.jsonl"
    landed.write_text(to_json_lines(_events(information_payload, status_payload)), "utf-8")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-landing-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        bronze = parse_station_events(landed_text_as_kafka_records(spark.read.text(str(landed))))
        expected = [
            (name, kind)
            for name, kind in (part.strip().split(" ", 1) for part in BRONZE_DDL.split(","))
        ]
        actual = [(field.name, field.dataType.simpleString()) for field in bronze.schema.fields]
        assert actual == expected
        rows = bronze.collect()
        assert len(rows) == 40
        assert not any(row["parse_error"] for row in rows)
        assert {row["kafka_topic"] for row in rows} == {AUTOLOADER_LINEAGE_TOPIC}
        assert all(row["kafka_offset"] is None for row in rows)
    finally:
        spark.stop()


def test_station_expectations_for_sample_and_event_hubs_runs(status_payload) -> None:
    from urbanflow.landing import station_expectations

    profile = expected_sample_profile(status_payload)
    sample = station_expectations("sample_autoloader", {}, profile)
    assert sample["bronze"] == sample["silver"] == sample["fact"] == 40
    assert sample["shortage"] == sample["priority"] == profile["actionable"]

    measured = {"bronze": 2520, "silver": 2520, "fact": 2520, "shortage": 657, "priority": 657}
    assert station_expectations("existing_bronze", measured) == {
        "silver": 2520,
        "fact": 2520,
        "priority": 657,
    }
    with pytest.raises(ValueError):
        station_expectations("sample_autoloader", measured)
    with pytest.raises(ValueError):
        station_expectations("unknown", measured)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({}, (False, False)),
        ({"run_station_pipeline": "true", "station_source": "sample_autoloader"}, (True, False)),
        ({"run_station_pipeline": "true", "station_source": "existing_bronze"}, (False, False)),
        ({"run_historical": "true"}, (False, True)),
        ({"run_historical": "true", "run_full_month": "true"}, (False, False)),
        ({"run_historical": "true", "historical_landing_subdir": "202401-full"}, (False, False)),
    ],
)
def test_landing_plan_lands_only_the_development_sample(values, expected) -> None:
    from urbanflow.landing import landing_plan

    assert landing_plan(values) == expected


# --- contracts added after the first DEV run (run 304828008837365) -----------------------------

from urbanflow.landing import (  # noqa: E402
    historical_dimension_problem,
    station_bronze_failures,
)

_GOOD = {"rows": 40, "distinct_event_ids": 40, "null_event_ids": 0, "null_station_ids": 0,
         "null_execution_ids": 0}  # fmt: skip


def test_sample_bronze_contract_uses_the_sample_size_not_a_remembered_count() -> None:
    profile = {"events": 40}
    assert station_bronze_failures("sample_autoloader", _GOOD, profile) == {}
    failures = station_bronze_failures("sample_autoloader", {**_GOOD, "rows": 39}, profile)
    assert failures["rows"] == {"expected": 40, "actual": 39}
    with pytest.raises(ValueError):
        station_bronze_failures("sample_autoloader", _GOOD, None)


def test_event_hubs_bronze_contract_needs_rows_unique_ids_and_no_null_keys() -> None:
    big = {**_GOOD, "rows": 2520, "distinct_event_ids": 2520}
    assert station_bronze_failures("existing_bronze", big) == {}
    assert "rows" in station_bronze_failures("existing_bronze", {**_GOOD, "rows": 0,
                                                                  "distinct_event_ids": 0})  # fmt: skip
    assert "distinct_event_ids" in station_bronze_failures(
        "existing_bronze", {**big, "distinct_event_ids": 2519}
    )
    assert "null_station_ids" in station_bronze_failures(
        "existing_bronze", {**big, "null_station_ids": 1}
    )


def test_historical_needs_the_station_dimension_built_or_present() -> None:
    assert historical_dimension_problem(run_historical=True, run_station_pipeline=True,
                                        dimension_exists=False) is None  # fmt: skip
    assert historical_dimension_problem(run_historical=True, run_station_pipeline=False,
                                        dimension_exists=True) is None  # fmt: skip
    assert "dim_station_development_sample" in historical_dimension_problem(
        run_historical=True, run_station_pipeline=False, dimension_exists=False
    )


def test_no_platform_notebook_asserts_a_fixed_demo3_row_count(project_root) -> None:
    """Fixed counts copied from Demo 3 (2,520 events, 657 actionable) failed the first DEV run."""
    import re

    for path in sorted((project_root / "notebooks").glob("*.py")):
        code = "\n".join(
            cell
            for cell in path.read_text(encoding="utf-8").split("# COMMAND ----------")
            if "# MAGIC %md" not in cell
        )
        code = "\n".join(line for line in code.splitlines() if not line.strip().startswith("#"))
        assert not re.search(r"\b(2520|657|746|1888085|1886318)\b", code), path.name

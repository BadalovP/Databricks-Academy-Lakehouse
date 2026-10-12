"""Tests for historical trip ingestion, focused on the station join key.

The join-key mistake this guards against is subtle and silent: historical
`start_station_id` values look like numbers ("7407.13") but are GBFS `short_name`
values, not the UUID `station_id`. Joining on the UUID matches nothing and returns an
empty frame that reads like missing data rather than a bug.
"""

from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.conftest_spark import station_information_frame
from urbanflow.gold import station_dimension
from urbanflow.historical import (
    RESCUED_COLUMN,
    TRIP_COLUMNS,
    autoloader_options,
    daily_trip_demand,
    historical_quality_report,
    join_trips_to_stations,
    start_historical_available_now,
    trip_join_match_rate,
    trip_schema,
    trip_schema_hints,
    validate_historical_trips,
)

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "samples"


# --- contract and Auto Loader configuration (no Spark needed) -----------------


def test_autoloader_requires_a_schema_location() -> None:
    """The schema location IS the schema checkpoint; without it evolution cannot work."""
    with pytest.raises(ValueError, match="schema checkpoint"):
        autoloader_options(schema_location="  ")


def test_explicit_contract_uses_rescue_without_invalid_add_new_columns_pairing() -> None:
    options = autoloader_options(schema_location="/Volumes/c/s/v/_schemas/trips")

    assert options["cloudFiles.format"] == "csv"
    assert options["cloudFiles.schemaLocation"] == "/Volumes/c/s/v/_schemas/trips"
    assert options["cloudFiles.rescuedDataColumn"] == RESCUED_COLUMN
    assert options["cloudFiles.schemaEvolutionMode"] == "rescue"
    # Types come from the explicit schema, never from inference.
    assert options["cloudFiles.inferColumnTypes"] == "false"


def test_schema_evolution_can_be_disabled_explicitly() -> None:
    options = autoloader_options(schema_location="/x", schema_evolution_mode="none")
    assert options["cloudFiles.schemaEvolutionMode"] == "none"


def test_add_new_columns_rejects_an_explicit_reader_schema() -> None:
    with pytest.raises(ValueError, match="does not allow addNewColumns"):
        autoloader_options(
            schema_location="/x",
            schema_evolution_mode="addNewColumns",
            explicit_schema=True,
        )


def test_additive_evolution_uses_hints_and_keeps_station_ids_as_strings() -> None:
    options = autoloader_options(
        schema_location="/x",
        schema_evolution_mode="addNewColumns",
        explicit_schema=False,
    )

    assert options["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert "start_station_id STRING" in trip_schema_hints()
    assert "ended_at TIMESTAMP" in trip_schema_hints()


def test_historical_writer_is_available_now_and_checkpointed() -> None:
    writer = Mock()
    writer.format.return_value = writer
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    writer.toTable.return_value = "query"
    trips = SimpleNamespace(writeStream=writer)

    result = start_historical_available_now(
        trips,
        table_name="dbr_dev.schema.bronze_historical_trips",
        checkpoint_location="/Volumes/dbr_dev/schema/landing/checkpoints/historical",
    )

    assert result == "query"
    writer.option.assert_called_once_with(
        "checkpointLocation", "/Volumes/dbr_dev/schema/landing/checkpoints/historical"
    )
    writer.trigger.assert_called_once_with(availableNow=True)
    writer.toTable.assert_called_once_with("dbr_dev.schema.bronze_historical_trips")


def test_station_ids_are_typed_as_strings_not_numbers() -> None:
    """ "7407.13" inferred as a double would break the short_name join."""
    fields = {f.name: f.dataType.simpleString() for f in trip_schema().fields}

    assert fields["start_station_id"] == "string"
    assert fields["end_station_id"] == "string"
    assert fields["started_at"] == "timestamp"
    assert fields["start_lat"] == "double"


def test_trip_schema_matches_the_real_archive_header() -> None:
    """The contract must match the official CSV, checked against the saved real sample."""
    with (SAMPLE / "historical_trips_202401_sample.csv").open(newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))

    assert tuple(header) == TRIP_COLUMNS
    assert tuple(f.name for f in trip_schema().fields) == TRIP_COLUMNS


def test_real_sample_station_ids_look_like_short_names_not_uuids() -> None:
    """Documents the actual shape of the data this join relies on."""
    with (SAMPLE / "historical_trips_202401_sample.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    ids = {r["start_station_id"] for r in rows}
    assert ids, "sample must contain station ids"
    # Dotted numeric short names such as 7407.13, never 36-character UUIDs.
    assert all("-" not in i and "." in i for i in ids)
    assert all(len(i) < 12 for i in ids)


# --- the join itself (real Spark) --------------------------------------------


pytest_spark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-historical-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _trips(spark):
    # Two trips at a station present in the dimension, one at a retired station.
    return spark.sql(
        "SELECT * FROM VALUES "
        "('r1','classic_bike',TIMESTAMP'2024-01-24 09:03:33',TIMESTAMP'2024-01-24 09:13:33',"
        "'E 102 St & 1 Ave','7407.13','x','7463.09',40.78,-73.94,40.79,-73.94,'member'), "
        "('r2','electric_bike',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 10:20:00',"
        "'E 102 St & 1 Ave','7407.13','x','7463.09',40.78,-73.94,40.79,-73.94,'casual'), "
        "('r3','classic_bike',TIMESTAMP'2024-01-25 08:00:00',TIMESTAMP'2024-01-25 08:10:00',"
        "'Retired Depot','9999.99','x','7463.09',40.70,-73.90,40.71,-73.91,'member') "
        "AS t(ride_id, rideable_type, started_at, ended_at, start_station_name, "
        "start_station_id, end_station_name, end_station_id, start_lat, start_lng, "
        "end_lat, end_lng, member_casual)"
    )


@pytest.mark.spark
def test_trips_join_to_current_stations_via_short_name(spark_session) -> None:
    dim = station_dimension(station_information_frame(spark_session))

    joined = join_trips_to_stations(_trips(spark_session), dim)

    rows = {r["ride_id"]: r for r in joined.collect()}
    assert rows["r1"]["start_station_uuid"] == "0138452a-b9f4-4aee-80b0-7fae9a122ffe"
    assert rows["r1"]["current_station_name"] == "E 102 St & 1 Ave"
    assert rows["r1"]["station_matched"] is True
    # A retired station legitimately has no current match and is KEPT, not dropped.
    assert rows["r3"]["start_station_uuid"] is None
    assert rows["r3"]["station_matched"] is False


@pytest.mark.spark
def test_joining_on_the_uuid_instead_would_match_nothing(spark_session) -> None:
    """Proves the failure mode this module exists to prevent.

    Joining historical start_station_id onto the GBFS UUID station_id yields zero
    matches, which is why the correct key is short_name.
    """
    dim = station_dimension(station_information_frame(spark_session))
    trips = _trips(spark_session)

    wrong = trips.join(
        dim.select("station_id"), trips["start_station_id"] == dim["station_id"], "inner"
    )

    assert wrong.count() == 0


@pytest.mark.spark
def test_historical_quality_report_accounts_for_rescued_and_invalid_rows(spark_session) -> None:
    trips = spark_session.sql(
        "SELECT * FROM VALUES "
        "('ok',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00',"
        "'7407.13','member',CAST(NULL AS STRING)), "
        "('bad',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 09:00:00',"
        "'7407.13','visitor','{\"new_column\":\"x\"}') "
        "AS t(ride_id, started_at, ended_at, start_station_id, member_casual, _rescued_data)"
    )

    report = historical_quality_report(validate_historical_trips(trips))

    assert report == {
        "rows": 2,
        "valid_rows": 1,
        "rejected_rows": 1,
        "rescued_rows": 1,
        "duplicate_ride_ids": 0,
        "counts_reconcile": True,
    }


@pytest.mark.spark
def test_missing_ride_ids_are_rejected_but_not_misreported_as_duplicates(spark_session) -> None:
    trips = spark_session.sql(
        "SELECT * FROM VALUES "
        "(CAST(NULL AS STRING),TIMESTAMP'2024-01-24 09:00:00',"
        "TIMESTAMP'2024-01-24 09:10:00','7407.13','member'), "
        "('dup',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00',"
        "'7407.13','member'), "
        "('dup',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 10:10:00',"
        "'7407.13','member') "
        "AS t(ride_id, started_at, ended_at, start_station_id, member_casual)"
    )

    report = historical_quality_report(validate_historical_trips(trips))

    assert report["rejected_rows"] == 1
    assert report["duplicate_ride_ids"] == 1


@pytest.mark.spark
def test_match_rate_makes_a_bad_join_loud(spark_session) -> None:
    dim = station_dimension(station_information_frame(spark_session))
    joined = join_trips_to_stations(_trips(spark_session), dim)

    rate = trip_join_match_rate(joined)

    assert rate["trips"] == 3
    assert rate["matched_to_current_station"] == 2
    assert rate["unmatched"] == 1
    assert rate["match_rate"] == pytest.approx(0.6667, abs=1e-4)


@pytest.mark.spark
def test_daily_trip_demand_is_real_history_unlike_the_availability_summary(
    spark_session,
) -> None:
    """One monthly archive contains weeks of real rides, so this IS a genuine trend."""
    dim = station_dimension(station_information_frame(spark_session))
    joined = join_trips_to_stations(_trips(spark_session), dim)

    rows = {
        (r["station_short_name"], str(r["trip_date"])): r
        for r in daily_trip_demand(joined).collect()
    }

    busy = rows[("7407.13", "2024-01-24")]
    assert busy["trips_started"] == 2
    assert busy["member_trips"] == 1
    assert busy["casual_trips"] == 1
    assert busy["avg_trip_minutes"] == pytest.approx(15.0)
    assert ("9999.99", "2024-01-25") in rows  # retired station still reported

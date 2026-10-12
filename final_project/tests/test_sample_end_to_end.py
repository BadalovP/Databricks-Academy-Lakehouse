"""End-to-end validation of the historical and weather paths against the REAL committed samples.

Every other test in this project drives these functions with synthetic frames built from SQL
`VALUES`. This file instead reads the actual committed files - the 40-row Citi Bike January 2024
trip sample and the 48-hour Open-Meteo archive response - and pushes them through the same
functions the authorized Azure runs will call.

That matters for a specific reason. A synthetic fixture is written by the same person who wrote
the code, so it tends to agree with the code's assumptions. The committed samples were taken
from the real sources, so they carry the real quirks: `start_station_id` values like "7407.13"
that look numeric, timestamps in the archive's own format, and parallel weather arrays. The
checks below are the same ones notebooks 06 and 07 perform, run locally so a failure costs
nothing.

What this does NOT prove: Auto Loader itself, the `availableNow` trigger and the Delta MERGE
statements, none of which run locally. Those are exercised only by the live run. What it does
prove is that the schema, the split, the join key, the reconciliation identities and the weather
alignment all behave correctly on real data.

LABELLING: 40 rows is a DEVELOPMENT SAMPLE, not January 2024. Nothing here may be described as
full-month historical data.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from urbanflow.gold import station_dimension
from urbanflow.historical import (
    daily_trip_demand,
    join_trips_to_stations,
    rider_mix_summary,
    split_trips_and_quarantine,
    trip_duration_profile,
    trip_join_match_rate,
    trip_schema,
    with_trip_lineage,
)
from urbanflow.weather import (
    WeatherRequest,
    enrich_trips_with_weather,
    parse_hourly_payload,
    reconcile_weather_join,
    weather_coverage,
)

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
TRIP_SAMPLE = SAMPLES / "historical_trips_202401_sample.csv"
STATION_SAMPLE = SAMPLES / "station_information.sample.json"
WEATHER_SAMPLE = SAMPLES / "open_meteo_archive_202401.sample.json"

# The sample's own size, asserted rather than assumed.
SAMPLE_ROWS = 40
EXECUTION = "urbanflow-hist-sample-local"


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-sample-end-to-end")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def trips(spark_session):
    """The committed sample read with the EXPLICIT contract, as the notebook reads it."""
    return (
        spark_session.read.schema(trip_schema())
        .option("header", "true")
        .csv(TRIP_SAMPLE.as_posix())
    )


# --- the sample itself ----------------------------------------------------------


def test_the_committed_sample_is_exactly_40_development_rows() -> None:
    """A changed sample size would silently invalidate every expectation below."""
    lines = TRIP_SAMPLE.read_text(encoding="utf-8").strip().splitlines()

    assert len(lines) - 1 == SAMPLE_ROWS  # minus the header
    assert lines[0].startswith("ride_id,rideable_type,started_at,ended_at")


@pytest.mark.spark
def test_the_explicit_schema_keeps_station_ids_as_strings(trips) -> None:
    """`7407.13` is a GBFS short name. Typed as a number, the join key is destroyed."""
    from pyspark.sql import types as T

    fields = {field.name: field.dataType for field in trips.schema.fields}

    assert isinstance(fields["start_station_id"], T.StringType)
    assert isinstance(fields["end_station_id"], T.StringType)
    values = {row["start_station_id"] for row in trips.collect()}
    assert "7407.13" in values  # the exact string, trailing digits intact


@pytest.mark.spark
def test_inferring_the_schema_would_destroy_the_join_key(spark_session, trips) -> None:
    """The failure this contract prevents, measured on the real file.

    Inference types `start_station_id` as a double. Most values survive a round-trip by luck -
    "7407.13" really does come back as "7407.13" - but any identifier whose decimal part ends
    in zero does not: this sample contains `5470.10` and `6740.10`, which become `5470.1` and
    `6740.1` and then match no GBFS short_name at all.

    That is what makes the bug so unpleasant. Inference does not fail, and it does not corrupt
    everything. It corrupts a small minority of keys, so the join still returns a plausible
    number of rows and the loss looks like genuinely missing stations.
    """
    from pyspark.sql import types as T

    inferred = (
        spark_session.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(TRIP_SAMPLE.as_posix())
    )

    field = {f.name: f.dataType for f in inferred.schema.fields}["start_station_id"]
    assert isinstance(field, T.DoubleType)  # NOT a string

    def _ids(frame: Any, column: str) -> set[str]:
        return {
            str(row[column]) for row in frame.select(column).collect() if row[column] is not None
        }

    contracted = _ids(trips, "start_station_id") | _ids(trips, "end_station_id")
    destroyed = _ids(inferred, "start_station_id") | _ids(inferred, "end_station_id")

    # The exact identifiers inference loses, named rather than described.
    lost = contracted - destroyed
    assert lost == {"5470.10", "6740.10"}
    # And the explicit contract preserves every one of them.
    assert {"5470.10", "6740.10"} <= contracted


@pytest.mark.spark
def test_every_sample_row_is_readable_with_no_corrupt_records(trips) -> None:
    assert trips.count() == SAMPLE_ROWS
    assert trips.where("ride_id IS NULL").count() == 0
    assert trips.where("started_at IS NULL OR ended_at IS NULL").count() == 0


# --- the three-way split, on real data ------------------------------------------


@pytest.fixture(scope="module")
def split(trips):
    stamped = with_trip_lineage(trips, execution_id=EXECUTION, include_source_file=False)
    return split_trips_and_quarantine(stamped, min_trip_seconds=60, max_trip_hours=24)


@pytest.mark.spark
def test_landed_rows_equal_valid_plus_quarantine_plus_duplicates(trips, split) -> None:
    """The reconciliation identity, on the real sample. No row may silently disappear."""
    valid = split["valid"].count()
    quarantine = split["quarantine"].count()
    duplicates = split["duplicates"].count()

    assert valid + quarantine + duplicates == trips.count() == SAMPLE_ROWS
    # The real sample is clean, so everything should pass the contract; if a future sample
    # refresh introduces a bad row this asserts the counts still add up rather than the
    # specific split.
    assert valid > 0


@pytest.mark.spark
def test_ride_ids_are_unique_in_the_valid_set(split) -> None:
    valid = split["valid"]

    assert valid.count() == valid.select("ride_id").distinct().count()


@pytest.mark.spark
def test_duration_rules_actually_ran_against_the_real_sample(split) -> None:
    """Proves the rules were applied, not merely available."""
    profile = trip_duration_profile(split["valid"])

    assert profile["trips"] == split["valid"].count()
    # Every surviving trip sits inside the configured bounds, because the rest quarantine.
    assert profile["min_minutes"] >= 1.0
    assert profile["max_minutes"] <= 24 * 60


@pytest.mark.spark
def test_every_quarantined_row_carries_a_reason(split) -> None:
    quarantine = split["quarantine"]

    if quarantine.count():
        assert quarantine.where("failed_rules IS NULL OR size(failed_rules) = 0").count() == 0


@pytest.mark.spark
def test_lineage_is_stamped_on_every_valid_row(split) -> None:
    assert {row["execution_id"] for row in split["valid"].collect()} == {EXECUTION}


# --- the join that the whole module exists to get right -------------------------


@pytest.fixture(scope="module")
def dimension(spark_session):
    """The station dimension built from the committed GBFS reference sample."""
    raw = (
        spark_session.read.option("multiLine", "true")
        .json(STATION_SAMPLE.as_posix())
        .selectExpr("explode(data.stations) AS station")
        .select("station.*")
    )
    return station_dimension(raw)


@pytest.mark.spark
def test_trips_match_current_stations_through_short_name(trips, split, dimension) -> None:
    """A real, non-zero match rate on real data, using short_name as the key."""
    joined = join_trips_to_stations(split["valid"], dimension)
    report = trip_join_match_rate(joined)

    assert report["trips"] == split["valid"].count()
    assert report["matched_to_current_station"] > 0
    assert 0.0 < report["match_rate"] <= 1.0
    # A partial rate is honest, not a defect: the dimension is a 40-station sample.
    assert report["matched_to_current_station"] + report["unmatched"] == report["trips"]


@pytest.mark.spark
def test_joining_on_the_uuid_instead_matches_nothing_on_real_data(split, dimension) -> None:
    """The wrong-key failure, demonstrated against the committed files."""
    valid = split["valid"]

    wrong = valid.join(
        dimension.select("station_id"),
        valid["start_station_id"] == dimension["station_id"],
        "inner",
    )

    assert wrong.count() == 0


@pytest.mark.spark
def test_a_zero_match_rate_would_fail_the_run(split, dimension) -> None:
    """Confirms the gate the notebook relies on actually trips."""
    from urbanflow.historical import reconcile_historical

    empty_dimension = dimension.where("1=0")
    joined = join_trips_to_stations(split["valid"], empty_dimension)
    report = trip_join_match_rate(joined)

    assert report["match_rate"] == 0.0
    reconciliation = reconcile_historical(
        landed_rows=SAMPLE_ROWS,
        valid_rows=split["valid"].count(),
        quarantine_rows=split["quarantine"].count(),
        duplicate_rows=split["duplicates"].count(),
        match_rate=report,
        demand_rows=0,
    )
    assert reconciliation["join_produced_no_matches"] is True
    assert reconciliation["status"] == "FAIL"


# --- demand and rider mix reconcile back to the valid trips ---------------------


@pytest.mark.spark
def test_daily_demand_reconciles_to_the_valid_trip_count(split, dimension) -> None:
    """Aggregates must account for every valid trip exactly once."""
    joined = join_trips_to_stations(split["valid"], dimension)
    demand = daily_trip_demand(joined, execution_id=EXECUTION)

    total = sum(row["trips_started"] for row in demand.collect())
    assert total == split["valid"].count()
    assert {row["execution_id"] for row in demand.collect()} == {EXECUTION}


@pytest.mark.spark
def test_member_and_casual_counts_reconcile_within_demand(split, dimension) -> None:
    joined = join_trips_to_stations(split["valid"], dimension)

    for row in daily_trip_demand(joined).collect():
        assert row["member_trips"] + row["casual_trips"] == row["trips_started"]


@pytest.mark.spark
def test_rider_mix_totals_match_the_valid_trips(split, dimension) -> None:
    joined = join_trips_to_stations(split["valid"], dimension)

    rows = rider_mix_summary(joined).collect()
    assert sum(row["trips"] for row in rows) == split["valid"].count()
    for row in rows:
        assert row["member_trips"] + row["casual_trips"] == row["trips"]


@pytest.mark.spark
def test_the_full_reconciliation_passes_on_the_real_sample(trips, split, dimension) -> None:
    """The exact gate notebook 06 applies, run against the committed files."""
    from urbanflow.historical import reconcile_historical

    joined = join_trips_to_stations(split["valid"], dimension)
    demand = daily_trip_demand(joined, execution_id=EXECUTION)

    reconciliation = reconcile_historical(
        landed_rows=trips.count(),
        valid_rows=split["valid"].count(),
        quarantine_rows=split["quarantine"].count(),
        duplicate_rows=split["duplicates"].count(),
        match_rate=trip_join_match_rate(joined),
        demand_rows=demand.count(),
    )

    assert reconciliation["status"] == "PASS"
    assert reconciliation["accounted_rows"] == SAMPLE_ROWS
    assert reconciliation["counts_reconcile"] is True


# --- the weather sample, parsed and joined --------------------------------------


def _weather_request() -> WeatherRequest:
    return WeatherRequest(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2024-01-24",
        end_date="2024-01-25",
    )


def test_the_committed_weather_sample_parses_to_48_aligned_hours() -> None:
    payload = json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8"))

    records = parse_hourly_payload(payload, _weather_request())

    assert len(records) == 48
    for index in (0, 11, 23, 47):
        assert records[index]["weather_hour"] == payload["hourly"]["time"][index]
        assert records[index]["temperature_celsius"] == payload["hourly"]["temperature_2m"][index]
        assert records[index]["precipitation_mm"] == payload["hourly"]["precipitation"][index]
        assert records[index]["wind_speed_kmh"] == payload["hourly"]["wind_speed_10m"][index]


def test_the_weather_sample_types_are_floats_or_none() -> None:
    records = parse_hourly_payload(
        json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8")), _weather_request()
    )

    for record in records:
        for field in ("temperature_celsius", "precipitation_mm", "wind_speed_kmh"):
            assert record[field] is None or isinstance(record[field], float)
        assert isinstance(record["has_temperature"], bool)


def test_coverage_of_the_committed_weather_sample_is_reported_explicitly() -> None:
    records = parse_hourly_payload(
        json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8")), _weather_request()
    )

    coverage = weather_coverage(records)

    assert coverage["hours"] == 48
    assert coverage["hours_with_temperature"] == 48
    assert coverage["complete"] is True


def test_a_null_hour_in_the_real_sample_stays_null() -> None:
    """Zero degrees and 'unknown' are different facts even on real data."""
    payload = json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8"))
    payload["hourly"]["temperature_2m"][6] = None

    records = parse_hourly_payload(payload, _weather_request())

    assert records[6]["temperature_celsius"] is None
    assert records[6]["has_temperature"] is False
    assert weather_coverage(records)["missing_temperature"] == 1


@pytest.mark.spark
def test_joining_real_weather_to_real_trips_preserves_every_trip(spark_session, split) -> None:
    """The fan-out check, on both real samples at once.

    The trip sample is January 2024 and the weather sample covers 2024-01-24/25, so only the
    trips in those two days find an hour. That partial coverage is the realistic case and is
    reported rather than hidden.
    """
    records = parse_hourly_payload(
        json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8")), _weather_request()
    )
    weather = _weather_frame_from_sql(spark_session, records)
    valid = split["valid"]

    enriched = enrich_trips_with_weather(valid, weather)
    matched = enriched.where("has_weather").count()
    report = reconcile_weather_join(enriched.count(), valid.count(), matched)

    assert report["rows_preserved"] is True
    assert report["enriched_rows"] == valid.count()
    assert report["status"] == "PASS"
    # Coverage is stated, whatever it happens to be.
    assert 0.0 <= report["weather_coverage"] <= 1.0


def _weather_frame_from_sql(spark: Any, records: list[dict[str, Any]]) -> Any:
    """Build the weather frame through Spark SQL rather than `createDataFrame`.

    `createDataFrame` from a Python list routes every row through the local Python worker,
    which resets its socket partway through on this platform. The production path uses
    `weather_frame`, which is fine on a cluster; here the same typed shape is produced in SQL
    so the join under test is exercised without that failure mode.
    """
    values = ", ".join(
        "(TIMESTAMP'{hour}', {temp}, {precip}, {wind}, '{label}')".format(
            hour=record["weather_hour"].replace("T", " "),
            temp=_sql_double(record["temperature_celsius"]),
            precip=_sql_double(record["precipitation_mm"]),
            wind=_sql_double(record["wind_speed_kmh"]),
            label=record["weather_grid_label"],
        )
        for record in records
    )
    return spark.sql(
        "SELECT weather_hour, CAST(temperature_celsius AS DOUBLE) AS temperature_celsius, "
        "CAST(precipitation_mm AS DOUBLE) AS precipitation_mm, "
        "CAST(wind_speed_kmh AS DOUBLE) AS wind_speed_kmh, weather_grid_label "
        f"FROM VALUES {values} AS t(weather_hour, temperature_celsius, precipitation_mm, "
        "wind_speed_kmh, weather_grid_label)"
    )


def _sql_double(value: float | None) -> str:
    return "CAST(NULL AS DOUBLE)" if value is None else repr(float(value))


@pytest.mark.spark
def test_a_duplicated_real_weather_hour_would_multiply_trips_and_is_caught(
    spark_session, split
) -> None:
    """The dangerous outcome is not a missing row but a multiplied one."""
    records = parse_hourly_payload(
        json.loads(WEATHER_SAMPLE.read_text(encoding="utf-8")), _weather_request()
    )
    weather = _weather_frame_from_sql(spark_session, records)
    valid = split["valid"]

    enriched = enrich_trips_with_weather(valid, weather.unionAll(weather))
    report = reconcile_weather_join(enriched.count(), valid.count(), enriched.count())

    assert report["rows_preserved"] is False
    assert report["status"] == "FAIL"

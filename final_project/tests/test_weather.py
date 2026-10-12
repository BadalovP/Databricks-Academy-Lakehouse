"""Tests for Open-Meteo retrieval and the weather enrichment of trip demand.

The payload fixture is a REAL Open-Meteo archive response for 2024-01-24/25 at the configured
New York coordinate, committed at `data/samples/open_meteo_archive_202401.sample.json`. Using
the genuine response shape matters: the API returns parallel arrays rather than a list of
objects, and a test built on an invented shape would not catch a positional misalignment,
which is the main way this parser could go wrong while still looking correct.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from urbanflow.weather import (
    ARCHIVE_URL,
    HOURLY_VARIABLES,
    WeatherRequest,
    build_archive_url,
    enrich_trips_with_weather,
    fetch_hourly_weather,
    parse_hourly_payload,
    reconcile_weather_join,
    span_days,
    truncate_to_hour,
    validate_request_size,
    weather_coverage,
    weather_demand_summary,
)

SAMPLE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "samples"
    / "open_meteo_archive_202401.sample.json"
)
LATITUDE = 40.7128
LONGITUDE = -74.0060


def _request(**overrides: Any) -> WeatherRequest:
    defaults = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": "2024-01-24",
        "end_date": "2024-01-25",
    }
    defaults.update(overrides)
    return WeatherRequest(**defaults)


def _payload() -> dict[str, Any]:
    return json.loads(SAMPLE.read_text(encoding="utf-8"))


# --- the request contract -------------------------------------------------------


def test_the_committed_sample_is_a_real_archive_response() -> None:
    """Guards the fixture itself: a 'current' response would silently lack hourly arrays."""
    payload = _payload()

    assert "hourly" in payload
    assert payload["hourly"]["time"][0] == "2024-01-24T00:00"
    assert len(payload["hourly"]["time"]) == 48  # two full days, hourly
    for variable in HOURLY_VARIABLES:
        assert len(payload["hourly"][variable]) == 48
    # The units confirm the column names used downstream are not a guess.
    units = payload["hourly_units"]
    assert units["precipitation"] == "mm"
    assert units["wind_speed_10m"] == "km/h"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"latitude": 91.0}, "latitude"),
        ({"longitude": -181.0}, "longitude"),
        ({"start_date": "24-01-2024"}, "ISO date"),
        ({"end_date": "not-a-date"}, "ISO date"),
        ({"start_date": "2024-01-25", "end_date": "2024-01-24"}, "precedes"),
        ({"hourly": ()}, "At least one hourly variable"),
    ],
)
def test_invalid_requests_are_refused(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _request(**overrides)


def test_span_and_size_bound_are_inclusive_of_both_dates() -> None:
    assert span_days(_request()) == 2
    assert validate_request_size(_request()) == 2


def test_an_oversized_window_is_refused_before_any_network_call() -> None:
    wide = _request(start_date="2024-01-01", end_date="2024-12-31")

    with pytest.raises(ValueError, match="above the 62-day bound"):
        validate_request_size(wide)


def test_the_request_url_states_exactly_what_was_asked_for() -> None:
    url = build_archive_url(_request())

    assert url.startswith(ARCHIVE_URL)
    assert "start_date=2024-01-24" in url
    assert "end_date=2024-01-25" in url
    assert "temperature_2m" in url and "precipitation" in url and "wind_speed_10m" in url
    assert "timezone=UTC" in url


def test_the_grid_label_names_the_single_coordinate_the_readings_describe() -> None:
    """City-level weather must not be presented as per-station weather."""
    assert _request().grid_label == "open-meteo@40.7128,-74.0060"


# --- parsing the real payload ---------------------------------------------------


def test_parsing_the_real_payload_yields_one_record_per_hour() -> None:
    records = parse_hourly_payload(_payload(), _request())

    assert len(records) == 48
    first = records[0]
    assert first["weather_hour"] == "2024-01-24T00:00"
    assert first["temperature_celsius"] == 0.7
    assert first["precipitation_mm"] == 0.0
    assert first["wind_speed_kmh"] == 5.5
    assert first["has_temperature"] is True
    assert first["source"] == "open-meteo-archive"
    assert first["weather_grid_label"] == "open-meteo@40.7128,-74.0060"


def test_values_stay_aligned_with_their_own_hour() -> None:
    """Positional misalignment is the failure this parser most needs to avoid."""
    payload = _payload()
    records = parse_hourly_payload(payload, _request())

    for index in (0, 7, 23, 47):
        assert records[index]["weather_hour"] == payload["hourly"]["time"][index]
        assert records[index]["temperature_celsius"] == payload["hourly"]["temperature_2m"][index]


def test_a_short_variable_array_is_refused_rather_than_aligned_positionally() -> None:
    """A truncated array would shift every later reading onto the wrong hour."""
    payload = _payload()
    payload["hourly"]["temperature_2m"] = payload["hourly"]["temperature_2m"][:10]

    with pytest.raises(ValueError, match="refusing to align them positionally"):
        parse_hourly_payload(payload, _request())


def test_a_missing_requested_variable_is_refused() -> None:
    payload = _payload()
    del payload["hourly"]["precipitation"]

    with pytest.raises(ValueError, match="missing the requested variable"):
        parse_hourly_payload(payload, _request())


@pytest.mark.parametrize("mutation", ["drop_hourly", "drop_time"])
def test_a_malformed_payload_is_refused(mutation: str) -> None:
    payload = _payload()
    if mutation == "drop_hourly":
        del payload["hourly"]
        expected = "missing its 'hourly' object"
    else:
        del payload["hourly"]["time"]
        expected = "missing its 'hourly.time' array"

    with pytest.raises(ValueError, match=expected):
        parse_hourly_payload(payload, _request())


def test_a_null_reading_stays_null_and_is_never_replaced_with_zero() -> None:
    """Zero degrees and 'unknown' are different facts; conflating them invents data."""
    payload = _payload()
    payload["hourly"]["temperature_2m"][3] = None
    payload["hourly"]["precipitation"][3] = None

    records = parse_hourly_payload(payload, _request())

    assert records[3]["temperature_celsius"] is None
    assert records[3]["has_temperature"] is False
    assert records[3]["precipitation_mm"] is None
    assert records[3]["has_precipitation"] is False
    # The surrounding hours are untouched, so a gap is a gap and not a shift.
    assert records[2]["temperature_celsius"] == payload["hourly"]["temperature_2m"][2]
    assert records[4]["temperature_celsius"] == payload["hourly"]["temperature_2m"][4]


def test_coverage_counts_the_gaps_instead_of_hiding_them() -> None:
    payload = _payload()
    for index in (1, 2, 3):
        payload["hourly"]["temperature_2m"][index] = None
    records = parse_hourly_payload(payload, _request())

    coverage = weather_coverage(records)

    assert coverage["hours"] == 48
    assert coverage["hours_with_temperature"] == 45
    assert coverage["missing_temperature"] == 3
    assert coverage["temperature_completeness"] == pytest.approx(45 / 48, abs=1e-4)
    assert coverage["complete"] is False


def test_coverage_of_the_untouched_real_sample_is_complete() -> None:
    coverage = weather_coverage(parse_hourly_payload(_payload(), _request()))

    assert coverage["complete"] is True
    assert coverage["missing_temperature"] == 0


# --- the bounded fetch, without a network call ----------------------------------


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


def test_fetch_validates_the_window_before_opening_a_connection() -> None:
    calls: list[str] = []

    def _opener(url: str, timeout: float) -> _FakeResponse:
        calls.append(url)
        return _FakeResponse(_payload())

    with pytest.raises(ValueError, match="above the 62-day bound"):
        fetch_hourly_weather(
            _request(start_date="2024-01-01", end_date="2024-12-31"), opener=_opener
        )

    assert calls == []  # nothing was requested


def test_fetch_parses_a_real_payload_through_the_injected_opener() -> None:
    seen: list[str] = []

    def _opener(url: str, timeout: float) -> _FakeResponse:
        seen.append(url)
        return _FakeResponse(_payload())

    records = fetch_hourly_weather(_request(), opener=_opener)

    assert len(records) == 48
    assert len(seen) == 1
    assert seen[0].startswith(ARCHIVE_URL)


# --- joining weather to trips ---------------------------------------------------


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-weather-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _trips(spark: Any) -> Any:
    """Three trips: two inside a covered hour, one in an hour the series does not cover."""
    return spark.sql(
        "SELECT * FROM VALUES "
        "('r1',TIMESTAMP'2024-01-24 00:47:00',TIMESTAMP'2024-01-24 01:02:00','member'), "
        "('r2',TIMESTAMP'2024-01-24 00:05:00',TIMESTAMP'2024-01-24 00:25:00','casual'), "
        "('r3',TIMESTAMP'2024-01-26 09:00:00',TIMESTAMP'2024-01-26 09:20:00','member') "
        "AS t(ride_id, started_at, ended_at, member_casual)"
    )


def _weather(spark: Any) -> Any:
    """The hourly series, typed as `weather_frame` types it.

    The DOUBLE casts are not cosmetic. A bare `0.7` in a SQL VALUES clause is parsed as
    DECIMAL, so without them the fixture would exercise a type the production frame never
    produces, and arithmetic in the summary would behave differently from the real thing.
    """
    return spark.sql(
        "SELECT weather_hour, "
        "CAST(temperature_celsius AS DOUBLE) AS temperature_celsius, "
        "CAST(precipitation_mm AS DOUBLE) AS precipitation_mm, "
        "CAST(wind_speed_kmh AS DOUBLE) AS wind_speed_kmh, "
        "weather_grid_label FROM VALUES "
        "(TIMESTAMP'2024-01-24 00:00:00',0.7,0.0,5.5,'open-meteo@40.7128,-74.0060'), "
        "(TIMESTAMP'2024-01-24 01:00:00',0.6,2.5,4.1,'open-meteo@40.7128,-74.0060') "
        "AS t(weather_hour, temperature_celsius, precipitation_mm, wind_speed_kmh, "
        "weather_grid_label)"
    )


@pytest.mark.spark
def test_a_trip_joins_to_the_hour_it_started_in(spark_session) -> None:
    """09:47 belongs to the 09:00 observation: a documented rounding, not interpolation."""
    enriched = enrich_trips_with_weather(_trips(spark_session), _weather(spark_session))

    rows = {row["ride_id"]: row for row in enriched.collect()}
    assert rows["r1"]["weather_hour"].hour == 0  # started 00:47, joined to 00:00
    assert rows["r1"]["temperature_celsius"] == 0.7
    assert rows["r2"]["temperature_celsius"] == 0.7
    assert rows["r1"]["has_weather"] is True


@pytest.mark.spark
def test_an_uncovered_hour_keeps_the_trip_and_reports_the_gap(spark_session) -> None:
    """Losing real rides to a weather gap would corrupt demand to flatter the weather."""
    enriched = enrich_trips_with_weather(_trips(spark_session), _weather(spark_session))

    rows = {row["ride_id"]: row for row in enriched.collect()}
    assert "r3" in rows  # kept
    assert rows["r3"]["temperature_celsius"] is None
    assert rows["r3"]["has_weather"] is False


@pytest.mark.spark
def test_truncating_to_the_hour_discards_only_minutes_and_seconds(spark_session) -> None:
    truncated = truncate_to_hour(_trips(spark_session), "started_at", alias="trip_hour")

    hours = {row["ride_id"]: row["trip_hour"] for row in truncated.collect()}
    assert hours["r1"].minute == 0 and hours["r1"].second == 0
    assert hours["r1"].hour == 0
    assert hours["r3"].hour == 9


@pytest.mark.spark
def test_the_join_neither_drops_nor_multiplies_a_trip(spark_session) -> None:
    trips = _trips(spark_session)
    enriched = enrich_trips_with_weather(trips, _weather(spark_session))
    matched = enriched.where("has_weather").count()

    report = reconcile_weather_join(enriched.count(), trips.count(), matched)

    assert report["trip_rows"] == 3
    assert report["enriched_rows"] == 3
    assert report["trips_with_weather"] == 2
    assert report["trips_without_weather"] == 1
    assert report["rows_preserved"] is True
    assert report["status"] == "PASS"


@pytest.mark.spark
def test_a_duplicated_weather_hour_multiplies_trips_and_is_caught(spark_session) -> None:
    """A duplicate hour inflates demand while every single number still looks plausible."""
    trips = _trips(spark_session)
    doubled = _weather(spark_session).unionAll(_weather(spark_session))

    enriched = enrich_trips_with_weather(trips, doubled)
    report = reconcile_weather_join(enriched.count(), trips.count(), enriched.count())

    assert enriched.count() > trips.count()
    assert report["rows_preserved"] is False
    assert report["status"] == "FAIL"


@pytest.mark.spark
def test_weather_demand_summary_buckets_temperature_and_sums_rainfall(spark_session) -> None:
    enriched = enrich_trips_with_weather(_trips(spark_session), _weather(spark_session))

    rows = {
        (str(row["trip_date"]), row["temperature_bucket"]): row
        for row in weather_demand_summary(enriched).collect()
    }

    covered = rows[("2024-01-24", "COLD_0_10C")]
    assert covered["trips"] == 2
    # The schema is stable whether or not an execution id is supplied.
    assert "execution_id" in covered.asDict()
    assert covered["avg_temperature_celsius"] == pytest.approx(0.7)
    # Precipitation is SUMMED because a day has a total rainfall, not an average one.
    assert covered["total_precipitation_mm"] == pytest.approx(0.0)
    assert covered["weather_coverage"] == pytest.approx(1.0)
    # The uncovered trip is reported under UNKNOWN rather than silently bucketed as cold.
    assert ("2024-01-26", "UNKNOWN") in rows
    assert rows[("2024-01-26", "UNKNOWN")]["trips_with_weather"] == 0


@pytest.mark.spark
def test_the_demand_summary_carries_the_execution_id_when_given_one(spark_session) -> None:
    enriched = enrich_trips_with_weather(_trips(spark_session), _weather(spark_session))

    summary = weather_demand_summary(enriched, execution_id="urbanflow-weather-r1")

    assert {row["execution_id"] for row in summary.collect()} == {"urbanflow-weather-r1"}
    assert weather_demand_summary(enriched).columns == summary.columns


def test_weather_persistence_requires_both_table_names() -> None:
    from unittest.mock import Mock

    from urbanflow.weather import persist_weather_outputs

    with pytest.raises(ValueError, match="Missing weather table names"):
        persist_weather_outputs(
            Mock(),
            weather=Mock(),
            demand_summary=Mock(),
            table_names={"weather": "cat.sch.dim_weather_hourly"},
            execution_id="r1",
        )


def test_the_derived_summary_is_execution_scoped_while_the_dimension_is_merged(
    monkeypatch,
) -> None:
    """A day whose trips all move to quarantine must lose its summary row."""
    from unittest.mock import Mock

    from urbanflow import persistence
    from urbanflow.weather import persist_weather_outputs

    merged: list[str] = []
    scoped: list[str] = []
    monkeypatch.setattr(persistence, "evolve_delta_schema", lambda *a, **k: {})
    monkeypatch.setattr(
        persistence,
        "merge_delta_table",
        lambda spark, frame, table, **k: merged.append(table) or {},
    )
    monkeypatch.setattr(
        persistence,
        "replace_execution_scope",
        lambda spark, frame, table, **k: scoped.append(table) or {},
    )

    persist_weather_outputs(
        Mock(),
        weather=Mock(),
        demand_summary=Mock(),
        table_names={
            "weather": "cat.sch.dim_weather_hourly",
            "demand_summary": "cat.sch.gold_weather_demand",
        },
        execution_id="r1",
    )

    assert merged == ["cat.sch.dim_weather_hourly"]
    assert scoped == ["cat.sch.gold_weather_demand"]

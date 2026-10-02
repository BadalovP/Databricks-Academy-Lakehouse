"""Tests for the Great Expectations suites.

These run the real framework against real local Spark DataFrames. The important property is not
that the suites pass on good data - that is easy and proves little - but that they FAIL on the
specific defects this project has actually produced: a duplicated event id, a negative count, an
unknown availability status, and a station identifier typed as a number instead of a string.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("great_expectations")

from urbanflow.expectations import (  # noqa: E402 - after the skip guard
    KNOWN_AVAILABILITY_STATUSES,
    SILVER_SUITE,
    TRIP_SUITE,
    silver_expectations,
    trip_expectations,
    validate_frame,
)


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-expectations-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _silver(spark: Any, *, rows: str) -> Any:
    return spark.sql(
        "SELECT event_id, execution_id, station_id, observed_at, "
        "CAST(num_bikes_available AS INT) AS num_bikes_available, "
        "CAST(num_docks_available AS INT) AS num_docks_available, "
        "availability_status, is_operational FROM VALUES "
        + rows
        + " AS t(event_id, execution_id, station_id, observed_at, num_bikes_available, "
        "num_docks_available, availability_status, is_operational)"
    )


GOOD_SILVER = (
    "('e1','r1','st-1',TIMESTAMP'2026-09-29 19:50:43',5,9,'AVAILABLE',true), "
    "('e2','r1','st-2',TIMESTAMP'2026-09-29 19:50:43',0,0,'OUT_OF_SERVICE',false)"
)


@pytest.mark.spark
def test_the_silver_suite_passes_on_contract_compliant_rows(spark_session) -> None:
    report = validate_frame(
        _silver(spark_session, rows=GOOD_SILVER),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    assert report["status"] == "PASS"
    assert report["failed_expectations"] == 0
    assert report["evaluated_expectations"] == len(silver_expectations())
    assert report["framework"] == "great_expectations"


@pytest.mark.spark
def test_a_duplicated_event_id_fails_the_suite(spark_session) -> None:
    """The Silver key must be unique; the MERGE that writes it depends on that."""
    duplicated = GOOD_SILVER.replace("('e2'", "('e1'")

    report = validate_frame(
        _silver(spark_session, rows=duplicated),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    assert report["status"] == "FAIL"
    failures = {check["expectation"] for check in report["failed"]}
    assert "expect_column_values_to_be_unique" in failures


@pytest.mark.spark
def test_a_negative_bike_count_fails_the_suite(spark_session) -> None:
    negative = GOOD_SILVER.replace(",5,9,'AVAILABLE'", ",-1,9,'AVAILABLE'")

    report = validate_frame(
        _silver(spark_session, rows=negative),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    assert report["status"] == "FAIL"
    assert any(check["column"] == "num_bikes_available" for check in report["failed"])


@pytest.mark.spark
def test_an_unknown_availability_status_fails_the_suite(spark_session) -> None:
    """A new status value must be a deliberate contract change, not a silent one."""
    unknown = GOOD_SILVER.replace("'AVAILABLE'", "'PROBABLY_FINE'")

    report = validate_frame(
        _silver(spark_session, rows=unknown),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    assert report["status"] == "FAIL"
    assert any(check["column"] == "availability_status" for check in report["failed"])
    assert "PROBABLY_FINE" not in KNOWN_AVAILABILITY_STATUSES


@pytest.mark.spark
def test_a_null_execution_id_fails_the_suite(spark_session) -> None:
    """An unassigned row is what let 89 stale priority rows escape a scoped delete."""
    unassigned = GOOD_SILVER.replace("('e2','r1'", "('e2',CAST(NULL AS STRING)")

    report = validate_frame(
        _silver(spark_session, rows=unassigned),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    assert report["status"] == "FAIL"
    assert any(check["column"] == "execution_id" for check in report["failed"])


# --- the trip suite --------------------------------------------------------------


def _trips(spark: Any, *, station_expr: str = "'7407.13'") -> Any:
    return spark.sql(
        f"SELECT ride_id, started_at, ended_at, {station_expr} AS start_station_id, member_casual "
        "FROM VALUES "
        "('r1',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00','member'), "
        "('r2',TIMESTAMP'2024-01-24 10:00:00',TIMESTAMP'2024-01-24 10:20:00','casual') "
        "AS t(ride_id, started_at, ended_at, member_casual)"
    )


@pytest.mark.spark
def test_the_trip_suite_passes_on_a_well_formed_batch(spark_session) -> None:
    report = validate_frame(
        _trips(spark_session), suite_name=TRIP_SUITE, expectations=trip_expectations()
    )

    assert report["status"] == "PASS"
    assert report["failed_expectations"] == 0


@pytest.mark.spark
def test_a_numeric_station_id_fails_the_trip_suite(spark_session) -> None:
    """The schema-inference hazard, caught by an independent framework.

    A double-typed `start_station_id` loses identifiers whose decimal part ends in zero, so the
    station join quietly matches fewer rows while still looking plausible. The type expectation
    makes that a loud failure instead.
    """
    report = validate_frame(
        _trips(spark_session, station_expr="CAST(7407.13 AS DOUBLE)"),
        suite_name=TRIP_SUITE,
        expectations=trip_expectations(),
    )

    assert report["status"] == "FAIL"
    failures = {check["expectation"] for check in report["failed"]}
    assert "expect_column_values_to_be_of_type" in failures


@pytest.mark.spark
def test_an_invalid_rider_type_fails_the_trip_suite(spark_session) -> None:
    report = validate_frame(
        spark_session.sql(
            "SELECT * FROM VALUES "
            "('r1',TIMESTAMP'2024-01-24 09:00:00',TIMESTAMP'2024-01-24 09:10:00','7407.13','subscriber') "
            "AS t(ride_id, started_at, ended_at, start_station_id, member_casual)"
        ),
        suite_name=TRIP_SUITE,
        expectations=trip_expectations(),
    )

    assert report["status"] == "FAIL"
    assert any(check["column"] == "member_casual" for check in report["failed"])


# --- the suites must agree with the pipeline they validate ------------------------


def test_the_status_set_matches_the_implementation() -> None:
    """A framework that disagrees with the pipeline gives confident reports about wrong rules."""
    from urbanflow.gold import SHORTAGE_STATUSES

    assert set(SHORTAGE_STATUSES) < set(KNOWN_AVAILABILITY_STATUSES)
    assert "OUT_OF_SERVICE" in KNOWN_AVAILABILITY_STATUSES
    assert "AVAILABLE" in KNOWN_AVAILABILITY_STATUSES
    # Exactly the five the Spark and pure-Python implementations both produce.
    assert len(KNOWN_AVAILABILITY_STATUSES) == 5


def test_the_report_is_json_safe_so_it_can_join_the_existing_evidence(spark_session) -> None:
    """The report goes into the same JSON evidence files as every other phase."""
    import json

    report = validate_frame(
        _silver(spark_session, rows=GOOD_SILVER),
        suite_name=SILVER_SUITE,
        expectations=silver_expectations(),
    )

    encoded = json.dumps(report)
    assert json.loads(encoded)["status"] == "PASS"
    assert "great_expectations" in encoded


def test_soda_was_rejected_for_a_recorded_measured_reason() -> None:
    """The choice between the two frameworks must stay justified, not become folklore."""
    doc = Path(__file__).resolve().parents[1] / "docs" / "QUALITY_FRAMEWORK.md"
    text = doc.read_text(encoding="utf-8")

    assert "soda-core-spark-df" in text
    assert "pyspark>=3.4,<4.0" in text
    assert "3.5.9" in text and "4.1.1" in text  # the measured resolver outcome
    assert "great_expectations" in text or "Great Expectations" in text


def test_project_spark_constraint_includes_the_measured_engine() -> None:
    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    text = project.read_text(encoding="utf-8")

    assert '"pyspark>=3.5,<4.2"' in text

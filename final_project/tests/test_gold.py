"""Real local-Spark tests for the Gold analytics layer."""

from __future__ import annotations

import pytest

from tests.conftest_spark import bronze_frame, bronze_row, station_information_frame
from urbanflow.gold import (
    FACT_COLUMNS,
    daily_station_summary,
    rebalancing_priority,
    reconcile_gold,
    shortage_indicators,
    station_availability_fact,
    station_dimension,
)
from urbanflow.silver import split_silver_and_quarantine

pytestmark = pytest.mark.spark

UUID_A = "66dd9fa6-0aca-11e7-82f6-3863bb44ef7c"
UUID_B = "0138452a-b9f4-4aee-80b0-7fae9a122ffe"


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-gold-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _silver(spark, rows):
    return split_silver_and_quarantine(bronze_frame(spark, rows)).silver


def test_station_dimension_keeps_both_identifiers(spark_session) -> None:
    """The dimension must carry the UUID and the short_name, or historical joins break."""
    dim = station_dimension(station_information_frame(spark_session))

    assert set(dim.columns) >= {"station_id", "station_short_name", "station_name", "capacity"}
    rows = {r["station_id"]: r for r in dim.collect()}
    assert rows[UUID_A]["station_short_name"] == "6626.01"
    assert rows[UUID_A]["capacity"] == 55
    assert rows[UUID_B]["station_short_name"] == "7407.13"


def test_fact_shares_silver_grain_and_enriches_from_the_dimension(spark_session) -> None:
    silver = _silver(spark_session, [bronze_row(event_id="e1", station_id=UUID_A, bikes=3)])
    dim = station_dimension(station_information_frame(spark_session))

    fact = station_availability_fact(silver, dim)

    assert fact.count() == silver.count() == 1
    row = fact.collect()[0]
    assert row["station_name"] == "E 48 St & 5 Ave"
    assert row["observation_date"] is not None


def test_fact_keeps_observations_for_stations_missing_from_the_dimension(spark_session) -> None:
    """A reference-data gap must not silently delete a real reading."""
    silver = _silver(spark_session, [bronze_row(event_id="e1", station_id="unknown-station")])
    dim = station_dimension(station_information_frame(spark_session))

    fact = station_availability_fact(silver, dim)

    assert fact.count() == 1
    assert fact.collect()[0]["station_name"] is None


def test_fact_schema_is_identical_with_or_without_station_information(spark_session) -> None:
    silver = _silver(spark_session, [bronze_row(event_id="e1", station_id=UUID_A)])

    without_dimension = station_availability_fact(silver)
    with_dimension = station_availability_fact(
        silver, station_dimension(station_information_frame(spark_session))
    )

    assert tuple(without_dimension.columns) == FACT_COLUMNS
    assert tuple(with_dimension.columns) == FACT_COLUMNS


def test_daily_summary_flags_that_one_snapshot_is_not_a_trend(spark_session) -> None:
    """The honesty guard: a single observation per station cannot show a trend."""
    silver = _silver(
        spark_session,
        [
            bronze_row(event_id="e1", station_id=UUID_A, bikes=4, offset=1),
            bronze_row(event_id="e2", station_id=UUID_B, bikes=9, offset=2),
        ],
    )
    fact = station_availability_fact(
        silver, station_dimension(station_information_frame(spark_session))
    )

    summary = daily_station_summary(fact).collect()

    assert len(summary) == 2
    for row in summary:
        assert row["observations_per_station"] == 1
        assert row["is_trend_capable"] is False
        # With one observation min, max and average all equal the single reading.
        assert row["min_bikes_available"] == row["max_bikes_available"]


def test_daily_summary_becomes_trend_capable_with_more_observations(spark_session) -> None:
    """Two readings for one station on one day is the minimum that can show movement."""
    silver = _silver(
        spark_session,
        [
            bronze_row(event_id="e1", station_id=UUID_A, bikes=2, offset=1),
            bronze_row(event_id="e2", station_id=UUID_A, bikes=8, offset=2),
        ],
    )
    fact = station_availability_fact(silver)

    row = daily_station_summary(fact).collect()[0]

    assert row["observations_per_station"] == 2
    assert row["is_trend_capable"] is True
    assert row["min_bikes_available"] == 2
    assert row["max_bikes_available"] == 8
    assert row["avg_bikes_available"] == 5.0


def test_shortage_indicators_list_only_stations_needing_action(spark_session) -> None:
    silver = _silver(
        spark_session,
        [
            bronze_row(event_id="healthy", station_id=UUID_A, bikes=10, docks=10, offset=1),
            bronze_row(event_id="short", station_id=UUID_B, bikes=0, docks=10, offset=2),
        ],
    )
    fact = station_availability_fact(
        silver, station_dimension(station_information_frame(spark_session))
    )

    rows = shortage_indicators(fact).collect()

    assert [r["station_id"] for r in rows] == [UUID_B]
    assert rows[0]["event_id"] == "short"
    assert rows[0]["availability_status"] == "LOW_BIKES"


def test_rebalancing_priority_is_arithmetic_a_human_can_recompute(spark_session) -> None:
    """The score must be reproducible by hand from the columns displayed beside it."""
    silver = _silver(
        spark_session,
        [
            # Both sides short, large station: severity 2 + deficit 2 + size 1 = 5
            bronze_row(
                event_id="worst",
                station_id=UUID_A,
                bikes=0,
                docks=0,
                bikes_disabled=10,
                docks_disabled=10,
                offset=1,
            ),
            # Bikes short by 1, small station: severity 1 + deficit 1 + size 0 = 2
            bronze_row(event_id="mild", station_id=UUID_B, bikes=1, docks=8, offset=2),
        ],
    )
    fact = station_availability_fact(
        silver, station_dimension(station_information_frame(spark_session))
    )

    rows = rebalancing_priority(fact).collect()

    assert [r["event_id"] if "event_id" in r.asDict() else r["station_id"] for r in rows] == [
        UUID_A,
        UUID_B,
    ]
    worst, mild = rows[0], rows[1]
    assert (worst["severity_points"], worst["deficit_points"], worst["size_points"]) == (2, 2, 1)
    assert worst["priority_score"] == 5
    assert worst["action"] == "INSPECT_STATION"
    assert (mild["severity_points"], mild["deficit_points"], mild["size_points"]) == (1, 1, 0)
    assert mild["priority_score"] == 2
    assert mild["action"] == "DELIVER_BIKES"


def test_priority_action_tells_the_van_which_way_to_move_bikes(spark_session) -> None:
    silver = _silver(
        spark_session,
        [bronze_row(event_id="full", station_id=UUID_A, bikes=20, docks=0, offset=1)],
    )
    fact = station_availability_fact(silver)

    row = rebalancing_priority(fact).collect()[0]

    assert row["availability_status"] == "LOW_DOCKS"
    assert row["action"] == "COLLECT_BIKES"


def test_gold_reconciles_against_silver(spark_session) -> None:
    silver = _silver(
        spark_session,
        [
            bronze_row(event_id="e1", station_id=UUID_A, offset=1),
            bronze_row(event_id="e2", station_id=UUID_B, offset=2),
        ],
    )
    fact = station_availability_fact(silver)
    summary = daily_station_summary(fact)

    result = reconcile_gold(silver, fact, summary)

    assert result["silver_rows"] == 2
    assert result["fact_rows"] == 2
    assert result["distinct_fact_event_ids"] == 2
    assert result["summary_observation_total"] == 2
    assert result["fact_matches_silver"] is True
    assert result["summary_totals_match_fact"] is True
    assert result["status"] == "PASS"


def test_gold_reconciliation_fails_when_the_fact_loses_rows(spark_session) -> None:
    """The check must be capable of failing, not just of reporting PASS."""
    silver = _silver(
        spark_session,
        [
            bronze_row(event_id="e1", station_id=UUID_A, offset=1),
            bronze_row(event_id="e2", station_id=UUID_B, offset=2),
        ],
    )
    truncated = station_availability_fact(silver).limit(1)

    result = reconcile_gold(silver, truncated, daily_station_summary(truncated))

    assert result["fact_matches_silver"] is False
    assert result["status"] == "FAIL"

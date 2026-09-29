"""Real local-Spark tests for the physical Bronze-to-Silver layer."""

from __future__ import annotations

import pytest

from tests.conftest_spark import bronze_frame, bronze_row
from urbanflow.silver import (
    SILVER_COLUMNS,
    add_contract_result,
    freshness,
    reconcile_silver,
    split_silver_and_quarantine,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-silver-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_valid_rows_reach_silver_with_the_declared_contract(spark_session) -> None:
    bronze = bronze_frame(spark_session, [bronze_row(event_id="e1", bikes=10, docks=10)])

    split = split_silver_and_quarantine(bronze)

    assert tuple(split.silver.columns) == SILVER_COLUMNS
    row = split.silver.collect()[0]
    assert row["event_id"] == "e1"
    assert row["passed_contract"] is True
    assert row["availability_status"] == "HEALTHY"
    # capacity_estimate sums bikes, docks and both disabled counts.
    assert row["capacity_estimate"] == 20
    assert row["observed_at"] is not None


def test_each_contract_violation_is_quarantined_not_dropped(spark_session) -> None:
    """Every invalid row must land in quarantine so the counts still reconcile."""
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="ok", bikes=10, docks=10),
            bronze_row(event_id="", station_id="st-2"),  # MISSING_EVENT_ID
            bronze_row(event_id="no-station", station_id=""),  # MISSING_STATION_ID
            bronze_row(event_id="parse", parse_error="true"),  # PARSE_ERROR
            bronze_row(event_id="negative", bikes=-1),  # NEGATIVE_AVAILABILITY
            bronze_row(event_id="no-ts", source_last_updated=0),  # MISSING_SOURCE_TIMESTAMP
        ],
    )

    split = split_silver_and_quarantine(bronze)

    assert split.silver.count() == 1
    assert split.quarantine.count() == 5
    reasons = {r for row in split.quarantine.collect() for r in row["failed_rules"]}
    assert reasons == {
        "MISSING_EVENT_ID",
        "MISSING_STATION_ID",
        "PARSE_ERROR",
        "NEGATIVE_AVAILABILITY",
        "MISSING_SOURCE_TIMESTAMP",
    }
    # Quarantined rows keep the reason, so an operator can see WHY each one failed.
    assert all(len(row["failed_rules"]) >= 1 for row in split.quarantine.collect())


def test_duplicate_event_ids_keep_the_first_arrival_and_report_the_rest(spark_session) -> None:
    """A repeated event_id is a redelivery, so the earliest Kafka offset wins."""
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="dup", bikes=5, offset=100),
            bronze_row(event_id="dup", bikes=9, offset=50),  # earlier offset = first arrival
            bronze_row(event_id="unique", bikes=7, offset=10),
        ],
    )

    split = split_silver_and_quarantine(bronze)

    assert split.silver.count() == 2
    assert split.duplicates.count() == 1
    kept = {r["event_id"]: r for r in split.silver.collect()}
    assert kept["dup"]["num_bikes_available"] == 9  # the offset-50 copy survived
    assert split.duplicates.collect()[0]["num_bikes_available"] == 5


def test_duplicate_choice_uses_broker_time_before_partition_offset(spark_session) -> None:
    """Offsets are ordered only inside one partition, so broker time is the global tie-break."""
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(
                event_id="dup",
                bikes=9,
                partition=0,
                offset=1,
                kafka_timestamp="2026-09-29 19:51:50",
            ),
            bronze_row(
                event_id="dup",
                bikes=4,
                partition=1,
                offset=999,
                kafka_timestamp="2026-09-29 19:51:40",
            ),
        ],
    )

    kept = split_silver_and_quarantine(bronze).silver.collect()[0]

    assert kept["num_bikes_available"] == 4
    assert kept["kafka_partition"] == 1


def test_availability_classification_covers_the_whole_matrix(spark_session) -> None:
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="healthy", bikes=10, docks=10),
            bronze_row(event_id="low-bikes", bikes=1, docks=10),
            bronze_row(event_id="low-docks", bikes=10, docks=1),
            bronze_row(event_id="both", bikes=0, docks=0),
            bronze_row(event_id="boundary", bikes=2, docks=10),  # at the threshold
        ],
    )

    statuses = {
        r["event_id"]: r["availability_status"]
        for r in split_silver_and_quarantine(bronze).silver.collect()
    }

    assert statuses["healthy"] == "HEALTHY"
    assert statuses["low-bikes"] == "LOW_BIKES"
    assert statuses["low-docks"] == "LOW_DOCKS"
    assert statuses["both"] == "LOW_BIKES_AND_DOCKS"
    # The rule is <= threshold, so exactly at the threshold counts as short.
    assert statuses["boundary"] == "LOW_BIKES"


def test_thresholds_are_configurable(spark_session) -> None:
    bronze = bronze_frame(spark_session, [bronze_row(event_id="e1", bikes=5, docks=10)])

    default = split_silver_and_quarantine(bronze).silver.collect()[0]
    raised = split_silver_and_quarantine(bronze, low_bike_threshold=6).silver.collect()[0]

    assert default["availability_status"] == "HEALTHY"
    assert raised["availability_status"] == "LOW_BIKES"


def test_all_numeric_counts_and_station_flags_are_enforced(spark_session) -> None:
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="disabled", bikes_disabled=-1),
            bronze_row(event_id="ebike", ebikes=-1, offset=2),
            bronze_row(event_id="state", is_renting=3, offset=3),
        ],
    )

    rows = {
        row["event_id"]: row["failed_rules"]
        for row in split_silver_and_quarantine(bronze).quarantine.collect()
    }

    assert "NEGATIVE_AVAILABILITY" in rows["disabled"]
    assert "NEGATIVE_AVAILABILITY" in rows["ebike"]
    assert "INVALID_STATION_STATE" in rows["state"]


def test_known_station_contract_quarantines_only_unknown_nonblank_ids(spark_session) -> None:
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="known", station_id="st-1"),
            bronze_row(event_id="unknown", station_id="st-2", offset=2),
        ],
    )
    known = spark_session.sql("SELECT 'st-1' AS station_id")

    split = split_silver_and_quarantine(bronze, known_station_ids=known)

    assert [row["event_id"] for row in split.silver.collect()] == ["known"]
    rejected = split.quarantine.collect()[0]
    assert rejected["event_id"] == "unknown"
    assert "UNKNOWN_STATION_ID" in rejected["failed_rules"]


def test_reconciliation_accounts_for_every_bronze_row(spark_session) -> None:
    bronze = bronze_frame(
        spark_session,
        [
            bronze_row(event_id="a", offset=1),
            bronze_row(event_id="a", offset=2),  # duplicate
            bronze_row(event_id="b", offset=3),
            bronze_row(event_id="bad", parse_error="true", offset=4),
        ],
    )
    split = split_silver_and_quarantine(bronze)

    result = reconcile_silver(bronze, split)

    assert result["bronze_rows"] == 4
    assert result["silver_rows"] == 2
    assert result["quarantine_rows"] == 1
    assert result["duplicate_rows"] == 1
    assert result["accounted_rows"] == 4
    assert result["counts_reconcile"] is True
    assert result["silver_ids_unique"] is True
    assert result["status"] == "PASS"


def test_freshness_reports_age_without_failing_the_pipeline(spark_session) -> None:
    observed = 1790711443
    bronze = bronze_frame(spark_session, [bronze_row(event_id="e1", source_last_updated=observed)])
    silver = split_silver_and_quarantine(bronze).silver

    fresh = freshness(silver, now_epoch_seconds=observed + 60, max_age_seconds=3600)
    stale = freshness(silver, now_epoch_seconds=observed + 7200, max_age_seconds=3600)

    assert fresh["age_seconds"] == 60
    assert fresh["is_fresh"] is True
    assert stale["age_seconds"] == 7200
    assert stale["is_fresh"] is False
    assert stale["newest_source_last_updated"] == observed


def test_future_source_timestamp_is_not_reported_as_fresh(spark_session) -> None:
    observed = 1790711443
    bronze = bronze_frame(
        spark_session, [bronze_row(event_id="future", source_last_updated=observed)]
    )

    result = freshness(
        split_silver_and_quarantine(bronze).silver,
        now_epoch_seconds=observed - 60,
        max_age_seconds=3600,
    )

    assert result["age_seconds"] == -60
    assert result["is_fresh"] is False


def test_add_contract_result_is_inspectable_before_routing(spark_session) -> None:
    """The annotated frame must expose why a row would be quarantined."""
    bronze = bronze_frame(spark_session, [bronze_row(event_id="e1", bikes=-5)])

    annotated = add_contract_result(bronze).collect()[0]

    assert annotated["passed_contract"] is False
    assert "NEGATIVE_AVAILABILITY" in annotated["failed_rules"]

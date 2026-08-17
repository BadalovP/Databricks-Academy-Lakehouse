"""
Lab 05 — Lakeflow pipeline integration tests.

These tests use Databricks' Lakeflow pipeline testing framework:

    from pyspark.pipelines.testing import TestPipeline, test_spark

They are intentionally different from test_quality_rules.py:

- test_quality_rules.py:
    pure pytest, no pipeline required

- test_pipeline.py:
    executes selected Lakeflow pipeline targets in an isolated test context
    and validates the real declarative transformations

Important limitation
--------------------
The Lakeflow test Spark session redirects NAME-BASED table reads/writes to a
temporary test schema.

Our Bronze sources are read by PATH from a Unity Catalog Volume, so those path
reads are not mocked/redirection-safe. They remain READ-ONLY in these tests and
use the small Lab 05 source files already prepared under the Lab 05 Volume.

Pipeline outputs are still evaluated through the testing framework.

Run these tests from the Lakeflow Pipelines Editor, not as a normal Python
file on general-purpose/serverless notebook compute.

The pipeline must use:
    continuous = false
    channel = PREVIEW

before running this testing framework.
"""

from __future__ import annotations

import pytest
from pyspark.pipelines.testing import (
    TestPipeline,
    test_spark,
)
from pyspark.sql import functions as F


# Reference the pipeline currently open in the Lakeflow Pipelines Editor.
test_pipeline = TestPipeline.active()


def _catalog(session) -> str:
    """Resolve the pipeline catalog from Spark configuration."""
    return session.conf.get(
        "lab05.catalog",
        "dbr_dev",
    )


def _schema(session) -> str:
    """Resolve the pipeline schema from Spark configuration."""
    return session.conf.get(
        "lab05.schema",
        "parvinbadalov",
    )


def _table(session, table_name: str) -> str:
    """Return a fully qualified pipeline dataset name."""
    return (
        f"{_catalog(session)}."
        f"{_schema(session)}."
        f"{table_name}"
    )


def _run_target(session, table_name: str) -> str:
    """
    Selectively execute a pipeline target and its required dependencies.

    Returns the fully qualified target name for convenient reuse.
    """
    target = _table(session, table_name)

    test_pipeline.run(
        session,
        {target},
    )

    return target


def test_station_status_silver_quality(test_spark) -> None:
    """
    The streaming Silver status table should contain valid station-level rows.

    Production drop expectations should leave no:
    - null station_id
    - negative bike counts
    - negative dock counts
    """
    target = _run_target(
        test_spark,
        "station_status_silver",
    )

    result = test_spark.table(target)

    assert result.count() > 0

    assert (
        result
        .filter(F.col("station_id").isNull())
        .count()
        == 0
    )

    assert (
        result
        .filter(F.col("num_bikes_available") < 0)
        .count()
        == 0
    )

    assert (
        result
        .filter(F.col("num_docks_available") < 0)
        .count()
        == 0
    )


def test_station_status_record_id_is_unique(
    test_spark,
) -> None:
    """
    status_record_id represents one station observation within one snapshot.

    It is generated from:
        _source_file + station_id
    """
    target = _run_target(
        test_spark,
        "station_status_silver",
    )

    result = test_spark.table(target)

    duplicate_ids = (
        result
        .groupBy("status_record_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_ids == 0


def test_station_information_silver_quality(
    test_spark,
) -> None:
    """
    The reference Silver dataset should contain one usable row per station.
    """
    target = _run_target(
        test_spark,
        "station_information_silver",
    )

    result = test_spark.table(target)

    assert result.count() > 0

    assert (
        result
        .filter(F.col("station_id").isNull())
        .count()
        == 0
    )

    assert (
        result
        .filter(
            F.col("latitude").isNotNull()
            & (
                (F.col("latitude") < -90)
                | (F.col("latitude") > 90)
            )
        )
        .count()
        == 0
    )

    assert (
        result
        .filter(
            F.col("longitude").isNotNull()
            & (
                (F.col("longitude") < -180)
                | (F.col("longitude") > 180)
            )
        )
        .count()
        == 0
    )

    assert (
        result
        .filter(F.col("capacity") < 0)
        .count()
        == 0
    )


def test_station_information_station_id_is_unique(
    test_spark,
) -> None:
    """The current station reference should have one row per station_id."""
    target = _run_target(
        test_spark,
        "station_information_silver",
    )

    result = test_spark.table(target)

    duplicates = (
        result
        .groupBy("station_id")
        .count()
        .filter(
            F.col("station_id").isNotNull()
            & (F.col("count") > 1)
        )
        .count()
    )

    assert duplicates == 0


def test_enriched_silver_join_matches_reference(
    test_spark,
) -> None:
    """
    Validate the real declarative stream-static enrichment.

    Source preparation already showed 100% station_id coverage. The pipeline
    output should therefore contain no unmatched reference rows.
    """
    target = _run_target(
        test_spark,
        "station_status_enriched_silver",
    )

    result = test_spark.table(target)

    assert result.count() > 0

    unmatched = (
        result
        .filter(
            F.col("station_reference_matched")
            == F.lit(False)
        )
        .count()
    )

    assert unmatched == 0

    required_columns = {
        "station_id",
        "station_name",
        "capacity",
        "num_bikes_available",
        "num_docks_available",
        "bike_availability_pct",
        "dock_availability_pct",
        "last_reported_at",
    }

    assert required_columns.issubset(
        set(result.columns)
    )


def test_gold_summary_is_business_ready(
    test_spark,
) -> None:
    """
    Validate the analytical Gold materialized view.

    Every Gold row should represent an aggregated station result with a
    positive observation count and a populated availability band.
    """
    target = _run_target(
        test_spark,
        "station_summary_gold",
    )

    result = test_spark.table(target)

    assert result.count() > 0

    assert (
        result
        .filter(F.col("observation_count") <= 0)
        .count()
        == 0
    )

    assert (
        result
        .filter(F.col("availability_band").isNull())
        .count()
        == 0
    )

    assert (
        result
        .filter(
            F.col(
                "unmatched_reference_observations"
            )
            > 0
        )
        .count()
        == 0
    )


def test_gold_observation_counts_reconcile_with_silver(
    test_spark,
) -> None:
    """
    Gold observation_count should reconcile to the enriched Silver history.

    This validates the aggregation rather than checking only that Gold exists.
    """
    gold_target = _run_target(
        test_spark,
        "station_summary_gold",
    )

    enriched_name = _table(
        test_spark,
        "station_status_enriched_silver",
    )

    gold = test_spark.table(gold_target)
    enriched = test_spark.table(enriched_name)

    silver_count = enriched.count()

    gold_observation_count = (
        gold
        .agg(
            F.sum("observation_count").alias(
                "observations"
            )
        )
        .collect()[0]["observations"]
    )

    assert gold_observation_count == silver_count

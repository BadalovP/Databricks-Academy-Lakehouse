"""Integration-test the published ETL pipeline outputs.

The assertions protect the learning summary and quality status consumed by
the output-validation Job task and dashboard refresh.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    """Provide the active Databricks Spark session for table assertions."""

    return SparkSession.builder.getOrCreate()


def test_learning_pipeline_outputs(spark_session):
    """Verify the published learning metrics and PASS quality outcome."""

    # Arrange: identify the published summary and quality-status tables.
    summary = (
        spark_session.table(
            "dbr_dev.parvinbadalov.learning_pipeline_summary"
        )
        .first()
    )

    assert summary is not None

    # Assert: the fixed development-data contract matches the pipeline output.
    assert summary["order_item_rows"] == 112650
    assert summary["distinct_orders"] == 98666
    assert float(summary["total_value"]) == pytest.approx(
        15843553.24,
        abs=0.01,
    )

    quality = (
        spark_session.table(
            "dbr_dev.parvinbadalov.learning_quality_status"
        )
        .first()
    )

    # Assert: failed quality status must block downstream dashboard publication.
    assert quality is not None
    assert quality["quality_status"] == "PASS"
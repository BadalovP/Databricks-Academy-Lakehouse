# Purpose: integration-test the published ETL pipeline outputs.

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_learning_pipeline_outputs(spark_session):
    summary = (
        spark_session.table(
            "dbr_dev.parvinbadalov.learning_pipeline_summary"
        )
        .first()
    )

    assert summary is not None
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

    assert quality is not None
    assert quality["quality_status"] == "PASS"
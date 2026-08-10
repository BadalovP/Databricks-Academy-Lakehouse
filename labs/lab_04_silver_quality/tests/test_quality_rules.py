"""Unit tests for the reusable Lab 04 Online Retail quality rules."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.quality_rules import (
    EXPECTED_SOURCE_COLUMNS,
    apply_online_retail_quality_rules,
    assert_required_columns,
    build_quality_metrics,
    build_rule_failure_summary,
    split_valid_and_quarantine,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Reuse Databricks Spark, or create a local session for pytest."""

    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        return active_session

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("lab04-quality-rules-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def retail_row(**overrides):
    """Return one valid source row with selected fields overridden."""

    row = {
        "InvoiceNo": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": 6,
        "InvoiceDate": datetime(2010, 12, 1, 8, 26),
        "UnitPrice": 2.55,
        "CustomerID": "17850",
        "Country": "United Kingdom",
        "_source_row_number": 1,
    }

    row.update(overrides)
    return row


def test_valid_row_passes_all_rules(spark):
    source_df = spark.createDataFrame([retail_row()])

    result = apply_online_retail_quality_rules(
        source_df,
        contract_version="v1",
    ).first()

    assert result["_quality_status"] == "VALID"
    assert result["_quality_rule_count"] == 0
    assert result["_quality_reasons"] == []
    assert result["_quality_contract_version"] == "v1"


def test_invalid_row_records_every_failure_reason(spark):
    source_df = spark.createDataFrame(
        [
            retail_row(
                InvoiceNo="C536366",
                Description="   ",
                Quantity=-1,
                UnitPrice=0.0,
                CustomerID="",
            )
        ]
    )

    result = apply_online_retail_quality_rules(source_df).first()
    reasons = set(result["_quality_reasons"])

    assert result["_quality_status"] == "REJECTED"

    assert {
        "MISSING_DESCRIPTION",
        "NON_POSITIVE_QUANTITY",
        "NON_POSITIVE_UNIT_PRICE",
        "MISSING_CUSTOMER_ID",
        "CANCELLED_INVOICE",
    }.issubset(reasons)

    assert result["_quality_rule_count"] == len(reasons)
    
def test_duplicate_business_row_is_quarantined(spark):
    source_df = spark.createDataFrame(
        [
            retail_row(_source_row_number=1),
            retail_row(_source_row_number=2),
        ]
    )

    quality_df = apply_online_retail_quality_rules(source_df)

    results = {
        row["_source_row_number"]: row
        for row in quality_df.collect()
    }

    assert results[1]["_quality_status"] == "VALID"
    assert results[2]["_quality_status"] == "REJECTED"
    assert (
        "DUPLICATE_BUSINESS_ROW"
        in results[2]["_quality_reasons"]
    )


def test_split_and_metrics_reconcile(spark):
    source_df = spark.createDataFrame(
        [
            retail_row(_source_row_number=1),
            retail_row(
                InvoiceNo="C536367",
                _source_row_number=2,
            ),
        ]
    )

    quality_df = apply_online_retail_quality_rules(source_df)

    valid_df, quarantine_df = split_valid_and_quarantine(
        quality_df
    )

    metrics = (
        build_quality_metrics(quality_df)
        .first()
        .asDict()
    )

    assert valid_df.count() == 1
    assert quarantine_df.count() == 1
    assert metrics["input_rows"] == 2
    assert metrics["valid_rows"] == 1
    assert metrics["rejected_rows"] == 1
    assert metrics["cancelled_rows"] == 1


def test_rule_failure_summary_counts_failures(spark):
    source_df = spark.createDataFrame(
        [
            retail_row(
                Quantity=0,
                _source_row_number=1,
            ),
            retail_row(
                Quantity=-2,
                _source_row_number=2,
            ),
        ]
    )

    summary = {
        row["quality_rule"]: row["failed_rows"]
        for row in build_rule_failure_summary(
            apply_online_retail_quality_rules(source_df)
        ).collect()
    }

    assert summary["NON_POSITIVE_QUANTITY"] == 2


def test_missing_required_column_raises_clear_error(spark):
    incomplete_df = spark.createDataFrame(
        [("536365",)],
        ["InvoiceNo"],
    )

    with pytest.raises(
        ValueError,
        match="missing required columns",
    ):
        assert_required_columns(
            incomplete_df,
            EXPECTED_SOURCE_COLUMNS,
        )


def test_split_requires_quality_metadata(spark):
    raw_df = spark.createDataFrame(
        [retail_row()]
    )

    with pytest.raises(
        ValueError,
        match="Quality rules must be applied",
    ):
        split_valid_and_quarantine(raw_df)    
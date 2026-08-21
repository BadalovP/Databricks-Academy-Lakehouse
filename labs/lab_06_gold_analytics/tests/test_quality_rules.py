from src.quality_rules import (
    duplicate_grain_group_count,
    key_quality_metrics,
    null_count,
    reconciliation_passes,
)


def test_key_quality_metrics_detect_duplicates_and_nulls(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "c"), (None, "d")],
        ["business_key", "value"],
    )

    metrics = key_quality_metrics(df, "business_key")

    assert metrics["row_count"] == 4
    assert metrics["distinct_key_count"] == 2
    assert metrics["null_key_count"] == 1


def test_duplicate_grain_group_count(spark):
    df = spark.createDataFrame(
        [("A", 1), ("A", 1), ("A", 2), ("B", 1)],
        ["code", "version"],
    )

    assert duplicate_grain_group_count(
        df,
        ["code", "version"],
    ) == 1


def test_null_count(spark):
    df = spark.createDataFrame(
        [(1,), (None,), (2,)],
        ["id"],
    )

    assert null_count(df, "id") == 1


def test_reconciliation_passes():
    assert reconciliation_passes(100, 100) is True
    assert reconciliation_passes(100, 99) is False

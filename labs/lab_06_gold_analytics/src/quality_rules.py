"""Reusable validation helpers for Lab 06."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def key_quality_metrics(df: DataFrame, key_column: str) -> dict:
    """Return row, distinct-key, and null-key counts for a key column."""
    row = (
        df.agg(
            F.count("*").alias("row_count"),
            F.countDistinct(key_column).alias("distinct_key_count"),
            F.sum(
                F.when(F.col(key_column).isNull(), 1).otherwise(0)
            ).alias("null_key_count"),
        )
        .first()
    )

    return {
        "row_count": int(row["row_count"]),
        "distinct_key_count": int(row["distinct_key_count"]),
        "null_key_count": int(row["null_key_count"] or 0),
    }


def duplicate_grain_group_count(
    df: DataFrame,
    grain_columns: list[str],
) -> int:
    """Count grain groups that occur more than once."""
    return (
        df.groupBy(*grain_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def null_count(df: DataFrame, column_name: str) -> int:
    """Count null values for one column."""
    return df.filter(F.col(column_name).isNull()).count()


def reconciliation_passes(expected_count: int, actual_count: int) -> bool:
    """Return True when source and target counts reconcile."""
    return int(expected_count) == int(actual_count)

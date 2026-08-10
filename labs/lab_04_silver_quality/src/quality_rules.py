"""Reusable data-quality rules for the Lab 04 Online Retail Silver layer.

These functions accept Spark DataFrames and return Spark DataFrames. They can
therefore be reused by notebooks, Databricks Jobs, and automated tests.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


EXPECTED_SOURCE_COLUMNS: tuple[str, ...] = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)


def assert_required_columns(
    df: DataFrame,
    required_columns: Sequence[str] = EXPECTED_SOURCE_COLUMNS,
) -> None:
    """Raise an error when required source columns are missing."""

    missing_columns = sorted(
        set(required_columns) - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "The incoming DataFrame is missing required columns: "
            + ", ".join(missing_columns)
        )


def _is_blank(column_name: str):
    """Return a condition for null or whitespace-only values."""

    value = F.col(column_name).cast("string")

    return (
        value.isNull()
        | (F.length(F.trim(value)) == 0)
    )


def _with_duplicate_rank(
    df: DataFrame,
    business_columns: Sequence[str],
) -> DataFrame:
    """Rank duplicate business records deterministically."""

    if "_source_row_number" in df.columns:
        order_columns = [
            F.col("_source_row_number").asc_nulls_last()
        ]

        if "_bronze_record_id" in df.columns:
            order_columns.append(
                F.col("_bronze_record_id")
            )

    elif "_bronze_record_id" in df.columns:
        order_columns = [
            F.col("_bronze_record_id")
        ]

    else:
        stable_values = [
            F.coalesce(
                F.col(name).cast("string"),
                F.lit("<NULL>"),
            )
            for name in business_columns
        ]

        order_columns = [
            F.sha2(
                F.concat_ws("||", *stable_values),
                256,
            )
        ]

    duplicate_window = (
        Window
        .partitionBy(*business_columns)
        .orderBy(*order_columns)
    )

    return df.withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window),
    )


def apply_online_retail_quality_rules(
    df: DataFrame,
    *,
    contract_version: str = "v1",
    business_columns: Sequence[str] = EXPECTED_SOURCE_COLUMNS,
) -> DataFrame:
    """Apply Online Retail quality rules and preserve rejection reasons."""

    assert_required_columns(
        df,
        business_columns,
    )

    ranked_df = _with_duplicate_rank(
        df,
        business_columns,
    )

    rule_results = [
        F.when(
            _is_blank("InvoiceNo"),
            F.lit("MISSING_INVOICE_NO"),
        ),
        F.when(
            _is_blank("StockCode"),
            F.lit("MISSING_STOCK_CODE"),
        ),
        F.when(
            _is_blank("Description"),
            F.lit("MISSING_DESCRIPTION"),
        ),
        F.when(
            F.col("Quantity").isNull()
            | (F.col("Quantity") <= 0),
            F.lit("NON_POSITIVE_QUANTITY"),
        ),
        F.when(
            F.col("InvoiceDate").isNull(),
            F.lit("MISSING_INVOICE_DATE"),
        ),
        F.when(
            F.col("UnitPrice").isNull()
            | (F.col("UnitPrice") <= 0),
            F.lit("NON_POSITIVE_UNIT_PRICE"),
        ),
        F.when(
            _is_blank("CustomerID"),
            F.lit("MISSING_CUSTOMER_ID"),
        ),
        F.when(
            _is_blank("Country"),
            F.lit("MISSING_COUNTRY"),
        ),
        F.when(
            F.upper(
                F.trim(
                    F.col("InvoiceNo").cast("string")
                )
            ).startswith("C"),
            F.lit("CANCELLED_INVOICE"),
        ),
        F.when(
            F.col("_duplicate_rank") > 1,
            F.lit("DUPLICATE_BUSINESS_ROW"),
        ),
    ]

    return (
        ranked_df
        .withColumn(
            "_quality_rule_results",
            F.array(*rule_results),
        )
        .withColumn(
            "_quality_reasons",
            F.filter(
                F.col("_quality_rule_results"),
                lambda reason: reason.isNotNull(),
            ),
        )
        .drop("_quality_rule_results")
        .withColumn(
            "_quality_rule_count",
            F.size(F.col("_quality_reasons")),
        )
        .withColumn(
            "_quality_status",
            F.when(
                F.col("_quality_rule_count") == 0,
                F.lit("VALID"),
            ).otherwise(
                F.lit("REJECTED")
            ),
        )
        .withColumn(
            "_quality_contract_version",
            F.lit(contract_version),
        )
        .withColumn(
            "_quality_checked_at",
            F.current_timestamp(),
        )
    )


def split_valid_and_quarantine(
    quality_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Return valid and quarantined DataFrames."""

    required_metadata = {
        "_quality_status",
        "_quality_reasons",
    }

    missing_metadata = sorted(
        required_metadata - set(quality_df.columns)
    )

    if missing_metadata:
        raise ValueError(
            "Quality rules must be applied before splitting. "
            "Missing columns: "
            + ", ".join(missing_metadata)
        )

    valid_df = quality_df.filter(
        F.col("_quality_status") == "VALID"
    )

    quarantine_df = quality_df.filter(
        F.col("_quality_status") == "REJECTED"
    )

    return valid_df, quarantine_df


def build_rule_failure_summary(
    quality_df: DataFrame,
) -> DataFrame:
    """Return the number of rejected rows for each quality rule."""

    return (
        quality_df
        .select(
            F.explode_outer(
                "_quality_reasons"
            ).alias("quality_rule")
        )
        .filter(
            F.col("quality_rule").isNotNull()
        )
        .groupBy("quality_rule")
        .agg(
            F.count("*").alias("failed_rows")
        )
        .orderBy(
            F.col("failed_rows").desc(),
            F.col("quality_rule"),
        )
    )


def build_quality_metrics(
    quality_df: DataFrame,
) -> DataFrame:
    """Build a one-row quality reconciliation summary."""

    return quality_df.agg(
        F.count("*").alias("input_rows"),

        F.sum(
            (
                F.col("_quality_status") == "VALID"
            ).cast("long")
        ).alias("valid_rows"),

        F.sum(
            (
                F.col("_quality_status") == "REJECTED"
            ).cast("long")
        ).alias("rejected_rows"),

        F.sum(
            F.array_contains(
                "_quality_reasons",
                "CANCELLED_INVOICE",
            ).cast("long")
        ).alias("cancelled_rows"),

        F.sum(
            F.array_contains(
                "_quality_reasons",
                "MISSING_CUSTOMER_ID",
            ).cast("long")
        ).alias("missing_customer_rows"),

        F.sum(
            F.array_contains(
                "_quality_reasons",
                "DUPLICATE_BUSINESS_ROW",
            ).cast("long")
        ).alias("duplicate_rows"),
    )

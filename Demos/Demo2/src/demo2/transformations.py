"""PySpark transformations used by the Lakeflow pipeline and Chispa tests."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from demo2.config import DEMO_AS_OF_TIMESTAMP


def add_nullable_evolved_columns(df: DataFrame) -> DataFrame:
    result = df
    for name in ("sales_channel", "coupon_code"):
        if name not in result.columns:
            result = result.withColumn(name, F.lit(None).cast("string"))
    return result


def _business_column(df: DataFrame, name: str) -> Column:
    return F.col(name) if name in df.columns else F.lit(None)


def with_canonical_row_hash(df: DataFrame) -> DataFrame:
    normalized = F.struct(
        _business_column(df, "order_line_id").cast("string").alias("order_line_id"),
        _business_column(df, "order_id").cast("string").alias("order_id"),
        _business_column(df, "customer_id").cast("string").alias("customer_id"),
        _business_column(df, "product_id").cast("string").alias("product_id"),
        _business_column(df, "quantity").cast("int").alias("quantity"),
        F.format_number(_business_column(df, "unit_price").cast(T.DecimalType(18, 2)), 2).alias(
            "unit_price"
        ),
        F.format_number(_business_column(df, "discount_pct").cast(T.DecimalType(9, 4)), 4).alias(
            "discount_pct"
        ),
        F.date_format(
            F.to_utc_timestamp(_business_column(df, "order_timestamp").cast("timestamp"), "UTC"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        ).alias("order_timestamp"),
        _business_column(df, "order_status").cast("string").alias("order_status"),
        _business_column(df, "sales_channel").cast("string").alias("sales_channel"),
        _business_column(df, "coupon_code").cast("string").alias("coupon_code"),
    )
    return df.withColumn(
        "_row_hash",
        F.sha2(F.to_json(normalized, {"ignoreNullFields": "false"}), 256),
    )


def normalize_orders(df: DataFrame) -> DataFrame:
    result = add_nullable_evolved_columns(df)
    return (
        result.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast(T.DecimalType(18, 2)))
        .withColumn("discount_pct", F.col("discount_pct").cast(T.DecimalType(9, 4)))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("_batch_loaded_at", F.to_timestamp("_batch_loaded_at"))
        .withColumn("_source_generated_at", F.to_timestamp("_source_generated_at"))
    )


def rank_order_lines(df: DataFrame) -> DataFrame:
    hashed = with_canonical_row_hash(df)
    ranking = Window.partitionBy("order_line_id").orderBy(
        F.col("_source_generated_at").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
        F.col("_row_hash").desc(),
    )
    return hashed.withColumn("_duplicate_rank", F.row_number().over(ranking))


def classify_orders(
    df: DataFrame,
    customer_ids: DataFrame,
    product_ids: DataFrame,
) -> DataFrame:
    ranked = rank_order_lines(normalize_orders(df))
    customer_ref = (
        customer_ids.select("customer_id").distinct().withColumn("_known_customer", F.lit(True))
    )
    product_ref = (
        product_ids.select("product_id").distinct().withColumn("_known_product", F.lit(True))
    )
    joined = ranked.join(customer_ref, "customer_id", "left").join(
        product_ref, "product_id", "left"
    )

    quarantine = F.array_compact(
        F.array(
            F.when(F.col("_duplicate_rank") > 1, F.lit("DUPLICATE_ORDER_LINE_ID")),
            F.when(F.col("customer_id").isNull(), F.lit("CUSTOMER_ID_MISSING")),
            F.when(
                F.col("customer_id").isNotNull() & F.col("_known_customer").isNull(),
                F.lit("UNKNOWN_CUSTOMER_ID"),
            ),
            F.when(F.col("product_id").isNull(), F.lit("PRODUCT_ID_MISSING")),
            F.when(
                F.col("product_id").isNotNull() & F.col("_known_product").isNull(),
                F.lit("UNKNOWN_PRODUCT_ID"),
            ),
            F.when(
                F.col("quantity").isNull() | (F.col("quantity") <= 0),
                F.lit("NON_POSITIVE_QUANTITY"),
            ),
            F.when(
                F.col("discount_pct").isNull() | (F.col("discount_pct") > F.lit("0.50")),
                F.lit("INVALID_DISCOUNT"),
            ),
            F.when(
                F.col("order_timestamp") > F.to_timestamp(F.lit(DEMO_AS_OF_TIMESTAMP)),
                F.lit("FUTURE_ORDER_TIMESTAMP"),
            ),
        )
    )
    warnings = F.array_compact(
        F.array(
            F.when(
                (F.col("discount_pct") > F.lit("0.30")) & (F.col("discount_pct") <= F.lit("0.50")),
                F.lit("HIGH_DISCOUNT"),
            )
        )
    )
    dimensions = F.array_distinct(
        F.array_compact(
            F.array(
                F.when(
                    F.array_contains(quarantine, "DUPLICATE_ORDER_LINE_ID"), F.lit("uniqueness")
                ),
                F.when(
                    F.exists(
                        quarantine,
                        lambda value: value.isin("CUSTOMER_ID_MISSING", "PRODUCT_ID_MISSING"),
                    ),
                    F.lit("completeness"),
                ),
                F.when(
                    F.exists(
                        quarantine,
                        lambda value: value.isin("UNKNOWN_CUSTOMER_ID", "UNKNOWN_PRODUCT_ID"),
                    ),
                    F.lit("referential_integrity"),
                ),
                F.when(
                    F.exists(
                        quarantine,
                        lambda value: value.isin("NON_POSITIVE_QUANTITY", "INVALID_DISCOUNT"),
                    )
                    | (F.size(warnings) > 0),
                    F.lit("validity"),
                ),
                F.when(F.array_contains(quarantine, "FUTURE_ORDER_TIMESTAMP"), F.lit("timeliness")),
            )
        )
    )
    gross = (F.col("quantity") * F.col("unit_price")).cast(T.DecimalType(20, 2))
    discount_amount = (gross * F.col("discount_pct")).cast(T.DecimalType(20, 2))
    return (
        joined.withColumn("_dq_warn_reasons", warnings)
        .withColumn("_dq_quarantine_reasons", quarantine)
        .withColumn("_dq_dimensions", dimensions)
        .withColumn(
            "_dq_status",
            F.when(F.size(quarantine) > 0, F.lit("QUARANTINE"))
            .when(F.size(warnings) > 0, F.lit("WARN"))
            .otherwise(F.lit("VALID")),
        )
        .withColumn("gross_amount", gross)
        .withColumn("discount_amount", discount_amount)
        .withColumn("net_amount", (gross - discount_amount).cast(T.DecimalType(20, 2)))
        .drop("_known_customer", "_known_product")
    )


def scd2_version_for_date(
    date_key: int,
    versions: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    matches = [
        version
        for version in versions
        if int(version["__START_AT"]) <= date_key
        and (version.get("__END_AT") is None or int(version["__END_AT"]) > date_key)
    ]
    if len(matches) > 1:
        raise ValueError(f"Overlapping SCD2 versions for date key {date_key}")
    return matches[0] if matches else None


def temporal_customer_join(orders: DataFrame, customers: DataFrame) -> DataFrame:
    condition = (
        (orders.customer_id == customers.customer_id)
        & (customers["__START_AT"] <= orders.date_key)
        & (customers["__END_AT"].isNull() | (customers["__END_AT"] > orders.date_key))
    )
    return orders.join(customers, condition, "left")

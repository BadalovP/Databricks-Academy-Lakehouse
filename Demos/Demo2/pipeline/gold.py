"""Gold star schema and purpose-built business/DQ aggregates."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_product")
def dim_product():
    return spark.read.table("demo2_products_validated").select(
        F.sha2(F.col("product_id"), 256).alias("product_key"),
        "product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
    )


@dp.materialized_view(name="dim_date")
def dim_date():
    return spark.sql(
        """
        SELECT
          CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
          calendar_date,
          year(calendar_date) AS calendar_year,
          month(calendar_date) AS calendar_month,
          day(calendar_date) AS calendar_day,
          date_format(calendar_date, 'EEEE') AS day_name
        FROM (
          SELECT explode(sequence(to_date('2026-08-01'), to_date('2026-09-01'), interval 1 day)) AS calendar_date
        )
        """
    )


def customer_history_with_key():
    return spark.read.table("dim_customer_scd2").withColumn(
        "customer_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("customer_id"),
                F.col("__START_AT").cast("string"),
            ),
            256,
        ),
    )


@dp.materialized_view(name="fact_order_lines")
@dp.expect_or_fail("customer_key_present", "customer_key IS NOT NULL")
@dp.expect_or_fail("product_key_present", "product_key IS NOT NULL")
@dp.expect_or_fail("date_key_present", "date_key IS NOT NULL")
def fact_order_lines():
    orders = spark.read.table("demo2_orders_validated").withColumn(
        "date_key", F.date_format("order_timestamp", "yyyyMMdd").cast("int")
    )
    customers = customer_history_with_key()
    customer_condition = (
        (orders.customer_id == customers.customer_id)
        & (customers["__START_AT"].cast("int") <= orders.date_key)
        & (customers["__END_AT"].isNull() | (customers["__END_AT"].cast("int") > orders.date_key))
    )
    products = spark.read.table("dim_product")
    dates = spark.read.table("dim_date")
    enriched = (
        orders.alias("o")
        .join(customers.alias("c"), customer_condition, "left")
        .join(products.alias("p"), F.col("o.product_id") == F.col("p.product_id"), "left")
        .join(dates.alias("d"), F.col("o.date_key") == F.col("d.date_key"), "left")
    )
    return enriched.select(
        F.col("o.order_line_id"),
        F.col("o.order_id"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("d.date_key"),
        F.col("o.customer_id"),
        F.col("o.product_id"),
        F.col("o.order_timestamp"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.discount_pct"),
        F.col("o.gross_amount"),
        F.col("o.discount_amount"),
        F.col("o.net_amount"),
        F.col("o.order_status"),
        F.col("o.sales_channel"),
        F.col("o.coupon_code"),
        F.col("o._dq_status"),
        F.col("o._source_batch_id"),
        F.col("o._batch_loaded_at"),
        F.col("o._row_hash"),
    )


@dp.materialized_view(name="demo2_sales_daily_gold")
def sales_daily():
    return (
        spark.read.table("fact_order_lines")
        .groupBy("date_key")
        .agg(
            F.sum("net_amount").alias("net_revenue"),
            F.sum("gross_amount").alias("gross_revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("items_sold"),
        )
    )


@dp.materialized_view(name="demo2_category_performance_gold")
def category_performance():
    return (
        spark.read.table("fact_order_lines")
        .alias("f")
        .join(spark.read.table("dim_product").alias("p"), "product_key")
        .groupBy("category")
        .agg(F.sum("net_amount").alias("net_revenue"), F.sum("quantity").alias("items_sold"))
    )


@dp.materialized_view(name="demo2_country_sales_gold")
def country_sales():
    facts = spark.read.table("fact_order_lines").alias("f")
    customers = customer_history_with_key().alias("c")
    return (
        facts.join(customers, "customer_key")
        .groupBy("country")
        .agg(
            F.sum("net_amount").alias("net_revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
    )


@dp.materialized_view(name="demo2_customer_segment_gold")
def customer_segment():
    facts = spark.read.table("fact_order_lines").alias("f")
    customers = customer_history_with_key().alias("c")
    return (
        facts.join(customers, "customer_key")
        .groupBy(F.col("c.loyalty_tier"))
        .agg(
            F.sum(F.col("f.net_amount")).alias("net_revenue"),
            F.countDistinct(F.col("f.customer_id")).alias("customers"),
        )
    )


@dp.materialized_view(name="demo2_dq_summary_gold")
def dq_summary():
    classified = spark.read.table("demo2_orders_classified")
    return (
        classified.groupBy("_source_batch_id", "_batch_loaded_at")
        .agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("_dq_status") == "VALID", 1).otherwise(0)).alias("valid_rows"),
            F.sum(F.when(F.col("_dq_status") == "WARN", 1).otherwise(0)).alias("warning_rows"),
            F.sum(F.when(F.col("_dq_status") == "QUARANTINE", 1).otherwise(0)).alias(
                "quarantined_rows"
            ),
        )
        .withColumn(
            "quarantine_rate_pct",
            F.round(F.col("quarantined_rows") / F.col("total_rows") * 100, 2),
        )
    )


@dp.materialized_view(name="demo2_dq_failures_by_rule_gold")
def dq_failures_by_rule():
    classified = spark.read.table("demo2_orders_classified")
    return (
        classified.select(
            "_source_batch_id",
            "_batch_loaded_at",
            "_dq_status",
            F.explode(F.concat(F.col("_dq_warn_reasons"), F.col("_dq_quarantine_reasons"))).alias(
                "rule_name"
            ),
        )
        .groupBy("_source_batch_id", "_batch_loaded_at", "_dq_status", "rule_name")
        .count()
        .withColumnRenamed("count", "affected_rows")
    )

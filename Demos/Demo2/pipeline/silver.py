"""Silver validation, deterministic deduplication, and quarantine routing."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from demo2.transformations import classify_orders


@dp.materialized_view(name="demo2_customers_validated")
@dp.expect_or_fail("valid_customer_key", "customer_id IS NOT NULL")
def customers_validated():
    return (
        spark.read.table("demo2_customers_bronze")
        .select(
            "customer_id",
            "customer_name",
            "email",
            "country",
            "city",
            "loyalty_tier",
            "snapshot_version",
            "_source_file",
            "_source_batch_id",
            "_batch_loaded_at",
            "_ingested_at",
        )
        .dropDuplicates(["customer_id", "snapshot_version"])
    )


@dp.materialized_view(name="demo2_products_validated")
@dp.expect_or_fail("valid_product_key", "product_id IS NOT NULL")
@dp.expect_or_fail("positive_product_price", "unit_price > 0")
def products_validated():
    return (
        spark.read.table("demo2_products_bronze")
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .dropDuplicates(["product_id"])
    )


@dp.materialized_view(name="demo2_orders_classified")
@dp.expect_or_fail("exclusive_dq_status", "_dq_status IN ('VALID', 'WARN', 'QUARANTINE')")
@dp.expect("order_line_id_observed", "order_line_id IS NOT NULL")
def orders_classified():
    orders = spark.read.table("demo2_orders_bronze")
    customers = spark.read.table("demo2_customers_validated")
    products = spark.read.table("demo2_products_validated")
    return classify_orders(orders, customers, products)


@dp.materialized_view(name="demo2_orders_validated")
@dp.expect_or_fail("trusted_only", "_dq_status IN ('VALID', 'WARN')")
@dp.expect_or_fail("winner_only", "_duplicate_rank = 1")
def orders_validated():
    return spark.read.table("demo2_orders_classified").filter(
        F.col("_dq_status").isin("VALID", "WARN")
    )


@dp.materialized_view(name="demo2_orders_quarantine")
@dp.expect_or_fail("quarantine_only", "_dq_status = 'QUARANTINE'")
def orders_quarantine():
    return spark.read.table("demo2_orders_classified").filter(F.col("_dq_status") == "QUARANTINE")

"""Trusted business-serving base used by the dynamic-view governance fallback."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="demo2_sales_business_base")
@dp.expect_or_fail("trusted_business_only", "_dq_status IN ('VALID', 'WARN')")
def sales_business_base():
    facts = spark.read.table("fact_order_lines").alias("f")
    customers = (
        spark.read.table("dim_customer_scd2")
        .withColumn(
            "customer_key",
            F.sha2(F.concat_ws("|", F.col("customer_id"), F.col("__START_AT").cast("string")), 256),
        )
        .alias("c")
    )
    products = spark.read.table("dim_product").alias("p")
    dates = spark.read.table("dim_date").alias("d")
    return (
        facts.join(customers, "customer_key")
        .join(products, "product_key")
        .join(dates, "date_key")
        .select(
            F.col("f.order_line_id"),
            F.col("f.order_id"),
            F.col("f.customer_key"),
            F.col("d.calendar_date").alias("order_date"),
            F.col("c.country"),
            F.col("c.city"),
            F.col("c.loyalty_tier"),
            F.col("p.category"),
            F.col("p.product_name"),
            F.col("f.sales_channel"),
            F.col("f.order_status"),
            F.col("f.quantity"),
            F.col("f.gross_amount"),
            F.col("f.discount_amount"),
            F.col("f.net_amount"),
            F.col("c.customer_name"),
            F.col("c.email"),
            F.col("f._dq_status"),
        )
    )

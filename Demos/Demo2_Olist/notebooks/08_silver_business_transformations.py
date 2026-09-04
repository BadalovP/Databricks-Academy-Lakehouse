# Databricks notebook source

# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# MAGIC %run ./00_setup
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
from pyspark.sql import functions as F

customers_silver_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_customers"
    )

    .select(
        "customer_id",
        "customer_unique_id",

        F.lpad(
            F.trim(F.col("customer_zip_code_prefix")),
            5,
            "0"
        ).alias("customer_zip_code_prefix"),

        F.initcap(
            F.trim(F.col("customer_city"))
        ).alias("customer_city"),

        F.upper(
            F.trim(F.col("customer_state"))
        ).alias("customer_state"),

        "_ingested_at"
    )

    .dropDuplicates(["customer_id"])

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    customers_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_customers"
    )
)

print(
    "silver_customers:",
    customers_silver_df.count()
)

display(customers_silver_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Clean and enrich Products

products_source_df = spark.table(
    f"{catalog}.{schema}.bronze_products"
)

category_translation_df = spark.table(
    f"{catalog}.{schema}.bronze_category_translation"
)

products_silver_df = (
    products_source_df.alias("p")

    .join(
        category_translation_df.alias("t"),
        F.col("p.product_category_name")
        == F.col("t.product_category_name"),
        "left"
    )

    .select(
        F.col("p.product_id"),

        F.col("p.product_category_name"),

        F.coalesce(
            F.col("t.product_category_name_english"),
            F.col("p.product_category_name"),
            F.lit("unknown")
        ).alias("product_category_name_english"),

        F.col("p.product_name_lenght")
        .cast("integer")
        .alias("product_name_length"),

        F.col("p.product_description_lenght")
        .cast("integer")
        .alias("product_description_length"),

        F.col("p.product_photos_qty")
        .cast("integer")
        .alias("product_photos_quantity"),

        F.col("p.product_weight_g")
        .cast("integer")
        .alias("product_weight_g"),

        F.col("p.product_length_cm")
        .cast("integer")
        .alias("product_length_cm"),

        F.col("p.product_height_cm")
        .cast("integer")
        .alias("product_height_cm"),

        F.col("p.product_width_cm")
        .cast("integer")
        .alias("product_width_cm"),

        F.col("p._ingested_at")
    )

    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm")
        * F.col("product_height_cm")
        * F.col("product_width_cm")
    )

    .dropDuplicates(["product_id"])

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    products_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_products"
    )
)

print(
    "silver_products:",
    products_silver_df.count()
)

display(products_silver_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Clean and standardize Sellers

sellers_silver_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_sellers"
    )

    .select(
        "seller_id",

        F.lpad(
            F.trim(F.col("seller_zip_code_prefix")),
            5,
            "0"
        ).alias("seller_zip_code_prefix"),

        F.initcap(
            F.trim(F.col("seller_city"))
        ).alias("seller_city"),

        F.upper(
            F.trim(F.col("seller_state"))
        ).alias("seller_state"),

        "_ingested_at"
    )

    .dropDuplicates(["seller_id"])

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    sellers_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_sellers"
    )
)

print(
    "silver_sellers:",
    sellers_silver_df.count()
)

display(sellers_silver_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Clean and aggregate Geolocations

geolocations_clean_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_geolocations"
    )

    .select(
        F.lpad(
            F.trim(F.col("geolocation_zip_code_prefix")),
            5,
            "0"
        ).alias("geolocation_zip_code_prefix"),

        F.col("geolocation_lat")
        .cast("double")
        .alias("geolocation_lat"),

        F.col("geolocation_lng")
        .cast("double")
        .alias("geolocation_lng"),

        F.initcap(
            F.trim(F.col("geolocation_city"))
        ).alias("geolocation_city"),

        F.upper(
            F.trim(F.col("geolocation_state"))
        ).alias("geolocation_state")
    )
)

geolocations_silver_df = (
    geolocations_clean_df

    .groupBy("geolocation_zip_code_prefix")

    .agg(
        F.avg("geolocation_lat")
        .alias("average_latitude"),

        F.avg("geolocation_lng")
        .alias("average_longitude"),

        F.first(
            "geolocation_city",
            True
        ).alias("geolocation_city"),

        F.first(
            "geolocation_state",
            True
        ).alias("geolocation_state"),

        F.count("*")
        .alias("source_location_count")
    )

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    geolocations_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_geolocations"
    )
)

print(
    "Bronze geolocation rows:",
    geolocations_clean_df.count()
)

print(
    "Unique Silver postal prefixes:",
    geolocations_silver_df.count()
)

display(geolocations_silver_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Aggregate payments by order

payments_summary_df = (
    spark.table(
        f"{catalog}.{schema}.silver_order_payments"
    )

    .groupBy("order_id")

    .agg(
        F.sum("payment_value")
        .alias("total_payment_value"),

        F.count("*")
        .alias("payment_transaction_count"),

        F.max("payment_installments")
        .alias("maximum_payment_installments"),

        F.concat_ws(
            ",",
            F.sort_array(
                F.collect_set("payment_type")
            )
        ).alias("payment_types")
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Aggregate reviews by order

reviews_summary_df = (
    spark.table(
        f"{catalog}.{schema}.silver_order_reviews"
    )

    .groupBy("order_id")

    .agg(
        F.round(
            F.avg("review_score"),
            2
        ).alias("average_review_score"),

        F.count("*")
        .alias("review_count"),

        F.max("review_answer_timestamp")
        .alias("last_review_answer_timestamp")
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 8
# MAGIC **Purpose:** Execute stage 8 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Enrich Orders

orders_enriched_df = (
    spark.table(
        f"{catalog}.{schema}.silver_orders"
    ).alias("o")

    .join(
        payments_summary_df.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left"
    )

    .join(
        reviews_summary_df.alias("r"),
        F.col("o.order_id") == F.col("r.order_id"),
        "left"
    )

    .select(
        "o.*",

        F.coalesce(
            F.col("p.total_payment_value"),
            F.lit(0)
        ).alias("total_payment_value"),

        F.coalesce(
            F.col("p.payment_transaction_count"),
            F.lit(0)
        ).alias("payment_transaction_count"),

        F.col("p.maximum_payment_installments"),

        F.coalesce(
            F.col("p.payment_types"),
            F.lit("no_payment")
        ).alias("payment_types"),

        F.col("r.average_review_score"),

        F.coalesce(
            F.col("r.review_count"),
            F.lit(0)
        ).alias("review_count"),

        F.col("r.last_review_answer_timestamp")
    )

    .withColumn(
        "delivery_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        )
    )

    .withColumn(
        "delivery_delay_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        )
    )

    .withColumn(
        "is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date").isNull() |
            F.col("order_estimated_delivery_date").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.col("order_delivered_customer_date")
            > F.col("order_estimated_delivery_date")
        )
    )

    .withColumn(
        "_business_transformed_at",
        F.current_timestamp()
    )
)

(
    orders_enriched_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_orders_enriched"
    )
)

print(
    "silver_orders_enriched:",
    orders_enriched_df.count()
)

display(
    orders_enriched_df.select(
        "order_id",
        "order_status",
        "total_payment_value",
        "average_review_score",
        "delivery_days",
        "delivery_delay_days",
        "is_late_delivery"
    ).limit(10)
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 9
# MAGIC **Purpose:** Execute stage 9 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Enrich Order Items

order_items_enriched_df = (
    spark.table(
        f"{catalog}.{schema}.silver_order_items"
    ).alias("i")

    .join(
        spark.table(
            f"{catalog}.{schema}.silver_orders_enriched"
        ).alias("o"),
        F.col("i.order_id") == F.col("o.order_id"),
        "left"
    )

    .join(
        spark.table(
            f"{catalog}.{schema}.silver_products"
        ).alias("p"),
        F.col("i.product_id") == F.col("p.product_id"),
        "left"
    )

    .join(
        spark.table(
            f"{catalog}.{schema}.silver_sellers"
        ).alias("s"),
        F.col("i.seller_id") == F.col("s.seller_id"),
        "left"
    )

    .select(
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        F.col("i.product_id"),
        F.col("i.seller_id"),
        F.col("i.shipping_limit_date"),
        F.col("i.price"),
        F.col("i.freight_value"),

        F.col("o.customer_id"),
        F.col("o.order_status"),
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),
        F.col("o.delivery_days"),
        F.col("o.delivery_delay_days"),
        F.col("o.is_late_delivery"),
        F.col("o.average_review_score"),

        F.col("p.product_category_name"),
        F.col("p.product_category_name_english"),
        F.col("p.product_weight_g"),
        F.col("p.product_length_cm"),
        F.col("p.product_height_cm"),
        F.col("p.product_width_cm"),
        F.col("p.product_volume_cm3"),

        F.col("s.seller_zip_code_prefix"),
        F.col("s.seller_city"),
        F.col("s.seller_state")
    )

    .withColumn(
        "item_total_value",
        F.col("price") + F.col("freight_value")
    )

    .withColumn(
        "_business_transformed_at",
        F.current_timestamp()
    )
)

(
    order_items_enriched_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_items_enriched"
    )
)

print(
    "silver_order_items_enriched:",
    order_items_enriched_df.count()
)

display(
    order_items_enriched_df.select(
        "order_id",
        "order_item_id",
        "product_category_name_english",
        "seller_state",
        "price",
        "freight_value",
        "item_total_value",
        "is_late_delivery"
    ).limit(10)
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 10
# MAGIC **Purpose:** Execute stage 10 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
business_validation_df = spark.sql(f"""
WITH validation AS (

    SELECT
        'silver_customers' AS table_name,
        99441 AS expected_rows,
        COUNT(*) AS actual_rows
    FROM {catalog}.{schema}.silver_customers

    UNION ALL

    SELECT
        'silver_products',
        32951,
        COUNT(*)
    FROM {catalog}.{schema}.silver_products

    UNION ALL

    SELECT
        'silver_sellers',
        3095,
        COUNT(*)
    FROM {catalog}.{schema}.silver_sellers

    UNION ALL

    SELECT
        'silver_geolocations',
        19015,
        COUNT(*)
    FROM {catalog}.{schema}.silver_geolocations

    UNION ALL

    SELECT
        'silver_orders_enriched',
        99441,
        COUNT(*)
    FROM {catalog}.{schema}.silver_orders_enriched

    UNION ALL

    SELECT
        'silver_order_items_enriched',
        112650,
        COUNT(*)
    FROM {catalog}.{schema}.silver_order_items_enriched
)

SELECT
    *,
    CASE
        WHEN expected_rows = actual_rows
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM validation
ORDER BY table_name
""")

display(business_validation_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 08 Silver Business Transformations: stage 11
# MAGIC **Purpose:** Execute stage 11 of the 08 Silver Business Transformations workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
join_validation_df = spark.sql(f"""
SELECT
    COUNT(*) AS total_items,

    SUM(
        CASE WHEN customer_id IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_order_matches,

    SUM(
        CASE WHEN product_category_name_english IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_product_matches,

    SUM(
        CASE WHEN seller_state IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_seller_matches

FROM {catalog}.{schema}.silver_order_items_enriched
""")

display(join_validation_df)

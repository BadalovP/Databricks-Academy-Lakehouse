# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 07 Silver Deduplication workflow.
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
# MAGIC ### 07 Silver Deduplication: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
duplicate_summary_df = spark.sql(f"""
SELECT
    'orders' AS table_name,
    COUNT(*) AS duplicate_groups,
    COALESCE(SUM(record_count - 1), 0) AS duplicate_rows
FROM (
    SELECT
        order_id,
        COUNT(*) AS record_count
    FROM {catalog}.{schema}.silver_orders_quality
    WHERE _quality_status = 'VALID'
    GROUP BY order_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'order_items',
    COUNT(*),
    COALESCE(SUM(record_count - 1), 0)
FROM (
    SELECT
        order_id,
        order_item_id,
        COUNT(*) AS record_count
    FROM {catalog}.{schema}.silver_order_items_quality
    WHERE _quality_status = 'VALID'
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'order_payments',
    COUNT(*),
    COALESCE(SUM(record_count - 1), 0)
FROM (
    SELECT
        order_id,
        payment_sequential,
        COUNT(*) AS record_count
    FROM {catalog}.{schema}.silver_order_payments_quality
    WHERE _quality_status = 'VALID'
    GROUP BY order_id, payment_sequential
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'order_reviews',
    COUNT(*),
    COALESCE(SUM(record_count - 1), 0)
FROM (
    SELECT
        review_id,
        order_id,
        COUNT(*) AS record_count
    FROM {catalog}.{schema}.silver_order_reviews_quality
    WHERE _quality_status = 'VALID'
    GROUP BY review_id, order_id
    HAVING COUNT(*) > 1
)
""")

display(duplicate_summary_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Deduplicate Orders and create quarantine table
import pyspark.sql.functions as F



orders_source_df = spark.table(
    f"{catalog}.{schema}.silver_orders_quality"
)

orders_quarantine_df = (
    orders_source_df
    .filter(F.col("_quality_status") == "INVALID")
)

orders_silver_df = (
    orders_source_df
    .filter(F.col("_quality_status") == "VALID")
    .dropDuplicates(["order_id"])
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    orders_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_orders"
    )
)

(
    orders_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.quarantine_orders"
    )
)

print("Source rows:", orders_source_df.count())
print("Silver rows:", orders_silver_df.count())
print("Quarantine rows:", orders_quarantine_df.count())
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Deduplicate Order Items and create quarantine table

order_items_source_df = spark.table(
    f"{catalog}.{schema}.silver_order_items_quality"
)

order_items_quarantine_df = (
    order_items_source_df
    .filter(F.col("_quality_status") == "INVALID")
)

order_items_silver_df = (
    order_items_source_df
    .filter(F.col("_quality_status") == "VALID")
    .dropDuplicates([
        "order_id",
        "order_item_id"
    ])
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    order_items_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_items"
    )
)

(
    order_items_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.quarantine_order_items"
    )
)

print("Source rows:", order_items_source_df.count())
print("Silver rows:", order_items_silver_df.count())
print(
    "Quarantine rows:",
    order_items_quarantine_df.count()
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Deduplicate Order Payments and create quarantine table

payments_source_df = spark.table(
    f"{catalog}.{schema}.silver_order_payments_quality"
)

payments_quarantine_df = (
    payments_source_df
    .filter(F.col("_quality_status") == "INVALID")
)

payments_silver_df = (
    payments_source_df
    .filter(F.col("_quality_status") == "VALID")
    .dropDuplicates([
        "order_id",
        "payment_sequential"
    ])
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    payments_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_payments"
    )
)

(
    payments_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.quarantine_order_payments"
    )
)

print("Source rows:", payments_source_df.count())
print("Silver rows:", payments_silver_df.count())
print(
    "Quarantine rows:",
    payments_quarantine_df.count()
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Deduplicate Order Reviews and create quarantine table

reviews_source_df = spark.table(
    f"{catalog}.{schema}.silver_order_reviews_quality"
)

reviews_quarantine_df = (
    reviews_source_df
    .filter(F.col("_quality_status") == "INVALID")
)

reviews_silver_df = (
    reviews_source_df
    .filter(F.col("_quality_status") == "VALID")
    .dropDuplicates([
        "review_id",
        "order_id"
    ])
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    reviews_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_reviews"
    )
)

(
    reviews_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.quarantine_order_reviews"
    )
)

print("Source rows:", reviews_source_df.count())
print("Silver rows:", reviews_silver_df.count())
print(
    "Quarantine rows:",
    reviews_quarantine_df.count()
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 07 Silver Deduplication: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 07 Silver Deduplication workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
dedup_validation_df = spark.sql(f"""
WITH validation AS (

    SELECT
        'orders' AS table_name,
        99441 AS expected_rows,
        COUNT(*) AS silver_rows,
        (
            SELECT COUNT(*)
            FROM {catalog}.{schema}.quarantine_orders
        ) AS quarantine_rows
    FROM {catalog}.{schema}.silver_orders

    UNION ALL

    SELECT
        'order_items',
        112650,
        COUNT(*),
        (
            SELECT COUNT(*)
            FROM {catalog}.{schema}.quarantine_order_items
        )
    FROM {catalog}.{schema}.silver_order_items

    UNION ALL

    SELECT
        'order_payments',
        103886,
        COUNT(*),
        (
            SELECT COUNT(*)
            FROM {catalog}.{schema}.quarantine_order_payments
        )
    FROM {catalog}.{schema}.silver_order_payments

    UNION ALL

    SELECT
        'order_reviews',
        99224,
        COUNT(*),
        (
            SELECT COUNT(*)
            FROM {catalog}.{schema}.quarantine_order_reviews
        )
    FROM {catalog}.{schema}.silver_order_reviews
)

SELECT
    *,
    CASE
        WHEN silver_rows = expected_rows
             AND quarantine_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM validation
ORDER BY table_name
""")

display(dedup_validation_df)

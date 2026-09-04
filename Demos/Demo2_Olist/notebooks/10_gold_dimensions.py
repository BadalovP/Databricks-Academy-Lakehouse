# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 10 Gold Dimensions workflow.
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
# MAGIC ### 10 Gold Dimensions: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
from pyspark.sql import functions as F

customer_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.silver_customer_scd2"
    ).alias("c")

    .join(
        spark.table(
            f"{catalog}.{schema}.silver_geolocations"
        ).alias("g"),

        F.col("c.customer_zip_code_prefix")
        == F.col("g.geolocation_zip_code_prefix"),

        "left"
    )

    .select(
        F.col("c.customer_sk"),
        F.col("c.customer_id"),
        F.col("c.customer_unique_id"),
        F.col("c.customer_zip_code_prefix"),
        F.col("c.customer_city"),
        F.col("c.customer_state"),

        F.col("g.average_latitude")
        .alias("customer_latitude"),

        F.col("g.average_longitude")
        .alias("customer_longitude"),

        F.col("c.effective_from"),
        F.col("c.effective_to"),
        F.col("c.is_current"),
        F.col("c.record_hash")
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )
)

(
    customer_dimension_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_dim_customer"
    )
)

print(
    "Gold customer dimension:",
    customer_dimension_df.count()
)

print(
    "Customers without coordinates:",
    customer_dimension_df
    .filter(F.col("customer_latitude").isNull())
    .count()
)

display(customer_dimension_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Create Gold Product Dimension

product_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.silver_products"
    )

    .select(
        F.sha2(
            F.col("product_id"),
            256
        ).alias("product_sk"),

        F.col("product_id"),
        F.col("product_category_name"),
        F.col("product_category_name_english"),
        F.col("product_name_length"),
        F.col("product_description_length"),
        F.col("product_photos_quantity"),
        F.col("product_weight_g"),
        F.col("product_length_cm"),
        F.col("product_height_cm"),
        F.col("product_width_cm"),
        F.col("product_volume_cm3")
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )
)

(
    product_dimension_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_dim_product"
    )
)

print(
    "Gold product dimension:",
    product_dimension_df.count()
)

print(
    "Duplicate product surrogate keys:",
    product_dimension_df
    .groupBy("product_sk")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

display(product_dimension_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Create Gold Seller Dimension

seller_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.silver_sellers"
    ).alias("s")

    .join(
        spark.table(
            f"{catalog}.{schema}.silver_geolocations"
        ).alias("g"),

        F.col("s.seller_zip_code_prefix")
        == F.col("g.geolocation_zip_code_prefix"),

        "left"
    )

    .select(
        F.sha2(
            F.col("s.seller_id"),
            256
        ).alias("seller_sk"),

        F.col("s.seller_id"),
        F.col("s.seller_zip_code_prefix"),
        F.col("s.seller_city"),
        F.col("s.seller_state"),

        F.col("g.average_latitude")
        .alias("seller_latitude"),

        F.col("g.average_longitude")
        .alias("seller_longitude")
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )
)

(
    seller_dimension_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_dim_seller"
    )
)

print(
    "Gold seller dimension:",
    seller_dimension_df.count()
)

print(
    "Duplicate seller surrogate keys:",
    seller_dimension_df
    .groupBy("seller_sk")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Sellers without coordinates:",
    seller_dimension_df
    .filter(F.col("seller_latitude").isNull())
    .count()
)

display(seller_dimension_df.limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Create the complete date range from Orders

date_dimension_df = spark.sql(f"""
WITH date_boundaries AS (
    SELECT
        MIN(
            TO_DATE(order_purchase_timestamp)
        ) AS minimum_date,

        MAX(
            TO_DATE(order_purchase_timestamp)
        ) AS maximum_date

    FROM {catalog}.{schema}.silver_orders_enriched
)

SELECT
    EXPLODE(
        SEQUENCE(
            minimum_date,
            maximum_date,
            INTERVAL 1 DAY
        )
    ) AS calendar_date

FROM date_boundaries
""")
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Add Date dimension attributes

date_dimension_df = (
    date_dimension_df

    .withColumn(
        "date_sk",
        F.date_format(
            "calendar_date",
            "yyyyMMdd"
        ).cast("integer")
    )

    .withColumn(
        "calendar_year",
        F.year("calendar_date")
    )

    .withColumn(
        "calendar_quarter",
        F.quarter("calendar_date")
    )

    .withColumn(
        "calendar_month",
        F.month("calendar_date")
    )

    .withColumn(
        "month_name",
        F.date_format(
            "calendar_date",
            "MMMM"
        )
    )

    .withColumn(
        "year_month",
        F.date_format(
            "calendar_date",
            "yyyy-MM"
        )
    )

    .withColumn(
        "week_of_year",
        F.weekofyear("calendar_date")
    )

    .withColumn(
        "day_of_month",
        F.dayofmonth("calendar_date")
    )

    .withColumn(
        "day_name",
        F.date_format(
            "calendar_date",
            "EEEE"
        )
    )

    .withColumn(
        "is_weekend",
        F.dayofweek("calendar_date").isin(
            1,
            7
        )
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )

    .select(
        "date_sk",
        "calendar_date",
        "calendar_year",
        "calendar_quarter",
        "calendar_month",
        "month_name",
        "year_month",
        "week_of_year",
        "day_of_month",
        "day_name",
        "is_weekend",
        "_gold_processed_at"
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Write Gold Date dimension

(
    date_dimension_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_dim_date"
    )
)

print(
    "Gold date dimension:",
    date_dimension_df.count()
)

display(
    date_dimension_df.orderBy(
        "calendar_date"
    ).limit(10)
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 8
# MAGIC **Purpose:** Execute stage 8 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Validate all Gold dimensions

dimension_checks = [
    (
        "gold_dim_customer",
        "silver_customer_scd2",
        "customer_sk"
    ),
    (
        "gold_dim_product",
        "silver_products",
        "product_sk"
    ),
    (
        "gold_dim_seller",
        "silver_sellers",
        "seller_sk"
    )
]

validation_rows = []

for gold_table, source_table, surrogate_key in dimension_checks:

    expected_rows = spark.table(
        f"{catalog}.{schema}.{source_table}"
    ).count()

    gold_df = spark.table(
        f"{catalog}.{schema}.{gold_table}"
    )

    result = (
        gold_df.agg(
            F.count("*").alias("actual_rows"),

            F.countDistinct(
                surrogate_key
            ).alias("distinct_surrogate_keys"),

            F.sum(
                F.when(
                    F.col(surrogate_key).isNull(),
                    1
                ).otherwise(0)
            ).alias("null_surrogate_keys")
        )
        .first()
    )

    actual_rows = result["actual_rows"]

    duplicate_surrogate_keys = (
        actual_rows
        - result["distinct_surrogate_keys"]
        - result["null_surrogate_keys"]
    )

    status = (
        "PASS"
        if (
            actual_rows == expected_rows
            and duplicate_surrogate_keys == 0
            and result["null_surrogate_keys"] == 0
        )
        else "FAIL"
    )

    validation_rows.append(
        (
            gold_table,
            expected_rows,
            actual_rows,
            duplicate_surrogate_keys,
            result["null_surrogate_keys"],
            status
        )
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 9
# MAGIC **Purpose:** Execute stage 9 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
expected_date_rows = (
    spark.sql(f"""
        SELECT
            DATEDIFF(
                MAX(TO_DATE(order_purchase_timestamp)),
                MIN(TO_DATE(order_purchase_timestamp))
            ) + 1 AS expected_rows
        FROM {catalog}.{schema}.silver_orders_enriched
    """)
    .first()["expected_rows"]
)

gold_date_df = spark.table(
    f"{catalog}.{schema}.gold_dim_date"
)

date_result = (
    gold_date_df.agg(
        F.count("*").alias("actual_rows"),
        F.countDistinct("date_sk").alias(
            "distinct_surrogate_keys"
        ),
        F.sum(
            F.when(
                F.col("date_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("null_surrogate_keys")
    )
    .first()
)

date_actual_rows = date_result["actual_rows"]

date_duplicate_keys = (
    date_actual_rows
    - date_result["distinct_surrogate_keys"]
    - date_result["null_surrogate_keys"]
)

date_status = (
    "PASS"
    if (
        date_actual_rows == expected_date_rows
        and date_duplicate_keys == 0
        and date_result["null_surrogate_keys"] == 0
    )
    else "FAIL"
)

validation_rows.append(
    (
        "gold_dim_date",
        expected_date_rows,
        date_actual_rows,
        date_duplicate_keys,
        date_result["null_surrogate_keys"],
        date_status
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10 Gold Dimensions: stage 10
# MAGIC **Purpose:** Execute stage 10 of the 10 Gold Dimensions workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
gold_dimension_validation_df = spark.createDataFrame(
    validation_rows,
    [
        "table_name",
        "expected_rows",
        "actual_rows",
        "duplicate_surrogate_keys",
        "null_surrogate_keys",
        "status"
    ]
)

display(
    gold_dimension_validation_df.orderBy("table_name")
)

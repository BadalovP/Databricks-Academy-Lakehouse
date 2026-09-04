# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

run_timestamp = (
    spark.sql(
        "SELECT current_timestamp() AS run_timestamp"
    )
    .first()["run_timestamp"]
)

customer_scd_source_df = (
    spark.table(
        f"{catalog}.{schema}.silver_customers"
    )

    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )

    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",

                F.coalesce(
                    F.col("customer_unique_id"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("customer_zip_code_prefix"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("customer_city"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("customer_state"),
                    F.lit("")
                )
            ),
            256
        )
    )

    .withColumn(
        "_run_timestamp",
        F.lit(run_timestamp).cast("timestamp")
    )
)

print(
    "Customer source rows:",
    customer_scd_source_df.count()
)

display(customer_scd_source_df.limit(10))

# COMMAND ----------

scd_table_name = (
    f"{catalog}.{schema}.silver_customer_scd2"
)

scd_table_exists = spark.catalog.tableExists(
    scd_table_name
)

if not scd_table_exists:

    initial_customer_scd_df = (
        customer_scd_source_df

        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("customer_id"),
                    F.col("_run_timestamp").cast("string")
                ),
                256
            ).alias("customer_sk"),

            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("record_hash"),

            F.col("_run_timestamp")
            .alias("effective_from"),

            F.to_timestamp(
                F.lit("9999-12-31 23:59:59")
            ).alias("effective_to"),

            F.lit(True).alias("is_current"),

            F.col("_run_timestamp")
            .alias("_scd_processed_at")
        )
    )

    (
        initial_customer_scd_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(scd_table_name)
    )

    print("Initial SCD2 table created")

else:
    print(
        "SCD2 table already exists; "
        "initial load was skipped"
    )

# COMMAND ----------

customer_scd_df = spark.table(
    scd_table_name
)

total_scd_rows = customer_scd_df.count()

current_scd_rows = (
    customer_scd_df
    .filter(F.col("is_current") == True)
    .count()
)

historical_scd_rows = (
    customer_scd_df
    .filter(F.col("is_current") == False)
    .count()
)

duplicate_current_customers = (
    customer_scd_df
    .filter(F.col("is_current") == True)
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Total SCD2 rows:", total_scd_rows)
print("Current rows:", current_scd_rows)
print("Historical rows:", historical_scd_rows)
print(
    "Customers with multiple current rows:",
    duplicate_current_customers
)

display(customer_scd_df.limit(10))

# COMMAND ----------

# Read only the current SCD2 versions

current_customer_scd_df = (
    spark.table(scd_table_name)

    .filter(
        F.col("is_current") == True
    )

    .select(
        "customer_id",
        "record_hash"
    )
)

# COMMAND ----------

# Compare the latest source against current SCD2 records

customer_comparison_df = (
    customer_scd_source_df.alias("source")

    .join(
        current_customer_scd_df.alias("target"),

        F.col("source.customer_id")
        == F.col("target.customer_id"),

        "left"
    )

    .select(
        "source.*",

        F.col("target.customer_id")
        .alias("_target_customer_id"),

        F.col("target.record_hash")
        .alias("_target_record_hash")
    )
)

# COMMAND ----------

# Find new or changed customers

customer_changes_df = (
    customer_comparison_df

    .filter(
        F.col("_target_customer_id").isNull()
        |
        (
            F.col("record_hash")
            != F.col("_target_record_hash")
        )
    )

    .select(
        *customer_scd_source_df.columns
    )
)

# COMMAND ----------

new_customer_count = (
    customer_comparison_df

    .filter(
        F.col("_target_customer_id").isNull()
    )

    .count()
)

changed_customer_count = (
    customer_comparison_df

    .filter(
        F.col("_target_customer_id").isNotNull()
        &
        (
            F.col("record_hash")
            != F.col("_target_record_hash")
        )
    )

    .count()
)

print("New customers:", new_customer_count)
print("Changed customers:", changed_customer_count)
print(
    "Versions to insert:",
    customer_changes_df.count()
)

# COMMAND ----------

# Materialize the detected changes before modifying the target table

customer_changes_df = customer_changes_df.cache()

versions_to_process = customer_changes_df.count()

if versions_to_process > 0:

    customer_changes_df.createOrReplaceTempView(
        "customer_scd2_changes"
    )

    # Close existing versions of changed customers
    spark.sql(f"""
        MERGE INTO {scd_table_name} AS target

        USING customer_scd2_changes AS source

        ON target.customer_id = source.customer_id
           AND target.is_current = true

        WHEN MATCHED
             AND target.record_hash <> source.record_hash

        THEN UPDATE SET
            target.effective_to =
                source._run_timestamp,

            target.is_current = false,

            target._scd_processed_at =
                source._run_timestamp
    """)

    # Build new versions for new and changed customers
    new_customer_versions_df = (
        customer_changes_df

        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("customer_id"),
                    F.col("_run_timestamp").cast("string")
                ),
                256
            ).alias("customer_sk"),

            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("record_hash"),

            F.col("_run_timestamp")
            .alias("effective_from"),

            F.to_timestamp(
                F.lit("9999-12-31 23:59:59")
            ).alias("effective_to"),

            F.lit(True).alias("is_current"),

            F.col("_run_timestamp")
            .alias("_scd_processed_at")
        )
    )

    (
        new_customer_versions_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(scd_table_name)
    )

    print(
        "SCD2 versions processed:",
        versions_to_process
    )

else:
    print(
        "No new or changed customers. "
        "SCD2 table was not modified."
    )

customer_changes_df.unpersist()

# COMMAND ----------

scd_validation_df = spark.sql(f"""
WITH scd_metrics AS (
    SELECT
        COUNT(*) AS total_versions,

        SUM(
            CASE WHEN is_current = true
            THEN 1 ELSE 0 END
        ) AS current_versions,

        SUM(
            CASE WHEN is_current = false
            THEN 1 ELSE 0 END
        ) AS historical_versions,

        COUNT(DISTINCT customer_sk)
            AS distinct_surrogate_keys,

        SUM(
            CASE
                WHEN effective_from >= effective_to
                THEN 1
                ELSE 0
            END
        ) AS invalid_date_ranges

    FROM {scd_table_name}
),

duplicate_metrics AS (
    SELECT
        COUNT(*) AS duplicate_current_customers
    FROM (
        SELECT customer_id
        FROM {scd_table_name}
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
),

source_metrics AS (
    SELECT
        COUNT(*) AS expected_current_versions
    FROM {catalog}.{schema}.silver_customers
)

SELECT
    scd_metrics.*,
    duplicate_metrics.duplicate_current_customers,
    source_metrics.expected_current_versions,

    CASE
        WHEN current_versions =
                 expected_current_versions
         AND total_versions =
                 distinct_surrogate_keys
         AND invalid_date_ranges = 0
         AND duplicate_current_customers = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status

FROM scd_metrics
CROSS JOIN duplicate_metrics
CROSS JOIN source_metrics
""")

display(scd_validation_df)

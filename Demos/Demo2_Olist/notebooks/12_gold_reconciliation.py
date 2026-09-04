# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 12 Gold Reconciliation workflow.
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
# MAGIC ### 12 Gold Reconciliation: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
from pyspark.sql import functions as F

silver_fact_source_df = spark.table(
    f"{catalog}.{schema}.silver_order_items_enriched"
)

gold_fact_df = spark.table(
    f"{catalog}.{schema}.gold_fact_order_items"
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
silver_summary_df = (
    silver_fact_source_df
    .agg(
        F.count("*").alias("silver_rows"),

        F.countDistinct(
            "order_id"
        ).alias("silver_distinct_orders"),

        F.round(
            F.sum("price"),
            2
        ).alias("silver_total_price"),

        F.round(
            F.sum("freight_value"),
            2
        ).alias("silver_total_freight"),

        F.round(
            F.sum("item_total_value"),
            2
        ).alias("silver_total_item_value")
    )
)

gold_summary_df = (
    gold_fact_df
    .agg(
        F.count("*").alias("gold_rows"),

        F.countDistinct(
            "order_id"
        ).alias("gold_distinct_orders"),

        F.round(
            F.sum("price"),
            2
        ).alias("gold_total_price"),

        F.round(
            F.sum("freight_value"),
            2
        ).alias("gold_total_freight"),

        F.round(
            F.sum("item_total_value"),
            2
        ).alias("gold_total_item_value")
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
gold_reconciliation_df = (
    silver_summary_df
    .crossJoin(gold_summary_df)

    .withColumn(
        "row_difference",
        F.col("gold_rows")
        - F.col("silver_rows")
    )

    .withColumn(
        "order_difference",
        F.col("gold_distinct_orders")
        - F.col("silver_distinct_orders")
    )

    .withColumn(
        "price_difference",
        F.round(
            F.col("gold_total_price")
            - F.col("silver_total_price"),
            2
        )
    )

    .withColumn(
        "freight_difference",
        F.round(
            F.col("gold_total_freight")
            - F.col("silver_total_freight"),
            2
        )
    )

    .withColumn(
        "item_value_difference",
        F.round(
            F.col("gold_total_item_value")
            - F.col("silver_total_item_value"),
            2
        )
    )

    .withColumn(
        "status",
        F.when(
            (F.col("row_difference") == 0)
            & (F.col("order_difference") == 0)
            & (F.abs(F.col("price_difference")) <= 0.01)
            & (F.abs(F.col("freight_difference")) <= 0.01)
            & (
                F.abs(
                    F.col("item_value_difference")
                ) <= 0.01
            ),
            "PASS"
        ).otherwise("FAIL")
    )
)

display(gold_reconciliation_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
customer_key_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_customer"
    )
    .select("customer_sk")
)

product_key_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_product"
    )
    .select("product_sk")
)

seller_key_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_seller"
    )
    .select("seller_sk")
)

date_key_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_date"
    )
    .select("date_sk")
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
invalid_item_value_rows = (
    gold_fact_df

    .filter(
        F.col("price").isNull()
        | F.col("freight_value").isNull()
        | F.col("item_total_value").isNull()
        | (
            F.abs(
                F.col("item_total_value")
                - (
                    F.col("price")
                    + F.col("freight_value")
                )
            ) > 0.01
        )
    )

    .count()
)

missing_customer_rows = (
    gold_fact_df
    .join(
        customer_key_df,
        "customer_sk",
        "left_anti"
    )
    .count()
)

missing_product_rows = (
    gold_fact_df
    .join(
        product_key_df,
        "product_sk",
        "left_anti"
    )
    .count()
)

missing_seller_rows = (
    gold_fact_df
    .join(
        seller_key_df,
        "seller_sk",
        "left_anti"
    )
    .count()
)

missing_date_rows = (
    gold_fact_df
    .join(
        date_key_df,
        "date_sk",
        "left_anti"
    )
    .count()
)


invalid_date_rows = (
    gold_fact_df

    .filter(
        F.col("date_sk").isNull()
        | (
            F.col("date_sk")
            != F.date_format(
                F.col("order_purchase_timestamp"),
                "yyyyMMdd"
            ).cast("integer")
        )
    )

    .count()
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
gold_integrity_validation_df = (
    spark.createDataFrame(
        [
            (
                missing_customer_rows,
                missing_product_rows,
                missing_seller_rows,
                missing_date_rows,
                invalid_item_value_rows,
                invalid_date_rows
            )
        ],
        [
            "missing_customer_rows",
            "missing_product_rows",
            "missing_seller_rows",
            "missing_date_rows",
            "invalid_item_value_rows",
            "invalid_date_rows"
        ]
    )

    .withColumn(
        "status",
        F.when(
            (F.col("missing_customer_rows") == 0)
            & (F.col("missing_product_rows") == 0)
            & (F.col("missing_seller_rows") == 0)
            & (F.col("missing_date_rows") == 0)
            & (F.col("invalid_item_value_rows") == 0)
            & (F.col("invalid_date_rows") == 0),
            "PASS"
        ).otherwise("FAIL")
    )
)

display(gold_integrity_validation_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 8
# MAGIC **Purpose:** Execute stage 8 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
gold_reconciliation_audit_df = (
    gold_reconciliation_df

    .withColumnRenamed(
        "status",
        "reconciliation_status"
    )

    .crossJoin(
        gold_integrity_validation_df
        .withColumnRenamed(
            "status",
            "integrity_status"
        )
    )

    .withColumn(
        "overall_status",
        F.when(
            (
                F.col("reconciliation_status")
                == "PASS"
            )
            & (
                F.col("integrity_status")
                == "PASS"
            ),
            "PASS"
        ).otherwise("FAIL")
    )

    .withColumn(
        "_validated_at",
        F.current_timestamp()
    )
)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 9
# MAGIC **Purpose:** Execute stage 9 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
display(gold_reconciliation_audit_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 12 Gold Reconciliation: stage 10
# MAGIC **Purpose:** Execute stage 10 of the 12 Gold Reconciliation workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
(
    gold_reconciliation_audit_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_reconciliation_audit"
    )
)


gold_reconciliation_audit_table_df = (
    spark.table(
        f"{catalog}.{schema}.gold_reconciliation_audit"
    )
    .orderBy(
        F.col("_validated_at").desc()
    )
)

display(
    gold_reconciliation_audit_table_df.limit(10)
)

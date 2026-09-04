# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Explore Learning Outputs: stage 1
# MAGIC **Purpose:** Execute stage 1 of the Explore Learning Outputs workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Purpose: explore the outputs created by the learning ETL pipeline.

summary_df = spark.table(
    "dbr_dev.parvinbadalov.learning_pipeline_summary"
)

display(summary_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### Explore Learning Outputs: stage 2
# MAGIC **Purpose:** Execute stage 2 of the Explore Learning Outputs workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# MAGIC %sql
# MAGIC -- Purpose: inspect order metrics by status.
# MAGIC
# MAGIC SELECT *
# MAGIC FROM dbr_dev.parvinbadalov.learning_orders_by_status
# MAGIC ORDER BY order_item_rows DESC;
# COMMAND ----------
# MAGIC %md
# MAGIC ### Explore Learning Outputs: stage 3
# MAGIC **Purpose:** Execute stage 3 of the Explore Learning Outputs workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dbr_dev.parvinbadalov.learning_quality_status;

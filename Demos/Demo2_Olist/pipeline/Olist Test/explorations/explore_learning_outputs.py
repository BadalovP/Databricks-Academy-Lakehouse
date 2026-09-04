# Databricks notebook source
# Purpose: explore the outputs created by the learning ETL pipeline.

summary_df = spark.table(
    "dbr_dev.parvinbadalov.learning_pipeline_summary"
)

display(summary_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Purpose: inspect order metrics by status.
# MAGIC
# MAGIC SELECT *
# MAGIC FROM dbr_dev.parvinbadalov.learning_orders_by_status
# MAGIC ORDER BY order_item_rows DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dbr_dev.parvinbadalov.learning_quality_status;

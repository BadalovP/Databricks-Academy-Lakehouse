# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# DBTITLE 1,Cell 1
dbutils.widgets.text("catalog","dbr_dev", "01_Catalog")
dbutils.widgets.text("schema","parvinbadalov", "02_Schema")
dbutils.widgets.text("volume_name","demo2_olist", "03_External_volume")
dbutils.widgets.text("landing_dir", "landing", "04_Landing_Dir")
dbutils.widgets.text("schema_dir", "schema", "05_Schema_Dir")
dbutils.widgets.text("checkpoint_dir", "checkpoints", "06_Checkpoint_Dir")
dbutils.widgets.text("quarantine_dir", "quarantine", "07_Quarantine_Dir")
dbutils.widgets.text("archive_dir", "archive", "08_Archive_Dir")
dbutils.widgets.dropdown("trigger_type", "availableNow", ["availableNow", "once", "continuous"], "09_Trigger_Type")
dbutils.widgets.text("environment", "dev", "10_Environment")
dbutils.widgets.text("storage_location","abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net","11_storage_location")
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
volume_name = dbutils.widgets.get("volume_name")
landing_dir = dbutils.widgets.get("landing_dir")
schema_dir = dbutils.widgets.get("schema_dir")
volume_path = f"{catalog}.{schema}.{volume_name}"
checkpoint_dir = dbutils.widgets.get("checkpoint_dir")
quarantine_dir = dbutils.widgets.get("quarantine_dir")
archive_dir = dbutils.widgets.get("archive_dir")
trigger_type = dbutils.widgets.get("trigger_type")
environment = dbutils.widgets.get("environment")
storage_location = dbutils.widgets.get("storage_location")
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name} LOCATION '{storage_location}/{volume_name}'")
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

landing_path = f"{volume_path}/{landing_dir}"
schema_path = f"{volume_path}/{schema_dir}"
checkpoint_path = f"{volume_path}/{checkpoint_dir}"
quarantine_path = f"{volume_path}/{quarantine_dir}"
archive_path = f"{volume_path}/{archive_dir}"
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
dbutils.fs.mkdirs(landing_path)
dbutils.fs.mkdirs(schema_path)
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.mkdirs(quarantine_path)
dbutils.fs.mkdirs(archive_path)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# DBTITLE 1,Cell 8
dbutils.fs.mkdirs(f"{landing_path}/customers")
dbutils.fs.mkdirs(f"{landing_path}/orders")
dbutils.fs.mkdirs(f"{landing_path}/order_items")
dbutils.fs.mkdirs(f"{landing_path}/payments")
dbutils.fs.mkdirs(f"{landing_path}/reviews")
dbutils.fs.mkdirs(f"{landing_path}/products")
dbutils.fs.mkdirs(f"{landing_path}/sellers")
dbutils.fs.mkdirs(f"{landing_path}/category_translation")
dbutils.fs.mkdirs(f"{landing_path}/geolocations")
# COMMAND ----------
# MAGIC %md
# MAGIC ### 00 Setup: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 00 Setup workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
dbutils.fs.mkdirs(f"{schema_path}/orders")
dbutils.fs.mkdirs(f"{schema_path}/order_items")
dbutils.fs.mkdirs(f"{schema_path}/payments")
dbutils.fs.mkdirs(f"{schema_path}/reviews")

dbutils.fs.mkdirs(f"{checkpoint_path}/orders")
dbutils.fs.mkdirs(f"{checkpoint_path}/order_items")
dbutils.fs.mkdirs(f"{checkpoint_path}/payments")
dbutils.fs.mkdirs(f"{checkpoint_path}/reviews")

# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 02 Validate Landing Files: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 02 Validate Landing Files workflow.
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
# MAGIC ### 02 Validate Landing Files: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 02 Validate Landing Files workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
sources = [
    "customers",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "products",
    "sellers",
    "category_translation",
    "geolocations"
]

for source in sources:
    print(f"\n=== {source} ===")
    for f in dbutils.fs.ls(f"{landing_path}/{source}"):
        print(f.name, f.size)

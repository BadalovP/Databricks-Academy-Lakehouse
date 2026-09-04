# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

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

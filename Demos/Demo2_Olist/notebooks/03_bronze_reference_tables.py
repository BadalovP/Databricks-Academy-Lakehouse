# Databricks notebook source

# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 1
# MAGIC **Purpose:** Execute stage 1 of the 03 Bronze Reference Tables workflow.
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
# MAGIC ### 03 Bronze Reference Tables: stage 2
# MAGIC **Purpose:** Execute stage 2 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
customers_schema = """
customer_id STRING,
customer_unique_id STRING,
customer_zip_code_prefix STRING,
customer_city STRING,
customer_state STRING
"""

product_category_name_translation_schema = """
product_category_name STRING,
product_category_name_english STRING
"""

products_schema = """
product_id STRING,
product_category_name STRING,
product_name_lenght STRING,
product_description_lenght STRING,
product_photos_qty STRING,
product_weight_g STRING,
product_length_cm STRING,
product_height_cm STRING,
product_width_cm STRING
"""



sellers_schema = """
seller_id STRING,
seller_zip_code_prefix STRING,
seller_city STRING,
seller_state STRING
"""

geolocations_schema = """
geolocation_zip_code_prefix STRING,
geolocation_lat STRING,
geolocation_lng STRING,
geolocation_city STRING,
geolocation_state STRING
"""
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 3
# MAGIC **Purpose:** Execute stage 3 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# DBTITLE 1,Cell 3
print("=== CUSTOMERS ===")
print(customers_schema)

print("=== CATEGORY TRANSLATION ===")
print(product_category_name_translation_schema)

print("=== PRODUCTS ===")
print(products_schema)



print("=== SELLERS ===")
print(sellers_schema)

print("=== GEOLOCATIONS ===")
print(geolocations_schema)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 4
# MAGIC **Purpose:** Execute stage 4 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Customers
from pyspark.sql import functions as F

customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv(f"{landing_path}/customers")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_customers")
)

print("bronze_customers:", customers_df.count())
display(customers_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 5
# MAGIC **Purpose:** Execute stage 5 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
from pyspark.sql import functions as F

# Products
products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv(f"{landing_path}/products")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    products_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_products")
)

print("bronze_products:", products_df.count())
display(products_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 6
# MAGIC **Purpose:** Execute stage 6 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Sellers
sellers_df = (
    spark.read
    .option("header", True)
    .schema(sellers_schema)
    .csv(f"{landing_path}/sellers")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    sellers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_sellers")
)

print("bronze_sellers:", sellers_df.count())
display(sellers_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 7
# MAGIC **Purpose:** Execute stage 7 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Category Translation
category_translation_df = (
    spark.read
    .option("header", True)
    .schema(product_category_name_translation_schema)
    .csv(f"{landing_path}/category_translation")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    category_translation_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_category_translation")
)

print("bronze_category_translation:", category_translation_df.count())
display(category_translation_df)
# COMMAND ----------
# MAGIC %md
# MAGIC ### 03 Bronze Reference Tables: stage 8
# MAGIC **Purpose:** Execute stage 8 of the 03 Bronze Reference Tables workflow.
# MAGIC
# MAGIC **Inputs:** Upstream tables, DataFrames, files, parameters, or the configured runtime context.
# MAGIC
# MAGIC **Outputs:** Stage-specific tables, views, metrics, or validation state used by downstream cells.
# MAGIC
# MAGIC **Why it matters:** This explanation makes the stage side effects and dependency contract reviewable.
# COMMAND ----------
# Geolocations
geolocations_df = (
    spark.read
    .option("header", True)
    .schema(geolocations_schema)
    .csv(f"{landing_path}/geolocations")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    geolocations_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_geolocations")
)

print("bronze_geolocations:", geolocations_df.count())
display(geolocations_df)

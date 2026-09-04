# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

orders_schema = """
order_id STRING,
customer_id STRING,
order_status STRING,
order_purchase_timestamp STRING,
order_approved_at STRING,
order_delivered_carrier_date STRING,
order_delivered_customer_date STRING,
order_estimated_delivery_date STRING
"""

order_items_schema = """
order_id STRING,
order_item_id STRING,
product_id STRING,
seller_id STRING,
shipping_limit_date STRING,
price STRING,
freight_value STRING
"""

order_payments_schema = """
order_id STRING,
payment_sequential STRING,
payment_type STRING,
payment_installments STRING,
payment_value STRING
"""

order_reviews_schema = """
review_id STRING,
order_id STRING,
review_score STRING,
review_comment_title STRING,
review_comment_message STRING,
review_creation_date STRING,
review_answer_timestamp STRING
"""

# COMMAND ----------

print("=== ORDERS ===")
print(orders_schema)

print("=== ORDER ITEMS ===")
print(order_items_schema)

print("=== PAYMENTS ===")
print(order_payments_schema)

print("=== REVIEWS ===")
print(order_reviews_schema)

# COMMAND ----------

# Orders Auto Loader
from pyspark.sql import functions as F

orders_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{schema_path}/orders")
    .option("header", True)
    .schema(orders_schema)
    .load(f"{landing_path}/orders")
    .withColumn("_ingested_at", F.current_timestamp())
)

orders_query = (
    orders_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{checkpoint_path}/orders"
    )
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_orders")
)

orders_query.awaitTermination()

print(
    "bronze_orders:",
    spark.table(
        f"{catalog}.{schema}.bronze_orders"
    ).count()
)

# COMMAND ----------

order_items_df=(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{schema_path}/order_items")
    .option("header", True)
    .schema(order_items_schema)
    .load(f"{landing_path}/order_items")
    .withColumn("_ingested_at", F.current_timestamp())
)

order_items_query = (
    order_items_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{checkpoint_path}/order_items"
    )
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_order_items")
)

order_items_query.awaitTermination()

print(
    "bronze_order_items:",
    spark.table(
        f"{catalog}.{schema}.bronze_order_items"
    ).count()
)




# COMMAND ----------

payments_df=(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{schema_path}/payments")
    .option("header", True)
    .schema(order_payments_schema)
    .load(f"{landing_path}/payments")
    .withColumn("_ingested_at", F.current_timestamp())
)

payments_query = (
    payments_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{checkpoint_path}/payments"
    )
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_order_payments")
)

payments_query.awaitTermination()

print(
    "bronze_order_payments:",
    spark.table(
        f"{catalog}.{schema}.bronze_order_payments"
    ).count()
)

# COMMAND ----------

from pyspark.sql import functions as F

order_reviews_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        f"{schema_path}/reviews"
    )
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .schema(order_reviews_schema)
    .load(f"{landing_path}/reviews")
    .withColumn("_ingested_at", F.current_timestamp())
)

order_reviews_query = (
    order_reviews_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{checkpoint_path}/reviews"
    )
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_order_reviews")
)

order_reviews_query.awaitTermination()

print(
    "bronze_order_reviews:",
    spark.table(
        f"{catalog}.{schema}.bronze_order_reviews"
    ).count()
)

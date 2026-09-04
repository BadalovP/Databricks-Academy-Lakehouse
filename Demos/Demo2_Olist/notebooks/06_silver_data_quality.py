# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

orders_quality_df = (
    spark.table(f"{catalog}.{schema}.bronze_orders")

    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_carrier_date",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )

    .withColumn(
        "_quality_status",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "") |
            F.col("customer_id").isNull() |
            (F.trim(F.col("customer_id")) == "") |
            F.col("order_status").isNull() |
            F.col("order_purchase_timestamp").isNull(),
            "INVALID"
        ).otherwise("VALID")
    )

    .withColumn(
        "_quality_reason",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == ""),
            "Missing order_id"
        )
        .when(
            F.col("customer_id").isNull() |
            (F.trim(F.col("customer_id")) == ""),
            "Missing customer_id"
        )
        .when(
            F.col("order_status").isNull(),
            "Missing order_status"
        )
        .when(
            F.col("order_purchase_timestamp").isNull(),
            "Invalid purchase timestamp"
        )
    )
)

(
    orders_quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_orders_quality"
    )
)

display(
    orders_quality_df
    .groupBy("_quality_status")
    .count()
)

# COMMAND ----------

# Order Items quality

order_items_quality_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_order_items"
    )

    .withColumn(
        "order_item_id",
        F.col("order_item_id").cast("integer")
    )
    .withColumn(
        "shipping_limit_date",
        F.to_timestamp("shipping_limit_date")
    )
    .withColumn(
        "price",
        F.col("price").cast("decimal(18,2)")
    )
    .withColumn(
        "freight_value",
        F.col("freight_value").cast("decimal(18,2)")
    )

    .withColumn(
        "_quality_status",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "") |
            F.col("product_id").isNull() |
            (F.trim(F.col("product_id")) == "") |
            F.col("seller_id").isNull() |
            (F.trim(F.col("seller_id")) == "") |
            F.col("order_item_id").isNull() |
            (F.col("order_item_id") <= 0) |
            F.col("price").isNull() |
            (F.col("price") < 0) |
            F.col("freight_value").isNull() |
            (F.col("freight_value") < 0),
            "INVALID"
        ).otherwise("VALID")
    )

    .withColumn(
        "_quality_reason",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == ""),
            "Missing order_id"
        )
        .when(
            F.col("product_id").isNull() |
            (F.trim(F.col("product_id")) == ""),
            "Missing product_id"
        )
        .when(
            F.col("seller_id").isNull() |
            (F.trim(F.col("seller_id")) == ""),
            "Missing seller_id"
        )
        .when(
            F.col("order_item_id").isNull() |
            (F.col("order_item_id") <= 0),
            "Invalid order_item_id"
        )
        .when(
            F.col("price").isNull() |
            (F.col("price") < 0),
            "Invalid price"
        )
        .when(
            F.col("freight_value").isNull() |
            (F.col("freight_value") < 0),
            "Invalid freight value"
        )
    )
)

(
    order_items_quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_items_quality"
    )
)

display(
    order_items_quality_df
    .groupBy("_quality_status")
    .count()
)

# COMMAND ----------

# Order Payments quality

payments_quality_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_order_payments"
    )

    .withColumn(
        "payment_sequential",
        F.col("payment_sequential").cast("integer")
    )
    .withColumn(
        "payment_installments",
        F.col("payment_installments").cast("integer")
    )
    .withColumn(
        "payment_value",
        F.col("payment_value").cast("decimal(18,2)")
    )

    .withColumn(
        "_quality_status",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "") |
            F.col("payment_sequential").isNull() |
            (F.col("payment_sequential") <= 0) |
            F.col("payment_type").isNull() |
            (F.trim(F.col("payment_type")) == "") |
            F.col("payment_installments").isNull() |
            (F.col("payment_installments") < 0) |
            F.col("payment_value").isNull() |
            (F.col("payment_value") < 0),
            "INVALID"
        ).otherwise("VALID")
    )

    .withColumn(
        "_quality_reason",
        F.when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == ""),
            "Missing order_id"
        )
        .when(
            F.col("payment_sequential").isNull() |
            (F.col("payment_sequential") <= 0),
            "Invalid payment sequence"
        )
        .when(
            F.col("payment_type").isNull() |
            (F.trim(F.col("payment_type")) == ""),
            "Missing payment type"
        )
        .when(
            F.col("payment_installments").isNull() |
            (F.col("payment_installments") < 0),
            "Invalid payment installments"
        )
        .when(
            F.col("payment_value").isNull() |
            (F.col("payment_value") < 0),
            "Invalid payment value"
        )
    )
)

(
    payments_quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_payments_quality"
    )
)

display(
    payments_quality_df
    .groupBy("_quality_status")
    .count()
)

# COMMAND ----------

# Order Reviews quality

reviews_quality_df = (
    spark.table(
        f"{catalog}.{schema}.bronze_order_reviews"
    )

    .withColumn(
        "review_score",
        F.col("review_score").cast("integer")
    )
    .withColumn(
        "review_creation_date",
        F.to_timestamp("review_creation_date")
    )
    .withColumn(
        "review_answer_timestamp",
        F.to_timestamp("review_answer_timestamp")
    )

    .withColumn(
        "_quality_status",
        F.when(
            F.col("review_id").isNull() |
            (F.trim(F.col("review_id")) == "") |
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "") |
            F.col("review_score").isNull() |
            (F.col("review_score") < 1) |
            (F.col("review_score") > 5) |
            F.col("review_creation_date").isNull() |
            F.col("review_answer_timestamp").isNull(),
            "INVALID"
        ).otherwise("VALID")
    )

    .withColumn(
        "_quality_reason",
        F.when(
            F.col("review_id").isNull() |
            (F.trim(F.col("review_id")) == ""),
            "Missing review_id"
        )
        .when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == ""),
            "Missing order_id"
        )
        .when(
            F.col("review_score").isNull() |
            (F.col("review_score") < 1) |
            (F.col("review_score") > 5),
            "Invalid review score"
        )
        .when(
            F.col("review_creation_date").isNull(),
            "Invalid review creation date"
        )
        .when(
            F.col("review_answer_timestamp").isNull(),
            "Invalid review answer timestamp"
        )
    )
)

(
    reviews_quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.silver_order_reviews_quality"
    )
)

display(
    reviews_quality_df
    .groupBy("_quality_status")
    .count()
)

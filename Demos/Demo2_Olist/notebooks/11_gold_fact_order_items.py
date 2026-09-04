# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

# One source row per order item
fact_source_df = (
    spark.table(
        f"{catalog}.{schema}.silver_order_items_enriched"
    )
    .alias("f")
)

# Use only the current SCD2 customer version
customer_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_customer"
    )
    .filter(
        F.col("is_current") == True
    )
    .select(
        "customer_id",
        "customer_sk"
    )
    .alias("c")
)

product_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_product"
    )
    .select(
        "product_id",
        "product_sk"
    )
    .alias("p")
)

seller_dimension_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_seller"
    )
    .select(
        "seller_id",
        "seller_sk"
    )
    .alias("s")
)

date_dimension_lookup_df = (
    spark.table(
        f"{catalog}.{schema}.gold_dim_date"
    )
    .select(
        "calendar_date",
        "date_sk"
    )
    .alias("d")
)

# COMMAND ----------

# MAGIC %md
# MAGIC display(customer_dimension_df)
# MAGIC display(product_dimension_df)
# MAGIC display(seller_dimension_df)
# MAGIC display(date_dimension_lookup_df)
# MAGIC display(fact_source_df)

# COMMAND ----------

source_rows = fact_source_df.count()

unique_fact_keys = (
    fact_source_df
    .select(
        "order_id",
        "order_item_id"
    )
    .dropDuplicates()
    .count()
)

print("Fact source rows:", source_rows)
print("Unique order-item keys:", unique_fact_keys)

print(
    "Current customers:",
    customer_dimension_df.count()
)

print(
    "Products:",
    product_dimension_df.count()
)

print(
    "Sellers:",
    seller_dimension_df.count()
)

print(
    "Dates:",
    date_dimension_lookup_df.count()
)

# COMMAND ----------

fact_with_dimension_keys_df = (
    fact_source_df

    # Customer dimension
    .join(
        customer_dimension_df,
        F.col("f.customer_id")
        == F.col("c.customer_id"),
        "left"
    )

    # Product dimension
    .join(
        product_dimension_df,
        F.col("f.product_id")
        == F.col("p.product_id"),
        "left"
    )

    # Seller dimension
    .join(
        seller_dimension_df,
        F.col("f.seller_id")
        == F.col("s.seller_id"),
        "left"
    )

    # Date dimension
    .join(
        date_dimension_lookup_df,
        F.to_date(
            F.col("f.order_purchase_timestamp")
        )
        == F.col("d.calendar_date"),
        "left"
    )
)

# COMMAND ----------

fact_join_validation_df = (
    fact_with_dimension_keys_df
    .agg(
        F.count("*").alias("joined_rows"),

        F.sum(
            F.when(
                F.col("c.customer_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_customer_keys"),

        F.sum(
            F.when(
                F.col("p.product_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_product_keys"),

        F.sum(
            F.when(
                F.col("s.seller_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_seller_keys"),

        F.sum(
            F.when(
                F.col("d.date_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_date_keys")
    )
)

display(fact_join_validation_df)

# COMMAND ----------

gold_fact_order_items_df = (
    fact_with_dimension_keys_df

    .select(
        # Unique fact surrogate key
        F.sha2(
            F.concat_ws(
                "||",
                F.col("f.order_id"),
                F.col("f.order_item_id").cast("string")
            ),
            256
        ).alias("order_item_sk"),

        # Dimension foreign keys
        F.col("c.customer_sk"),
        F.col("p.product_sk"),
        F.col("s.seller_sk"),
        F.col("d.date_sk"),

        # Business keys for traceability
        F.col("f.order_id"),
        F.col("f.order_item_id"),
        F.col("f.customer_id"),
        F.col("f.product_id"),
        F.col("f.seller_id"),

        # Order information
        F.col("f.order_status"),
        F.col("f.order_purchase_timestamp"),

        # Additive item-level measures
        F.col("f.price"),
        F.col("f.freight_value"),
        F.col("f.item_total_value"),

        # Each fact row represents one item
        F.lit(1)
        .cast("integer")
        .alias("item_quantity"),

        # Delivery indicator
        F.col("f.is_late_delivery"),

        # Processing metadata
        F.current_timestamp()
        .alias("_gold_processed_at")
    )
)

# COMMAND ----------

print(
    "Gold fact rows:",
    gold_fact_order_items_df.count()
)

display(
    gold_fact_order_items_df.limit(10)
)

# COMMAND ----------

expected_fact_rows = fact_source_df.count()

fact_validation_df = (
    gold_fact_order_items_df

    .agg(
        F.count("*").alias("actual_rows"),

        F.countDistinct(
            "order_item_sk"
        ).alias("distinct_fact_keys"),

        F.countDistinct(
            "order_id",
            "order_item_id"
        ).alias("distinct_business_keys"),

        F.sum(
            F.when(
                F.col("order_item_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("null_fact_keys"),

        F.sum(
            F.when(
                F.col("customer_sk").isNull()
                | F.col("product_sk").isNull()
                | F.col("seller_sk").isNull()
                | F.col("date_sk").isNull(),
                1
            ).otherwise(0)
        ).alias("null_dimension_keys"),

        F.sum(
            F.when(
                F.col("price").isNull()
                | F.col("freight_value").isNull()
                | F.col("item_total_value").isNull()
                | (F.col("price") < 0)
                | (F.col("freight_value") < 0)
                | (F.col("item_total_value") < 0),
                1
            ).otherwise(0)
        ).alias("invalid_measure_rows")
    )

    .withColumn(
        "expected_rows",
        F.lit(expected_fact_rows)
    )

    .withColumn(
        "duplicate_fact_keys",
        F.col("actual_rows")
        - F.col("distinct_fact_keys")
    )

    .withColumn(
        "duplicate_business_keys",
        F.col("actual_rows")
        - F.col("distinct_business_keys")
    )

    .withColumn(
        "status",
        F.when(
            (F.col("actual_rows") == F.col("expected_rows"))
            & (F.col("duplicate_fact_keys") == 0)
            & (F.col("duplicate_business_keys") == 0)
            & (F.col("null_fact_keys") == 0)
            & (F.col("null_dimension_keys") == 0)
            & (F.col("invalid_measure_rows") == 0),
            "PASS"
        ).otherwise("FAIL")
    )

    .select(
        "expected_rows",
        "actual_rows",
        "duplicate_fact_keys",
        "duplicate_business_keys",
        "null_fact_keys",
        "null_dimension_keys",
        "invalid_measure_rows",
        "status"
    )
)

display(fact_validation_df)

# COMMAND ----------

(
    gold_fact_order_items_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog}.{schema}.gold_fact_order_items"
    )
)

# COMMAND ----------

saved_gold_fact_df = spark.table(
    f"{catalog}.{schema}.gold_fact_order_items"
)

print(
    "Saved Gold fact rows:",
    saved_gold_fact_df.count()
)

display(
    saved_gold_fact_df.limit(10)
)

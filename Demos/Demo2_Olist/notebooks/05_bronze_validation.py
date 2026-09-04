# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

validation_df = spark.sql(f"""
WITH validation AS (

    SELECT
        'bronze_customers' AS table_name,
        99441 AS expected_rows,
        COUNT(*) AS actual_rows,
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
            AS missing_ingested_at
    FROM {catalog}.{schema}.bronze_customers

    UNION ALL

    SELECT
        'bronze_products',
        32951,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_products

    UNION ALL

    SELECT
        'bronze_sellers',
        3095,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_sellers

    UNION ALL

    SELECT
        'bronze_category_translation',
        71,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_category_translation

    UNION ALL

    SELECT
        'bronze_geolocations',
        1000163,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_geolocations

    UNION ALL

    SELECT
        'bronze_orders',
        99441,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_orders

    UNION ALL

    SELECT
        'bronze_order_items',
        112650,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_order_items

    UNION ALL

    SELECT
        'bronze_order_payments',
        103886,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_order_payments

    UNION ALL

    SELECT
        'bronze_order_reviews',
        99224,
        COUNT(*),
        SUM(CASE WHEN _ingested_at IS NULL THEN 1 ELSE 0 END)
    FROM {catalog}.{schema}.bronze_order_reviews
)

SELECT
    *,
    CASE
        WHEN actual_rows = expected_rows
             AND missing_ingested_at = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM validation
ORDER BY table_name
""")

display(validation_df)

# COMMAND ----------

key_validation_df = spark.sql(f"""
WITH key_validation AS (

    SELECT
        'bronze_customers' AS table_name,
        COUNT(*) AS invalid_keys
    FROM {catalog}.{schema}.bronze_customers
    WHERE customer_id IS NULL OR TRIM(customer_id) = ''

    UNION ALL

    SELECT
        'bronze_products',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_products
    WHERE product_id IS NULL OR TRIM(product_id) = ''

    UNION ALL

    SELECT
        'bronze_sellers',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_sellers
    WHERE seller_id IS NULL OR TRIM(seller_id) = ''

    UNION ALL

    SELECT
        'bronze_orders',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_orders
    WHERE order_id IS NULL OR TRIM(order_id) = ''

    UNION ALL

    SELECT
        'bronze_order_items',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_order_items
    WHERE order_id IS NULL
       OR TRIM(order_id) = ''
       OR order_item_id IS NULL
       OR TRIM(order_item_id) = ''

    UNION ALL

    SELECT
        'bronze_order_payments',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_order_payments
    WHERE order_id IS NULL OR TRIM(order_id) = ''

    UNION ALL

    SELECT
        'bronze_order_reviews',
        COUNT(*)
    FROM {catalog}.{schema}.bronze_order_reviews
    WHERE order_id IS NULL OR TRIM(order_id) = ''
)

SELECT
    *,
    CASE
        WHEN invalid_keys = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM key_validation
ORDER BY table_name
""")

display(key_validation_df)

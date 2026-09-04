# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

customers = spark.read.csv(f"{landing_path}/customers",header=True,inferSchema=True)
products = spark.read.csv(f"{landing_path}/products",header=True,inferSchema=True)
orders = spark.read.csv(f"{landing_path}/orders",header=True,inferSchema=True)
order_items = spark.read.csv(f"{landing_path}/order_items",header=True,inferSchema=True)
payments = spark.read.csv(f"{landing_path}/payments",header=True,inferSchema=True)
reviews = spark.read.csv(f"{landing_path}/reviews",header=True,inferSchema=True,multiLine=True,quote='"',escape='"')
category_translation = spark.read.csv(f"{landing_path}/category_translation",header=True,inferSchema=True)
sellers = spark.read.csv(f"{landing_path}/sellers",header=True,inferSchema=True)
geolocations = spark.read.csv(f"{landing_path}/geolocations",header=True,inferSchema=True)



# COMMAND ----------

display(customers.limit(10))
display(products.limit(10))
display(orders.limit(10))
display(order_items.limit(10))
display(payments.limit(10))
display(reviews.limit(10))
display(sellers.limit(10))
display(category_translation.limit(10))
display(geolocations.limit(10))

# COMMAND ----------

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

# MAGIC %md
# MAGIC display(customers.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC print(customers.schema)

# COMMAND ----------

customers.printSchema()
category_translation.printSchema()
products.printSchema()
orders.printSchema()
order_items.printSchema()
payments.printSchema()
reviews.printSchema()
sellers.printSchema()
geolocations.printSchema()

# COMMAND ----------

display(customers.count())
display(products.count())
display(category_translation.count())
display(order_items.count())
display(payments.count())
display(reviews.count())
display(sellers.count())
display(category_translation.count())
display(geolocations.count())   



# COMMAND ----------

print(customers.count())
print(products.count())
print(category_translation.count())
print(order_items.count())
print(payments.count())
print(orders.count())    
print(reviews.count())
print(sellers.count())
print(geolocations.count())

# COMMAND ----------

from pyspark.sql.functions import col

# customers
print("Rows", customers.count())
print("Distinct customer_id", customers.select("customer_id").distinct().count())
print("Null customer_id", customers.filter(col("customer_id").isNull()).count())

# products
print("Rows", products.count())
print("Distinct product_id", products.select("product_id").distinct().count())
print("Null product_id", products.filter(col("product_id").isNull()).count())

# orders
print("Rows", orders.count())
print("Distinct order_id", orders.select("order_id").distinct().count())
print("Null order_id", orders.filter(col("order_id").isNull()).count())

# order_items
print("Rows", order_items.count())
print("Distinct order_item_id", order_items.select("order_item_id").distinct().count())
print("Null order_item_id", order_items.filter(col("order_item_id").isNull()).count())

# payments
print("Rows", payments.count())
print("Distinct order_id", payments.select("order_id").distinct().count())
print("Null order_id", payments.filter(col("order_id").isNull()).count())

# reviews
print("Rows", reviews.count())
print("Distinct review_id", reviews.select("review_id").distinct().count())
print("Null review_id", reviews.filter(col("review_id").isNull()).count())

# sellers
print("Rows", sellers.count())
print("Distinct seller_id", sellers.select("seller_id").distinct().count())
print("Null seller_id", sellers.filter(col("seller_id").isNull()).count())

# category_translation
print("Rows", category_translation.count())
print("Distinct product_category_name", category_translation.select("product_category_name").distinct().count())
print("Null product_category_name", category_translation.filter(col("product_category_name").isNull()).count())

# geolocations
print("Rows", geolocations.count())
print("Distinct geolocation_zip_code_prefix", geolocations.select("geolocation_zip_code_prefix").distinct().count())
print("Null geolocation_zip_code_prefix", geolocations.filter(col("geolocation_zip_code_prefix").isNull()).count())


# COMMAND ----------

# order_items
print("Rows", order_items.count())
print("Distinct order_item_id", order_items.select("order_item_id","order_id").distinct().count())
print("Null order_item_id", order_items.filter((col("order_item_id").isNull()) | (col("order_id").isNull())).count())

# payments
print("Rows", payments.count())
print("Distinct (order_id, payment_sequential)", payments.select("order_id", "payment_sequential").distinct().count())
print("Null (order_id, payment_sequential)", payments.filter((col("order_id").isNull()) | (col("payment_sequential").isNull())).count())

# reviews
print("Rows", reviews.count())
print("Distinct (review_id, order_id)", reviews.select("review_id", "order_id").distinct().count())
print("Null (review_id, order_id)", reviews.filter((col("review_id").isNull()) | (col("order_id").isNull())).count())

# COMMAND ----------

print("Rows:", customers.count())
print(
    "Distinct customer_unique_id:",
    customers.select("customer_unique_id").distinct().count()
)

# COMMAND ----------

# orders.customer_id -> customers.customer_id
missing_customers = orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "left_anti"
)
print("Orders with missing customer:", missing_customers.count())

# order_items.order_id -> orders.order_id
missing_order_items_orders = order_items.join(
    orders,
    order_items.order_id == orders.order_id,
    "left_anti"
)
print("Order items with missing order:", missing_order_items_orders.count())

# order_items.product_id -> products.product_id
missing_order_items_products = order_items.join(
    products,
    order_items.product_id == products.product_id,
    "left_anti"
)
print("Order items with missing product:", missing_order_items_products.count())

# order_items.seller_id -> sellers.seller_id
missing_order_items_sellers = order_items.join(
    sellers,
    order_items.seller_id == sellers.seller_id,
    "left_anti"
)
print("Order items with missing seller:", missing_order_items_sellers.count())

# payments.order_id -> orders.order_id
missing_payments_orders = payments.join(
    orders,
    payments.order_id == orders.order_id,
    "left_anti"
)
print("Payments with missing order:", missing_payments_orders.count())

# reviews.order_id -> orders.order_id
missing_reviews_orders = reviews.join(
    orders,
    reviews.order_id == orders.order_id,
    "left_anti"
)
print("Reviews with missing order:", missing_reviews_orders.count())

# COMMAND ----------

# DBTITLE 1,Cell 15
import pandas as pd

# customers
print("customers:")
print(customers.select("customer_id","customer_unique_id","customer_state").toPandas().isnull().sum())

# orders
print("\norders:")
print(orders.select("order_id","customer_id","order_status","order_purchase_timestamp","order_delivered_customer_date").toPandas().isnull().sum())

# order_items
print("\norder_items:")
print(order_items.select("order_id","order_item_id","product_id","seller_id","price","freight_value").toPandas().isnull().sum())

# payments
print("\npayments:")
print(payments.select("order_id","payment_type","payment_value").toPandas().isnull().sum())

# reviews
print("\nreviews:")
print(reviews.select("review_id","order_id","review_score").toPandas().isnull().sum())

# products
print("\nproducts:")
print(products.select("product_id","product_category_name").toPandas().isnull().sum())

# sellers
print("\nsellers:")
print(sellers.select("seller_id","seller_state").toPandas().isnull().sum())





# COMMAND ----------

from pyspark.sql.functions import col, count, when

# orders → order_status counts
print("=== orders: order_status counts ===")
display(orders.groupBy("order_status").count().orderBy("count", ascending=False))

# payments → payment_type counts
print("=== payments: payment_type counts ===")
display(payments.groupBy("payment_type").count().orderBy("count", ascending=False))

# reviews → review_score counts
print("=== reviews: review_score counts ===")
display(reviews.groupBy("review_score").count().orderBy("review_score"))

# products → null category count
print("=== products: null product_category_name ===")
display(products.select(
    count(when(col("product_category_name").isNull(), 1)).alias("null_category_count"),
    count("*").alias("total_rows")
))

# customers → customer_state counts
print("=== customers: customer_state counts ===")
display(customers.groupBy("customer_state").count().orderBy("count", ascending=False))

# sellers → seller_state counts
print("=== sellers: seller_state counts ===")
display(sellers.groupBy("seller_state").count().orderBy("count", ascending=False))

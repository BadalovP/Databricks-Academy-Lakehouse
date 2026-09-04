# Purpose:
# Read the existing Gold fact table and expose selected columns
# as a pipeline-managed materialized view.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="learning_orders_base",
    comment="Base order-item data for the learning pipeline",
)
@dp.expect_or_fail(
    "order_id_not_null",
    "order_id IS NOT NULL",
)
def learning_orders_base():
    # Keep your existing return code here
    return (
        spark.read
        .table("dbr_dev.parvinbadalov.gold_fact_order_items")
        .select(
            "order_item_sk",
            "order_id",
            "order_status",
            "price",
            "freight_value",
            "item_total_value"
        )
        .filter(F.col("order_item_sk").isNotNull())
        .filter(F.col("order_id").isNotNull())
    )
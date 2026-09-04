# Purpose:
# Create an overall KPI summary from the shared base materialized view.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="learning_pipeline_summary",
    comment="Overall KPI summary for the mixed-language learning pipeline"
)
def learning_pipeline_summary():
    return (
        spark.read
        .table("learning_orders_base")
        .agg(
            F.count("*").alias("order_item_rows"),
            F.countDistinct("order_id").alias("distinct_orders"),
            F.round(F.sum("price"), 2).alias("total_price"),
            F.round(F.sum("freight_value"), 2).alias("total_freight"),
            F.round(
                F.sum("item_total_value"),
                2
            ).alias("total_value")
        )
    )
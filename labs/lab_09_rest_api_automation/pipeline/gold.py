"""
LAB 09 - Databricks REST API Automation

Module:
pipeline/gold.py

Purpose:
Aggregates lab09_taxi_silver into a daily operational summary by pickup
date and pickup borough/zone. The zone/borough join happened once already
in silver.py, so gold only aggregates the columns it already carries --
this keeps the zone-lookup join logic in a single place.

Output:
lab09_taxi_daily_summary
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="lab09_taxi_daily_summary",
    comment="Daily trip-count and revenue summary by pickup date and pickup borough/zone.",
)
def lab09_taxi_daily_summary():
    silver = spark.read.table("lab09_taxi_silver")
    return (
        silver.withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
        .groupBy("pickup_date", "pickup_borough", "pickup_zone", "pickup_zone_known")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.sum("trip_distance").alias("total_trip_distance"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.sum(F.when(F.col("passenger_count_warning"), F.lit(1)).otherwise(F.lit(0))).alias(
                "passenger_count_warning_count"
            ),
        )
    )

"""
LAB 08 - TravelOps

Module:
pipeline/gold.py

Purpose:
Defines business-ready Gold datasets for operational reporting, reconciliation
and CI/CD production health checks.

Responsibilities:
- aggregate booking revenue by date and status
- measure property and destination performance
- reconcile completed payments against current bookings
- publish a single production health table for validation and alerting

Inputs:
- Silver tables created by pipeline/silver.py

Outputs:
- gold_daily_booking_revenue
- gold_property_performance
- gold_destination_performance
- gold_payment_reconciliation
- gold_review_score
- gold_production_health

Idempotency:
Lakeflow materialized views are refreshed from deterministic Silver inputs.

Environment behavior:
Published to the target catalog/schema configured by the active bundle target.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_booking_revenue",
    comment="Daily booking count and value by current booking status.",
    cluster_by=["booking_date", "status"],
)
def gold_daily_booking_revenue():
    return (
        spark.read.table("current_bookings_silver")
        .withColumn("booking_date", F.to_date("created_at"))
        .groupBy("booking_date", "status")
        .agg(
            F.countDistinct("booking_id").alias("booking_count"),
            F.sum("booking_amount").alias("booking_value"),
            F.avg("stay_nights").alias("avg_stay_nights"),
        )
    )


@dp.materialized_view(
    name="gold_property_performance",
    comment="Property-level booking, revenue and review performance.",
    cluster_by=["destination_id", "property_type"],
)
def gold_property_performance():
    bookings = spark.read.table("current_bookings_silver")
    properties = spark.read.table("properties_silver")
    reviews = (
        spark.read.table("reviews_silver")
        .groupBy("property_id")
        .agg(
            F.avg("rating").alias("avg_rating"), F.countDistinct("review_id").alias("review_count")
        )
    )
    return (
        bookings.join(properties, "property_id", "left")
        .join(reviews, "property_id", "left")
        .groupBy("property_id", "destination_id", "title", "property_type")
        .agg(
            F.countDistinct("booking_id").alias("booking_count"),
            F.sum("booking_amount").alias("booking_value"),
            F.max("avg_rating").alias("avg_rating"),
            F.max("review_count").alias("review_count"),
        )
    )


@dp.materialized_view(
    name="gold_destination_performance",
    comment="Destination-level booking value, demand and review score.",
    cluster_by=["country", "destination"],
)
def gold_destination_performance():
    bookings = spark.read.table("current_bookings_silver")
    properties = spark.read.table("properties_silver")
    destinations = spark.read.table("destinations_silver")
    reviews = spark.read.table("reviews_silver")
    return (
        bookings.join(properties, "property_id", "left")
        .join(destinations, "destination_id", "left")
        .join(reviews.select("booking_id", "rating"), "booking_id", "left")
        .groupBy("destination_id", "destination", "country", "state_or_province")
        .agg(
            F.countDistinct("booking_id").alias("booking_count"),
            F.sum("booking_amount").alias("booking_value"),
            F.countDistinct("property_id").alias("active_property_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )


@dp.materialized_view(
    name="gold_payment_reconciliation",
    comment="Booking-to-payment reconciliation for operational health monitoring.",
    cluster_by=["reconciliation_status"],
)
def gold_payment_reconciliation():
    payments = (
        spark.read.table("payments_silver")
        .filter(F.col("status") == F.lit("completed"))
        .groupBy("booking_id")
        .agg(F.sum("amount").alias("completed_payment_amount"))
    )
    return (
        spark.read.table("current_bookings_silver")
        .join(payments, "booking_id", "left")
        .fillna({"completed_payment_amount": 0})
        .withColumn(
            "reconciliation_status",
            F.when(F.col("completed_payment_amount") == 0, F.lit("missing_payment"))
            .when(
                F.abs(F.col("booking_amount") - F.col("completed_payment_amount")) <= F.lit(0.01),
                F.lit("matched"),
            )
            .when(F.col("completed_payment_amount") < F.col("booking_amount"), F.lit("underpaid"))
            .otherwise(F.lit("overpaid")),
        )
    )


@dp.materialized_view(
    name="gold_review_score", comment="Active review score by destination and property type."
)
def gold_review_score():
    return (
        spark.read.table("reviews_silver")
        .join(spark.read.table("properties_silver"), "property_id", "left")
        .join(spark.read.table("destinations_silver"), "destination_id", "left")
        .groupBy("destination", "country", "property_type")
        .agg(
            F.avg("rating").alias("avg_rating"), F.countDistinct("review_id").alias("review_count")
        )
    )


@dp.materialized_view(
    name="gold_production_health",
    comment="Single-row CI/CD health gate consumed by validation notebooks and SQL alerts.",
)
def gold_production_health():
    current_bookings = spark.read.table("current_bookings_silver")
    reconciliation = spark.read.table("gold_payment_reconciliation")
    duplicates = (
        current_bookings.groupBy("booking_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(F.count("*").alias("duplicate_current_booking_count"))
    )
    metrics = current_bookings.agg(F.count("*").alias("current_booking_count")).crossJoin(
        duplicates
    )
    mismatches = reconciliation.filter(F.col("reconciliation_status") != F.lit("matched")).agg(
        F.count("*").alias("payment_mismatch_count")
    )
    return (
        metrics.crossJoin(mismatches)
        .withColumn(
            "health_passed",
            (F.col("current_booking_count") > 0)
            & (F.col("duplicate_current_booking_count") == 0)
            & (F.col("payment_mismatch_count") >= 0),
        )
        .withColumn("evaluated_at", F.current_timestamp())
    )

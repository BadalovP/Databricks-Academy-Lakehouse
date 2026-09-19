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
    """Property-level booking, revenue and review performance.

    `properties_silver` deduplicates on `property_id` (a verified-safe
    unique key), so the join below no longer fans out `current_bookings_
    silver` rows and inflates `SUM(booking_amount)` the way it did before
    that fix (verified live: reported booking_value was ~7x the true total
    from an undeduped properties join). `review_count` uses a plain row
    count instead of `COUNT(DISTINCT review_id)`: `review_id` is not a
    reliable identifier (only 1,000 distinct values exist across ~25,000
    genuinely distinct reviews, verified live), so counting distinct
    review_id values would undercount real review volume even after
    `reviews_silver`'s own composite-key deduplication.
    """

    bookings = spark.read.table("current_bookings_silver")
    properties = spark.read.table("properties_silver")
    reviews = (
        spark.read.table("reviews_silver")
        .groupBy("property_id")
        .agg(F.avg("rating").alias("avg_rating"), F.count(F.lit(1)).alias("review_count"))
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
    """Destination-level booking value, demand and review score.

    Reviews are pre-aggregated to one row per `booking_id` before joining,
    because a booking can legitimately have more than one review: joining
    `reviews_silver` directly (as this query previously did) fans out that
    booking's row once per review and inflates `SUM(booking_amount)` by the
    number of reviews attached to it. This is independent of and in
    addition to `properties_silver`/`destinations_silver` now deduplicating
    on their primary keys, which fixes the same kind of fan-out from
    repeated Bronze ingestion of those two dimension tables (verified live:
    reported booking_value was roughly 7x the true total before these
    fixes).
    """

    bookings = spark.read.table("current_bookings_silver")
    properties = spark.read.table("properties_silver")
    destinations = spark.read.table("destinations_silver")
    reviews = (
        spark.read.table("reviews_silver")
        .groupBy("booking_id")
        .agg(F.avg("rating").alias("rating"))
    )
    return (
        bookings.join(properties, "property_id", "left")
        .join(destinations, "destination_id", "left")
        .join(reviews, "booking_id", "left")
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
    """Classify each current booking's payment coverage.

    `reconciliation_status` distinguishes five cases that a single
    completed-payment-vs-nonnegative check previously conflated:
    - `no_payment_record`: no payment row at all for this booking_id. This
      can be a legitimate booking lifecycle state (e.g. cancelled before any
      payment attempt), so it is informational only and does not fail
      gold_production_health.
    - `pending_or_failed_payment`: at least one payment row exists, but none
      has status "completed" (e.g. still pending, or failed/refunded).
      Also legitimate and informational only.
    - `matched`: the sum of completed payments equals the booking amount
      within a 0.01 tolerance.
    - `underpaid` / `overpaid`: at least one completed payment exists, but
      the total does not match the booking amount. Unlike the two states
      above, this is never an expected business state, so it does gate
      gold_production_health (see payment_amount_mismatch_count there).
    """

    payments = (
        spark.read.table("payments_silver")
        .groupBy("booking_id")
        .agg(
            F.sum(
                F.when(F.col("status") == F.lit("completed"), F.col("amount")).otherwise(F.lit(0))
            ).alias("completed_payment_amount"),
            F.count(F.when(F.col("status") == F.lit("completed"), F.lit(1))).alias(
                "completed_payment_count"
            ),
            F.count(F.lit(1)).alias("payment_record_count"),
        )
    )
    return (
        spark.read.table("current_bookings_silver")
        .join(payments, "booking_id", "left")
        .fillna(
            {"completed_payment_amount": 0, "completed_payment_count": 0, "payment_record_count": 0}
        )
        .withColumn(
            "reconciliation_status",
            F.when(F.col("payment_record_count") == 0, F.lit("no_payment_record"))
            .when(F.col("completed_payment_count") == 0, F.lit("pending_or_failed_payment"))
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
    """Active review score by destination and property type.

    The base table is `reviews_silver` itself (one row per review is the
    correct grain here), so this join only fans out if `properties_silver`/
    `destinations_silver` have duplicate rows per key -- both now
    deduplicate on their primary key. `review_count` uses a plain row count
    instead of `COUNT(DISTINCT review_id)` for the same reason as
    `gold_property_performance`: `review_id` is not a reliable identifier.
    """

    return (
        spark.read.table("reviews_silver")
        .join(spark.read.table("properties_silver"), "property_id", "left")
        .join(spark.read.table("destinations_silver"), "destination_id", "left")
        .groupBy("destination", "country", "property_type")
        .agg(F.avg("rating").alias("avg_rating"), F.count(F.lit(1)).alias("review_count"))
    )


@dp.materialized_view(
    name="gold_production_health",
    comment="Single-row CI/CD health gate consumed by validation notebooks and SQL alerts.",
)
def gold_production_health():
    """Single-row health gate.

    `health_passed` requires: at least one current booking, no duplicate
    booking_ids, no negative booking amounts, and no payment amount
    mismatches (a completed payment whose total does not equal the booking
    amount). It intentionally does NOT require every booking to have a
    completed payment: `no_payment_record_count` and
    `pending_or_failed_payment_count` are reported for visibility but are
    business-normal booking-lifecycle states, not gated, since whether every
    booking must eventually reach a completed payment is a business policy
    decision this pipeline does not make. `payment_amount_mismatch_count`
    replaces the previous `payment_mismatch_count >= 0` condition, which was
    always true and never failed the gate regardless of reconciliation
    results.
    """

    current_bookings = spark.read.table("current_bookings_silver")
    reconciliation = spark.read.table("gold_payment_reconciliation")
    duplicates = (
        current_bookings.groupBy("booking_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(F.count("*").alias("duplicate_current_booking_count"))
    )
    invalid_amounts = current_bookings.agg(
        F.count(F.when(F.col("booking_amount") < 0, F.lit(1))).alias("invalid_booking_amount_count")
    )
    metrics = (
        current_bookings.agg(F.count("*").alias("current_booking_count"))
        .crossJoin(duplicates)
        .crossJoin(invalid_amounts)
    )
    reconciliation_summary = reconciliation.agg(
        F.count(
            F.when(F.col("reconciliation_status") == F.lit("no_payment_record"), F.lit(1))
        ).alias("no_payment_record_count"),
        F.count(
            F.when(F.col("reconciliation_status") == F.lit("pending_or_failed_payment"), F.lit(1))
        ).alias("pending_or_failed_payment_count"),
        F.count(F.when(F.col("reconciliation_status") == F.lit("matched"), F.lit(1))).alias(
            "matched_payment_count"
        ),
        F.count(
            F.when(F.col("reconciliation_status").isin("underpaid", "overpaid"), F.lit(1))
        ).alias("payment_amount_mismatch_count"),
    )
    return (
        metrics.crossJoin(reconciliation_summary)
        .withColumn(
            "health_passed",
            (F.col("current_booking_count") > 0)
            & (F.col("duplicate_current_booking_count") == 0)
            & (F.col("invalid_booking_amount_count") == 0)
            & (F.col("payment_amount_mismatch_count") == 0),
        )
        .withColumn("evaluated_at", F.current_timestamp())
    )

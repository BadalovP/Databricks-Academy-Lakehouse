"""
LAB 08 - TravelOps

Module:
pipeline/silver.py

Purpose:
Defines validated and normalized TravelOps Silver datasets.

Responsibilities:
- enforce reusable data-quality expectations
- normalize booking and payment status text
- derive current booking state from base bookings plus update events
- filter soft-deleted reviews for downstream analytics

Inputs:
- Bronze tables created by pipeline/bronze.py

Outputs:
- bookings_silver
- current_bookings_silver
- payments_silver
- users_silver
- properties_silver
- reviews_silver
- destinations_silver

Idempotency:
Datasets are declaratively managed by Lakeflow and recomputed from deterministic
Bronze inputs.

Environment behavior:
Published to the pipeline target catalog/schema configured by the active bundle
target. DEV and PROD use dedicated target schemas, so table names are consistent
and unprefixed across environments.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

BOOKING_EXPECTATIONS = {
    "valid_booking_id": "booking_id IS NOT NULL",
    "valid_booking_dates": "check_in IS NOT NULL AND check_out IS NOT NULL AND check_out > check_in",
    "valid_guest_count": "guests_count BETWEEN 1 AND 20",
    "valid_booking_amount": "total_amount >= 0",
    "valid_booking_status": "status IN ('pending', 'confirmed', 'cancelled', 'completed')",
}

PAYMENT_EXPECTATIONS = {
    "valid_payment_id": "payment_id IS NOT NULL",
    "valid_payment_amount": "amount >= 0",
    "valid_payment_status": "status IN ('pending', 'completed', 'failed', 'refunded')",
}

REVIEW_EXPECTATIONS = {
    "valid_review_id": "review_id IS NOT NULL",
    "valid_rating_range": "rating IS NULL OR rating BETWEEN 1.0 AND 5.0",
}


def _private_silver() -> bool:
    """Return whether Silver should be a private pipeline intermediate."""

    return spark.conf.get("travelops.private_silver", "false").lower() == "true"


@dp.materialized_view(
    name="bookings_silver",
    comment="Validated booking rows with normalized status and stay-night metrics.",
    cluster_by=["check_in", "status"],
    private=_private_silver(),
)
@dp.expect_all_or_drop(BOOKING_EXPECTATIONS)
def bookings_silver():
    return (
        spark.read.table("bookings_bronze")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("stay_nights", F.datediff(F.col("check_out"), F.col("check_in")))
        .withColumn("booking_amount", F.col("total_amount").cast("decimal(15,4)"))
    )


@dp.materialized_view(
    name="current_bookings_silver",
    comment="Latest known booking state after applying booking update events.",
    cluster_by=["check_in", "status"],
    private=_private_silver(),
)
def current_bookings_silver():
    base = spark.read.table("bookings_silver").withColumn(
        "_change_sequence", F.col("updated_at").cast("timestamp")
    )
    updates = (
        spark.read.table("booking_updates_bronze")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("stay_nights", F.datediff(F.col("check_out"), F.col("check_in")))
        .withColumn("booking_amount", F.col("total_amount").cast("decimal(15,4)"))
        .withColumn("_change_sequence", F.col("updated_at").cast("timestamp"))
        .select(base.columns)
    )
    ranked = base.unionByName(updates).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("booking_id").orderBy(
                F.col("_change_sequence").desc(), F.col("_travelops_ingested_at").desc()
            )
        ),
    )
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


@dp.materialized_view(
    name="payments_silver",
    comment="Validated payment rows with normalized payment status.",
    cluster_by=["payment_date", "status"],
    private=_private_silver(),
)
@dp.expect_all_or_drop(PAYMENT_EXPECTATIONS)
def payments_silver():
    return spark.read.table("payments_bronze").withColumn(
        "status", F.lower(F.trim(F.col("status")))
    )


@dp.materialized_view(
    name="users_silver",
    comment="User dimension with minimal PII retained for lab analytics.",
    private=_private_silver(),
)
def users_silver():
    return spark.read.table("users_bronze").select(
        "user_id",
        "country",
        "user_type",
        "is_business",
        "company_name",
        "created_at",
        "_travelops_ingested_at",
    )


@dp.materialized_view(
    name="properties_silver",
    comment="Property dimension enriched with geospatial coordinates and capacity attributes.",
    cluster_by=["destination_id", "property_type"],
    private=_private_silver(),
)
def properties_silver():
    return spark.read.table("properties_bronze").withColumn(
        "base_price_amount", F.col("base_price").cast("decimal(15,4)")
    )


@dp.materialized_view(
    name="reviews_silver",
    comment="Active review records with rating range expectations.",
    cluster_by=["property_id"],
    private=_private_silver(),
)
@dp.expect_all_or_drop(REVIEW_EXPECTATIONS)
def reviews_silver():
    return spark.read.table("reviews_bronze").filter(F.col("is_deleted") == F.lit(False))


@dp.materialized_view(
    name="destinations_silver",
    comment="Destination dimension for TravelOps reporting.",
    private=_private_silver(),
)
def destinations_silver():
    return spark.read.table("destinations_bronze")

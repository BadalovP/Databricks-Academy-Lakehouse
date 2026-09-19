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
Silver materialized views are fully recomputed from Bronze on every run, so
they self-heal even though Bronze itself is not idempotent: the raw seeding
notebook overwrites each source Parquet folder with new physical files on
every run, and Auto Loader has no way to recognize that a newly written file
carries previously ingested content, so Bronze tables accumulate duplicate
rows across repeated deployments. `current_bookings_silver` already absorbs
this for bookings by keeping only the latest row per `booking_id`.
`payments_silver` explicitly deduplicates on the composite business key
below for the same reason: `payment_id` alone is not a safe key because the
source data reuses some `payment_id` values across genuinely different
events (different `booking_id`, `amount`, `status` or `payment_date`), so
deduplicating by `payment_id` alone would silently discard distinct events.
Historical duplicates already accumulated in Bronze before this fix are not
removed by it; see `evidence/lab08_photon_fix_and_reconciliation_observation.md`
and the production remediation plan for that separate cleanup decision.
`booking_updates_bronze` previously bypassed `BOOKING_EXPECTATIONS` entirely
because `current_bookings_silver` read it directly instead of through a
quality-gated Silver table; it now passes through the same expectations as
new bookings before being merged into `current_bookings_silver`.

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

# payment_id alone is not a safe deduplication key: some payment_id values are
# reused across genuinely different events (different booking_id, amount,
# status or payment_date). Deduplicating on this full tuple collapses only
# exact-duplicate rows (the shape produced by repeated raw-file ingestion)
# while preserving distinct events that happen to share a payment_id.
PAYMENT_BUSINESS_KEY = ["payment_id", "booking_id", "amount", "status", "payment_date"]


def _private_silver() -> bool:
    """Return whether Silver should be a private pipeline intermediate."""

    return spark.conf.get("travelops.private_silver", "false").lower() == "true"


def _booking_quality_filter(df):
    """Apply BOOKING_EXPECTATIONS as an inline filter.

    Reuses the same predicates enforced on new bookings via
    @dp.expect_all_or_drop so booking_updates_bronze is held to an identical
    data contract instead of bypassing it.
    """

    condition = None
    for predicate in BOOKING_EXPECTATIONS.values():
        clause = F.expr(predicate)
        condition = clause if condition is None else condition & clause
    return df.filter(condition)


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
    updates = _booking_quality_filter(
        spark.read.table("booking_updates_bronze")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("stay_nights", F.datediff(F.col("check_out"), F.col("check_in")))
        .withColumn("booking_amount", F.col("total_amount").cast("decimal(15,4)"))
        .withColumn("_change_sequence", F.col("updated_at").cast("timestamp"))
    ).select(base.columns)
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
    return (
        spark.read.table("payments_bronze")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .dropDuplicates(PAYMENT_BUSINESS_KEY)
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

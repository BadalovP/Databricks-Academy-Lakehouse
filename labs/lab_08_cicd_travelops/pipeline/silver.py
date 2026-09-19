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
Silver materialized views are fully recomputed from Bronze on every pipeline
run. `notebooks/00_seed_raw_data.ipynb` treats each raw table's seed as an
immutable, write-once snapshot identified by an explicit, human-controlled
`(SEED_VERSION, seed_limit)` pair plus the sampled row count, rather than
resampling on every run: it writes once, skips on an unchanged rerun, and
raises instead of writing if the row count differs under an unchanged
identity. Auto Loader's default `cloudFiles.allowOverwrites=false` should
then skip reprocessing a rerun with unchanged content — a genuine
ingestion-idempotency fix at the source, not merely a downstream
mitigation; see that notebook's architecture decision for the full
reasoning and its residual limitations (not empirically validated by an
actual run in this repository, and row count is multiplicity-aware but not
a full content check — a same-row-count in-place value edit is not
detected). Deduplication here in Silver remains
a necessary defense-in-depth backstop, not a claim that it prevents Bronze
from accumulating duplicates: it does not shrink or stop Bronze row growth
by itself, it only prevents whatever duplication Bronze does have from
corrupting Silver/Gold results. `current_bookings_silver` absorbs bookings
duplication by keeping only the latest row per `booking_id`. `payments_silver`
explicitly deduplicates on the composite business key below for the same
reason: `payment_id` alone is not a safe key because the source data reuses
some `payment_id` values across genuinely different events (different
`booking_id`, `amount`, `status` or `payment_date`), so deduplicating by
`payment_id` alone would silently discard distinct events. Historical
duplicates already accumulated in Bronze before the ingestion fix shipped
are not removed by either fix; see
`evidence/lab08_photon_fix_and_reconciliation_observation.md` and
`evidence/lab08_production_remediation_plan.md` for that separate cleanup
decision and its validation/recovery procedure.
`booking_updates_bronze` previously bypassed `BOOKING_EXPECTATIONS` entirely
because `current_bookings_silver` read it directly instead of through a
quality-gated Silver table; it now passes through the same expectations as
new bookings before being merged into `current_bookings_silver`.
`current_bookings_silver`'s "latest state" selection also previously ordered
candidate rows only by `updated_at` then `_travelops_ingested_at`, both of
which can be identical across a booking's own update events in this source
(verified live on booking_id 13594: an intermediate, superseded update and
the true final `confirmed` update shared one `updated_at`), making the
selection arbitrary on a tie. It now also orders by the update event's own
`booking_update_id` (a monotonically increasing per-event identity,
preserved through the union instead of being dropped) as an additional
tie-break, with a sentinel value ranking the base booking row below any of
its own real update events.

The same repeated-ingestion duplication affects `properties_bronze`,
`destinations_bronze`, `users_bronze` and `reviews_bronze` (verified: each
holds roughly 7x its distinct business-row count). Unlike Gold tables that
read only from `current_bookings_silver` (already deduped), `gold_property_
performance` and `gold_destination_performance` join `current_bookings_silver`
to `properties_silver`/`destinations_silver`, so undeduped duplicate rows in
those dimension tables fan out the join and inflate `SUM(booking_amount)` by
the same factor — verified live on Azure PROD at roughly 7x
($97.2M reported vs. a $13.9M ground truth from `current_bookings_silver`
alone). `property_id`, `destination_id` and `user_id` were each confirmed
(by comparing distinct full-business-row counts to distinct key counts) to
be safe, uniquely-identifying keys with no cross-record reuse, unlike
`payment_id` — so `properties_silver`, `destinations_silver` and
`users_silver` deduplicate on their primary key alone. `reviews_bronze` is
different again: only 1,000 distinct `review_id` values exist across 24,999
genuinely distinct review records (confirmed the same way), meaning
`review_id` is far less reliable a key than even `payment_id` — deduplicating
`reviews_silver` by `review_id` alone would silently discard the vast
majority of real reviews, so it deduplicates on the full composite
`REVIEW_BUSINESS_KEY` below instead, exactly like `payments_silver`.
`pipeline/gold.py`'s `gold_property_performance`/`gold_review_score` also
switch their `review_count` metric from `COUNT(DISTINCT review_id)` to a
plain row count post-dedup, since `review_id` cannot be trusted to
distinguish reviews; `gold_destination_performance` additionally
pre-aggregates reviews by `booking_id` before joining, because a booking can
legitimately have more than one review, which fanned out `SUM(booking_amount)`
independently of any ingestion duplication.

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

# review_id is even less reliable than payment_id: verified live, only 1,000
# distinct review_id values exist across 24,999 genuinely distinct review
# records. Deduplicating on this full tuple collapses only exact-duplicate
# rows while preserving every genuinely distinct review, regardless of how
# many other reviews happen to share its review_id.
REVIEW_BUSINESS_KEY = [
    "review_id",
    "booking_id",
    "property_id",
    "user_id",
    "rating",
    "comment",
    "created_at",
    "updated_at",
    "is_deleted",
]


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
    # This source's `updated_at` is not fine-grained enough to order a
    # booking's own update events: multiple booking_updates rows for the same
    # booking_id, and even the base booking row, commonly share one identical
    # `updated_at` value (and, since they are ingested together, an identical
    # `_travelops_ingested_at` too) -- verified live on booking_id 13594,
    # where an intermediate $818.56 update and the true final $775.56/
    # `confirmed` update both carried the same `updated_at`, causing the
    # previous two-column ordering to pick between them arbitrarily.
    # `booking_update_id` is a monotonically increasing per-event identity
    # that disambiguates this correctly; it is preserved through the union
    # below (as `_booking_update_id`) instead of being dropped, and used as
    # the tie-break between `_change_sequence` and `_travelops_ingested_at`.
    # A base bookings_silver row has no `booking_update_id` of its own, so it
    # is given a sentinel of -1 -- always lower than a real update's ID -- so
    # a booking's own update events always outrank the row it started from
    # once their timestamps tie.
    base = (
        spark.read.table("bookings_silver")
        .withColumn("_change_sequence", F.col("updated_at").cast("timestamp"))
        .withColumn("_booking_update_id", F.lit(-1).cast("long"))
    )
    updates = _booking_quality_filter(
        spark.read.table("booking_updates_bronze")
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("stay_nights", F.datediff(F.col("check_out"), F.col("check_in")))
        .withColumn("booking_amount", F.col("total_amount").cast("decimal(15,4)"))
        .withColumn("_change_sequence", F.col("updated_at").cast("timestamp"))
        .withColumn("_booking_update_id", F.col("booking_update_id").cast("long"))
    ).select(base.columns)
    ranked = base.unionByName(updates).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("booking_id").orderBy(
                F.col("_change_sequence").desc(),
                F.col("_booking_update_id").desc(),
                F.col("_travelops_ingested_at").desc(),
            )
        ),
    )
    return ranked.filter(F.col("_rn") == 1).drop("_rn", "_booking_update_id")


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
    return (
        spark.read.table("users_bronze")
        .select(
            "user_id",
            "country",
            "user_type",
            "is_business",
            "company_name",
            "created_at",
            "_travelops_ingested_at",
        )
        .dropDuplicates(["user_id"])
    )


@dp.materialized_view(
    name="properties_silver",
    comment="Property dimension enriched with geospatial coordinates and capacity attributes.",
    cluster_by=["destination_id", "property_type"],
    private=_private_silver(),
)
def properties_silver():
    return (
        spark.read.table("properties_bronze")
        .withColumn("base_price_amount", F.col("base_price").cast("decimal(15,4)"))
        .dropDuplicates(["property_id"])
    )


@dp.materialized_view(
    name="reviews_silver",
    comment="Active review records with rating range expectations.",
    cluster_by=["property_id"],
    private=_private_silver(),
)
@dp.expect_all_or_drop(REVIEW_EXPECTATIONS)
def reviews_silver():
    return (
        spark.read.table("reviews_bronze")
        .dropDuplicates(REVIEW_BUSINESS_KEY)
        .filter(F.col("is_deleted") == F.lit(False))
    )


@dp.materialized_view(
    name="destinations_silver",
    comment="Destination dimension for TravelOps reporting.",
    private=_private_silver(),
)
def destinations_silver():
    return spark.read.table("destinations_bronze").dropDuplicates(["destination_id"])

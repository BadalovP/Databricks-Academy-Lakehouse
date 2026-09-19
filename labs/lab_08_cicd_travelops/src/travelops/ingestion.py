"""
LAB 08 - TravelOps

Module:
src/travelops/ingestion.py

Purpose:
Local, Spark-free mirrors of the raw-seeding and Bronze-to-Silver
deduplication behavior in notebooks/00_seed_raw_data.ipynb and
pipeline/silver.py, so the ingestion-idempotency and referential-sampling
fixes are unit-testable without a Databricks runtime.

Responsibilities:
- define the payment and review business keys used for deduplication
- deduplicate payment/review records on those keys without collapsing
  distinct events
- deduplicate dimension records (properties/destinations/users) on their
  verified-safe primary key
- filter booking-scoped records to a sampled booking_id set
- derive the deterministic, seed_limit-and-content-fingerprint-based raw
  file name used for genuine source-level ingestion idempotency

Inputs:
Plain dictionaries representing rows in local tests; mirrors PySpark
DataFrame operations in the pipeline files.

Outputs:
Deduplicated/filtered lists of records used by tests to prove the pipeline
logic they mirror.

Idempotency:
Functions are deterministic and side-effect free. deduplicate_payment_records,
deduplicate_review_records and deduplicate_by_primary_key are each
idempotent: applying any of them to its own output is a no-op.

Environment behavior:
Logic is environment-agnostic; it mirrors pipeline behavior shared by every
DAB target.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

# payment_id alone is not a safe deduplication key: the TravelOps payments
# source reuses some payment_id values across genuinely different events
# (different booking_id, amount, status or payment_date). Deduplicating on
# this full tuple mirrors pipeline/silver.py's PAYMENT_BUSINESS_KEY: it
# collapses only exact-duplicate rows (the shape produced by Auto Loader
# re-ingesting an overwritten raw file) while preserving distinct events that
# happen to share a payment_id.
PAYMENT_BUSINESS_KEY_FIELDS = ("payment_id", "booking_id", "amount", "status", "payment_date")


def payment_business_key(record: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return the composite key that identifies a distinct payment event."""

    return tuple(record.get(field) for field in PAYMENT_BUSINESS_KEY_FIELDS)


def deduplicate_payment_records(
    records: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse exact-duplicate payment events.

    Two records are duplicates of the same event only when every field in
    PAYMENT_BUSINESS_KEY_FIELDS is equal. Records that share a payment_id but
    differ in booking_id, amount, status or payment_date are preserved as
    separate events. Applying this function to its own output is a no-op,
    matching pipeline/silver.py's `.dropDuplicates(PAYMENT_BUSINESS_KEY)`,
    which is safe to recompute on every pipeline run regardless of how many
    times the underlying Bronze table has accumulated repeat ingestions.
    """

    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = payment_business_key(record)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(record))
    return deduped


# review_id is far less reliable than payment_id: verified live against Azure
# PROD, only 1,000 distinct review_id values exist across 24,999 genuinely
# distinct review records. Deduplicating on this full tuple mirrors
# pipeline/silver.py's REVIEW_BUSINESS_KEY: it collapses only exact-duplicate
# rows while preserving every genuinely distinct review, regardless of how
# many other reviews happen to share its review_id.
REVIEW_BUSINESS_KEY_FIELDS = (
    "review_id",
    "booking_id",
    "property_id",
    "user_id",
    "rating",
    "comment",
    "created_at",
    "updated_at",
    "is_deleted",
)


def review_business_key(record: Mapping[str, Any]) -> tuple[Any, ...]:
    """Return the composite key that identifies a distinct review event."""

    return tuple(record.get(field) for field in REVIEW_BUSINESS_KEY_FIELDS)


def deduplicate_review_records(
    records: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse exact-duplicate review events, mirroring deduplicate_payment_records.

    Two records are duplicates of the same event only when every field in
    REVIEW_BUSINESS_KEY_FIELDS is equal. Records that share a review_id but
    differ in any other field are preserved as separate events -- this
    matters far more for reviews than for payments, since review_id was
    found to be reused across roughly 25 genuinely distinct reviews on
    average.
    """

    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = review_business_key(record)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(record))
    return deduped


def deduplicate_by_primary_key(
    records: Iterable[Mapping[str, Any]], key_field: str
) -> list[dict[str, Any]]:
    """Collapse duplicate rows sharing key_field, keeping one arbitrary row per key.

    Unlike payment_id/review_id, property_id, destination_id and user_id
    were each verified live (comparing distinct full-business-row counts to
    distinct key counts) to have zero cross-record reuse: every distinct
    business row maps to exactly one key value. Deduplicating on the key
    alone is therefore safe for those three tables specifically -- it is
    not a generally safe pattern for every key, as payment_id and review_id
    demonstrate.
    """

    seen_keys: set[Any] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = record.get(key_field)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(dict(record))
    return deduped


def filter_to_sampled_bookings(
    records: Iterable[Mapping[str, Any]], sampled_booking_ids: Iterable[Any]
) -> list[dict[str, Any]]:
    """Keep only records whose booking_id is in the sampled set.

    Mirrors the left-semi join used in 00_seed_raw_data.ipynb to seed
    booking-scoped tables (payments, booking_updates, reviews): each is
    filtered to the exact booking_id set sampled from bookings, instead of
    being truncated independently by its own row order. All matching records
    for a sampled booking are kept, including multiple legitimate events
    sharing a booking_id.
    """

    allowed = set(sampled_booking_ids)
    return [dict(record) for record in records if record.get("booking_id") in allowed]


def seed_file_name(seed_limit: int, content_fingerprint: str) -> str:
    """Deterministic Parquet file name for a raw seed's content version.

    Mirrors 00_seed_raw_data.ipynb's `_seed_file_name`: encoding both
    seed_limit and a content fingerprint means the path is stable when the
    actual sampled rows are unchanged, and changes whenever they are not --
    whether from a seed_limit config change or from the upstream
    samples.wanderbricks source itself changing while seed_limit stays the
    same. seed_limit alone is not a safe content version, since two runs
    with the same seed_limit could still sample different upstream data;
    only a fingerprint of the actual content can distinguish that case. Auto
    Loader's documented default cloudFiles.allowOverwrites=false means it
    will not reprocess a path it has already ingested, so reusing the same
    path for unchanged content is what stops Bronze from accumulating
    duplicates at the source, rather than only being cleaned up downstream
    in Silver.

    This is a Spark-free mirror of the notebook's file-naming logic only. It
    cannot compute an actual content fingerprint (that requires hashing a
    real DataFrame, which requires a Databricks runtime) or exercise Auto
    Loader's actual file-discovery behavior; see
    evidence/lab08_production_remediation_plan.md for the corresponding
    integration validation procedure.
    """

    return f"seed-{seed_limit}-{content_fingerprint}.snappy.parquet"

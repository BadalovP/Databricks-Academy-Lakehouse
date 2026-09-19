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
- define the payment business key used for deduplication
- deduplicate payment records on that key without collapsing distinct events
- filter booking-scoped records to a sampled booking_id set
- derive the deterministic, seed_limit-based raw file name used for
  genuine source-level ingestion idempotency

Inputs:
Plain dictionaries representing rows in local tests; mirrors PySpark
DataFrame operations in the pipeline files.

Outputs:
Deduplicated/filtered lists of records used by tests to prove the pipeline
logic they mirror.

Idempotency:
Functions are deterministic and side-effect free. deduplicate_payment_records
is itself idempotent: applying it to its own output is a no-op.

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


def seed_file_name(seed_limit: int) -> str:
    """Deterministic Parquet file name for a raw seed at a given seed_limit.

    Mirrors 00_seed_raw_data.ipynb's `_seed_file_name`: encoding seed_limit
    in the path means a rerun with an unchanged seed_limit writes to the
    same path every time, while a deliberate seed_limit change produces a
    new path. Auto Loader's documented default
    cloudFiles.allowOverwrites=false means it will not reprocess a path it
    has already ingested, even if that path is later overwritten with new
    bytes -- so reusing this same path on an unchanged-config rerun is what
    stops Bronze from accumulating duplicates at the source, rather than
    only being cleaned up downstream in Silver.

    This is a Spark-free mirror of the notebook's file-naming logic only. It
    cannot exercise Auto Loader's actual file-discovery behavior, which
    requires a real Databricks runtime; see
    evidence/lab08_production_remediation_plan.md for the corresponding
    integration validation procedure. It also does not address every
    possible content-change scenario -- for example, the upstream
    samples.wanderbricks source changing without any local seed_limit
    change keeps the same path and would not be picked up.
    """

    return f"seed-{seed_limit}.snappy.parquet"

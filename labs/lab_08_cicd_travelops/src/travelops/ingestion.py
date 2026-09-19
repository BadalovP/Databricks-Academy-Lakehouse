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
- derive the deterministic, (SEED_VERSION, seed_limit, row_count)-based raw
  file identity used for the immutable, write-once seed design, and mirror
  the notebook's write/skip/fail decision logic for that design

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


def seed_identity_prefix(seed_version: str, seed_limit: int) -> str:
    """Human-controlled identity prefix mirroring 00_seed_raw_data.ipynb's `_seed_identity_prefix`.

    seed_version and seed_limit are the only two things allowed to define a
    new seed identity, and both require a deliberate, reviewed code/config
    change -- never inferred from data content. This is why the design
    replaced a content fingerprint (which silently detects some changes and
    misses others, and cancels under XOR) with an explicit, human-owned
    identity plus the honest, limited verification in seed_file_name below.
    """

    return f"seed-{seed_version}-{seed_limit}-"


def seed_file_name(seed_version: str, seed_limit: int, row_count: int) -> str:
    """Deterministic file name for a seed at a given identity and row count.

    Mirrors 00_seed_raw_data.ipynb's `_seed_file_name`. Encoding row_count
    directly in the name is the verification signal: unlike a hash-based
    fingerprint, it cannot cancel out a multiplicity change (one copy vs.
    three copies of the same row always produce different counts -- the
    exact case where the prior bit_xor(xxhash64(...)) fingerprint could
    collide), but it is an explicitly limited signal: a same-row-count
    in-place value edit is not detected. This is accepted because the
    source (samples.wanderbricks) is a fixed sample dataset, not a live
    feed -- see 00_seed_raw_data.ipynb's architecture decision.
    """

    return f"{seed_identity_prefix(seed_version, seed_limit)}{row_count}rows.snappy.parquet"


def decide_seed_action(
    existing_file_names: Iterable[str], seed_version: str, seed_limit: int, row_count: int
) -> tuple[str, str]:
    """Pure-Python mirror of the seed notebook's per-table control flow.

    Returns (action, file_name):
    - "write": no file exists yet for this identity; write it once.
    - "skip": a file already exists for this identity with this exact row
      count; do nothing (the common, fully idempotent case).
    - "fail": a file exists for this identity but with a different row
      count -- samples.wanderbricks appears to have changed under an
      identity declared immutable. The notebook raises rather than
      silently keeping stale data or silently writing a second,
      incompatible snapshot.

    This mirrors the notebook's decision logic exactly, but it is a
    Spark-free simulation: it cannot exercise the actual dbutils.fs/Spark
    file operations, Auto Loader's real file-discovery and
    cloudFiles.allowOverwrites behavior, or a genuine interrupted write
    against a live Volume. See tests/test_seed_ingestion_simulation.py for
    a simulated (not pure-mirror) test of the file-operation sequence, and
    evidence/lab08_production_remediation_plan.md for what still requires
    a real Databricks run.
    """

    prefix = seed_identity_prefix(seed_version, seed_limit)
    file_name = seed_file_name(seed_version, seed_limit, row_count)
    matching = [name for name in existing_file_names if name.startswith(prefix)]
    if not matching:
        return ("write", file_name)
    if file_name in matching:
        return ("skip", file_name)
    return ("fail", file_name)

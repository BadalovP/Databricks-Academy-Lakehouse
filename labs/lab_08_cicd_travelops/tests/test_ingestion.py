"""
LAB 08 - TravelOps

Test module:
tests/test_ingestion.py

Purpose:
Checks the local mirrors of the ingestion-idempotency and referential-sampling
fixes: payment/review deduplication on composite business keys, dimension
deduplication on verified-safe primary keys, and booking-scoped sampling for
payments/booking_updates/reviews.

Inputs:
Small in-memory Python records.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are side-effect free.

Local unit tests vs Databricks integration:
These tests exercise the pure-Python mirrors in src/travelops/ingestion.py,
not the actual PySpark pipeline code in pipeline/silver.py or the Auto
Loader/notebook seeding behavior. They prove the intended business-key,
filtering and file-naming logic in isolation; they cannot exercise
Lakeflow's @dp.expect_all_or_drop wiring, Auto Loader's actual
cloudFiles.allowOverwrites file-discovery behavior, or an actual
repeated-deployment scenario against a live Volume. In particular,
test_seed_file_name_* below prove only that the notebook's file-naming
function is deterministic and config-sensitive -- they cannot confirm that
Auto Loader actually skips re-ingesting a stable path, which requires a
real Databricks run (see evidence/lab08_production_remediation_plan.md's
validation procedure). Only a Databricks bundle run (not executed as part
of this change) can validate the end-to-end behavior of the real pipeline.
"""

from travelops.ingestion import (
    deduplicate_by_primary_key,
    deduplicate_payment_records,
    deduplicate_review_records,
    filter_to_sampled_bookings,
    payment_business_key,
    review_business_key,
    seed_file_name,
)


def _payment(**overrides: object) -> dict:
    payment = {
        "payment_id": 501,
        "booking_id": 22691,
        "amount": "305.74",
        "status": "completed",
        "payment_date": "2026-01-06",
    }
    payment.update(overrides)
    return payment


def test_repeated_ingestion_of_identical_records_collapses_to_one() -> None:
    # Mirrors Auto Loader re-ingesting the same seeded row from a newly
    # overwritten raw file: the same logical event appears multiple times.
    records = [_payment(), _payment(), _payment()]

    deduped = deduplicate_payment_records(records)

    assert deduped == [_payment()]


def test_exact_duplicate_payment_records_collapse_regardless_of_count() -> None:
    records = [_payment() for _ in range(7)]

    deduped = deduplicate_payment_records(records)

    assert len(deduped) == 1


def test_multiple_legitimate_events_sharing_payment_id_are_preserved() -> None:
    # The TravelOps payments source reuses some payment_id values across
    # genuinely different events. Deduplicating on payment_id alone would
    # wrongly collapse these into one row.
    first_event = _payment(amount="100.00", status="failed", payment_date="2026-01-01")
    second_event = _payment(amount="100.00", status="completed", payment_date="2026-01-03")

    deduped = deduplicate_payment_records([first_event, second_event])

    assert len(deduped) == 2
    assert first_event in deduped
    assert second_event in deduped


def test_payment_business_key_ignores_payment_id_alone() -> None:
    a = _payment(status="failed")
    b = _payment(status="completed")

    assert payment_business_key(a) != payment_business_key(b)
    assert payment_business_key(a)[0] == payment_business_key(b)[0]  # same payment_id


def test_deduplication_is_idempotent_on_repeated_execution() -> None:
    records = [_payment(), _payment(), _payment(amount="999.00")]

    once = deduplicate_payment_records(records)
    twice = deduplicate_payment_records(once)

    assert once == twice


def test_referentially_consistent_sampling_keeps_only_related_bookings() -> None:
    sampled_booking_ids = {1, 2}
    payments = [
        _payment(booking_id=1, payment_id=901),
        _payment(booking_id=2, payment_id=902),
        _payment(booking_id=3, payment_id=903),  # not in the sampled bookings
    ]

    filtered = filter_to_sampled_bookings(payments, sampled_booking_ids)

    assert {p["booking_id"] for p in filtered} == {1, 2}


def test_referentially_consistent_sampling_preserves_multiple_events_per_booking() -> None:
    sampled_booking_ids = {1}
    payments = [
        _payment(booking_id=1, payment_id=901, status="failed"),
        _payment(booking_id=1, payment_id=902, status="completed"),
    ]

    filtered = filter_to_sampled_bookings(payments, sampled_booking_ids)

    assert len(filtered) == 2


def test_seed_file_name_is_stable_for_unchanged_seed_limit_and_fingerprint() -> None:
    # A rerun with the same seed_limit and the same content fingerprint must
    # resolve to the same path so Auto Loader's default
    # cloudFiles.allowOverwrites=false recognizes it as already-ingested and
    # skips reprocessing it.
    assert seed_file_name(25000, "abc123") == seed_file_name(25000, "abc123")


def test_seed_file_name_changes_when_seed_limit_changes() -> None:
    # A deliberate configuration change must still be picked up as new
    # content by Auto Loader, so the path must differ.
    assert seed_file_name(25000, "abc123") != seed_file_name(30000, "abc123")


def test_seed_file_name_changes_when_content_fingerprint_changes() -> None:
    # An upstream source change that leaves seed_limit unchanged must still
    # be picked up as new content -- this is why seed_limit alone is not a
    # safe content version and the fingerprint is required.
    assert seed_file_name(25000, "abc123") != seed_file_name(25000, "def456")


def _review(**overrides: object) -> dict:
    review = {
        "review_id": 42,
        "booking_id": 22691,
        "property_id": 501,
        "user_id": 9001,
        "rating": 4.5,
        "comment": "Great stay",
        "created_at": "2026-01-06",
        "updated_at": "2026-01-06",
        "is_deleted": False,
    }
    review.update(overrides)
    return review


def test_repeated_review_ingestion_collapses_to_one() -> None:
    records = [_review(), _review(), _review()]

    deduped = deduplicate_review_records(records)

    assert deduped == [_review()]


def test_reviews_sharing_a_review_id_are_preserved_when_genuinely_different() -> None:
    # review_id was found to be reused across roughly 25 genuinely distinct
    # reviews on average (only 1,000 distinct review_id values across 24,999
    # distinct review records, verified live). Deduplicating by review_id
    # alone would silently discard almost all real reviews.
    first_review = _review(booking_id=1, rating=5.0, comment="Loved it")
    second_review = _review(booking_id=2, rating=2.0, comment="Not great")

    deduped = deduplicate_review_records([first_review, second_review])

    assert len(deduped) == 2
    assert review_business_key(first_review) != review_business_key(second_review)
    assert review_business_key(first_review)[0] == review_business_key(second_review)[0]


def test_review_deduplication_is_idempotent_on_repeated_execution() -> None:
    records = [_review(), _review(), _review(rating=1.0)]

    once = deduplicate_review_records(records)
    twice = deduplicate_review_records(once)

    assert once == twice


def _property(**overrides: object) -> dict:
    prop = {"property_id": 501, "destination_id": 7, "title": "Seaside Villa", "base_price": 200.0}
    prop.update(overrides)
    return prop


def test_repeated_dimension_ingestion_collapses_by_primary_key() -> None:
    # property_id, destination_id and user_id were each verified live to
    # have zero cross-record reuse (unlike payment_id/review_id), so a plain
    # primary-key dedup is safe for these dimension tables specifically.
    records = [_property(), _property(), _property()]

    deduped = deduplicate_by_primary_key(records, "property_id")

    assert len(deduped) == 1


def test_dimension_deduplication_keeps_distinct_keys() -> None:
    records = [_property(property_id=1), _property(property_id=2)]

    deduped = deduplicate_by_primary_key(records, "property_id")

    assert {r["property_id"] for r in deduped} == {1, 2}


def test_dimension_deduplication_is_idempotent_on_repeated_execution() -> None:
    records = [_property(), _property(), _property(property_id=2)]

    once = deduplicate_by_primary_key(records, "property_id")
    twice = deduplicate_by_primary_key(once, "property_id")

    assert once == twice

"""
LAB 08 - TravelOps

Test module:
tests/test_ingestion.py

Purpose:
Checks the local mirrors of the ingestion-idempotency and referential-sampling
fixes: payment deduplication on a composite business key, and booking-scoped
sampling for payments/booking_updates/reviews.

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
    deduplicate_payment_records,
    filter_to_sampled_bookings,
    payment_business_key,
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


def test_seed_file_name_is_stable_for_an_unchanged_seed_limit() -> None:
    # A rerun with the same seed_limit must resolve to the same path so Auto
    # Loader's default cloudFiles.allowOverwrites=false recognizes it as
    # already-ingested and skips reprocessing it.
    assert seed_file_name(25000) == seed_file_name(25000)


def test_seed_file_name_changes_when_seed_limit_changes() -> None:
    # A deliberate configuration change must still be picked up as new
    # content by Auto Loader, so the path must differ.
    assert seed_file_name(25000) != seed_file_name(30000)

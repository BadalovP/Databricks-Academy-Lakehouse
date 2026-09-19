"""
LAB 08 - TravelOps

Test module:
tests/test_reconciliation.py

Purpose:
Checks the payment reconciliation helper used to explain Gold-layer health
conditions and SQL alert semantics.

Inputs:
Primitive numeric examples.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are side-effect free.
"""

from travelops.reconciliation import health_passes, reconciliation_status


def test_reconciliation_matched_underpaid_overpaid() -> None:
    assert reconciliation_status("100.00", "100.00", has_payment_record=True) == "matched"
    assert reconciliation_status("100.00", "90.00", has_payment_record=True) == "underpaid"
    assert reconciliation_status("100.00", "110.00", has_payment_record=True) == "overpaid"


def test_reconciliation_distinguishes_no_payment_from_pending_or_failed() -> None:
    # No payment row exists at all for the booking.
    assert reconciliation_status("100.00", "0", has_payment_record=False) == "no_payment_record"
    # A payment row exists (e.g. pending/failed/refunded) but nothing completed.
    assert (
        reconciliation_status("100.00", "0", has_payment_record=True) == "pending_or_failed_payment"
    )


def test_health_gate_requires_rows_no_duplicates_no_invalid_amounts_no_mismatches() -> None:
    assert health_passes(
        row_count=10,
        duplicate_current_booking_count=0,
        invalid_booking_amount_count=0,
        payment_amount_mismatch_count=0,
    )
    assert not health_passes(
        row_count=0,
        duplicate_current_booking_count=0,
        invalid_booking_amount_count=0,
        payment_amount_mismatch_count=0,
    )
    assert not health_passes(
        row_count=10,
        duplicate_current_booking_count=1,
        invalid_booking_amount_count=0,
        payment_amount_mismatch_count=0,
    )
    assert not health_passes(
        row_count=10,
        duplicate_current_booking_count=0,
        invalid_booking_amount_count=1,
        payment_amount_mismatch_count=0,
    )
    assert not health_passes(
        row_count=10,
        duplicate_current_booking_count=0,
        invalid_booking_amount_count=0,
        payment_amount_mismatch_count=1,
    )


def test_health_gate_does_not_require_every_booking_to_have_completed_payment() -> None:
    # A large number of bookings with no payment yet, or only a pending/failed
    # payment, must not fail the gate on its own: only duplicate bookings,
    # invalid amounts and completed-payment amount mismatches do.
    assert health_passes(
        row_count=25000,
        duplicate_current_booking_count=0,
        invalid_booking_amount_count=0,
        payment_amount_mismatch_count=0,
    )

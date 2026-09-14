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


def test_reconciliation_statuses() -> None:
    assert reconciliation_status("100.00", "100.00") == "matched"
    assert reconciliation_status("100.00", "0") == "missing_payment"
    assert reconciliation_status("100.00", "90.00") == "underpaid"
    assert reconciliation_status("100.00", "110.00") == "overpaid"


def test_health_gate_requires_rows_no_duplicates_and_no_mismatches() -> None:
    assert health_passes(row_count=10, duplicate_current_booking_count=0, mismatch_count=0)
    assert not health_passes(row_count=0, duplicate_current_booking_count=0, mismatch_count=0)
    assert not health_passes(row_count=10, duplicate_current_booking_count=1, mismatch_count=0)
    assert not health_passes(row_count=10, duplicate_current_booking_count=0, mismatch_count=1)

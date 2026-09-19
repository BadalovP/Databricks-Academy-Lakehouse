"""
LAB 08 - TravelOps

Module:
src/travelops/reconciliation.py

Purpose:
Implements local reconciliation helpers for comparing booking value to completed
payment value.

Responsibilities:
- classify reconciliation status consistently
- make alert threshold behavior unit-testable outside Databricks

Inputs:
Booking totals, completed payment totals, and payment record presence as
numeric/boolean values.

Outputs:
Stable status labels used by tests, documentation and Gold health logic.

Idempotency:
Functions are deterministic and side-effect free.

Environment behavior:
Thresholds are identical across DEV and PROD so promotion tests prove the same
application logic is deployed.
"""

from __future__ import annotations

from decimal import Decimal


def reconciliation_status(
    booking_amount: object,
    completed_paid_amount: object,
    has_payment_record: bool,
    tolerance: str = "0.01",
) -> str:
    """Classify payment coverage for a booking.

    `has_payment_record` distinguishes a booking with no payment row at all
    ("no_payment_record") from one whose payment row(s) exist but none has
    status "completed" ("pending_or_failed_payment") — both previously
    collapsed into a single "missing_payment" bucket that made it impossible
    to tell a legitimate in-progress booking from one with no payment
    activity whatsoever. Both remain informational, non-failing states: only
    "underpaid"/"overpaid" (a completed payment for the wrong amount)
    represents a data-integrity defect.
    """

    booking = Decimal(str(booking_amount or 0))
    paid = Decimal(str(completed_paid_amount or 0))
    allowed_delta = Decimal(tolerance)

    if paid == 0:
        return "pending_or_failed_payment" if has_payment_record else "no_payment_record"
    if abs(booking - paid) <= allowed_delta:
        return "matched"
    if paid < booking:
        return "underpaid"
    return "overpaid"


def health_passes(
    row_count: int,
    duplicate_current_booking_count: int,
    invalid_booking_amount_count: int,
    payment_amount_mismatch_count: int,
) -> bool:
    """Return true when the production health gate should pass.

    Deliberately does not take a "missing payment" count: a booking with no
    payment yet, or only a pending/failed payment, is a normal booking
    lifecycle state and must not fail the gate on its own. Only a completed
    payment for the wrong amount (`payment_amount_mismatch_count`) and
    negative booking amounts (`invalid_booking_amount_count`) are treated as
    unconditional defects, alongside the pre-existing row-count and
    duplicate-booking checks.
    """

    return (
        row_count > 0
        and duplicate_current_booking_count == 0
        and invalid_booking_amount_count == 0
        and payment_amount_mismatch_count == 0
    )

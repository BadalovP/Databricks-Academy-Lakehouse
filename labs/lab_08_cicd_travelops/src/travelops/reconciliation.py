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
Booking totals and completed payment totals as numeric values.

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
    booking_amount: object, paid_amount: object, tolerance: str = "0.01"
) -> str:
    """Classify payment coverage for a booking."""

    booking = Decimal(str(booking_amount or 0))
    paid = Decimal(str(paid_amount or 0))
    allowed_delta = Decimal(tolerance)

    if paid == 0:
        return "missing_payment"
    if abs(booking - paid) <= allowed_delta:
        return "matched"
    if paid < booking:
        return "underpaid"
    return "overpaid"


def health_passes(
    row_count: int, duplicate_current_booking_count: int, mismatch_count: int
) -> bool:
    """Return true when the production health gate should pass."""

    return row_count > 0 and duplicate_current_booking_count == 0 and mismatch_count == 0

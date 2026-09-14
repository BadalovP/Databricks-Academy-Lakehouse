"""
LAB 08 - TravelOps

Module:
src/travelops/quality_rules.py

Purpose:
Defines reusable data-quality rules for booking, payment, property and review
records before they are promoted from Bronze to Silver.

Responsibilities:
- keep rule names stable for tests and pipeline expectations
- provide plain-Python evaluators for local unit tests
- mirror the expressions used by Lakeflow expectation decorators

Inputs:
Dictionary-like row objects in local tests; Spark columns in pipeline files.

Outputs:
Boolean pass/fail decisions and expectation expression dictionaries.

Idempotency:
Rules are deterministic and contain no external state.

Environment behavior:
Rules do not vary by environment. DEV and PROD receive the same data contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Any

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


def has_valid_booking_dates(row: Mapping[str, Any]) -> bool:
    """Return true when a booking row has a strictly positive stay window."""

    check_in = row.get("check_in")
    check_out = row.get("check_out")
    if not isinstance(check_in, date) or not isinstance(check_out, date):
        return False
    return check_out > check_in


def is_positive_money(value: object) -> bool:
    """Return true when a numeric money value is non-negative."""

    if value is None:
        return False
    return Decimal(str(value)) >= Decimal("0")

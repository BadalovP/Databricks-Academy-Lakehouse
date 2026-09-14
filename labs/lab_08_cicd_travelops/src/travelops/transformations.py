"""
LAB 08 - TravelOps

Module:
src/travelops/transformations.py

Purpose:
Contains testable transformation helpers that express key TravelOps business
semantics without requiring a Spark runtime.

Responsibilities:
- normalize status values
- calculate booking stay nights
- classify booking lifecycle rows for dashboard use

Inputs:
Primitive Python values and dictionary-like records.

Outputs:
Normalized values used by unit tests and mirrored in Spark pipeline code.

Idempotency:
All helpers are deterministic and safe to rerun.

Environment behavior:
Logic is environment-agnostic; deployment targets only change data locations.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

FINAL_BOOKING_STATUSES = {"cancelled", "completed"}


def normalize_status(value: object) -> str:
    """Normalize nullable and mixed-case status labels."""

    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    return normalized or "unknown"


def stay_nights(row: Mapping[str, Any]) -> int:
    """Calculate the number of booked nights from check-in and check-out dates."""

    check_in = row["check_in"]
    check_out = row["check_out"]
    if not isinstance(check_in, date) or not isinstance(check_out, date):
        raise TypeError("check_in and check_out must be date values")
    return (check_out - check_in).days


def is_final_status(status: object) -> bool:
    """Return whether a booking status is terminal for operational reporting."""

    return normalize_status(status) in FINAL_BOOKING_STATUSES

"""
LAB 08 - TravelOps

Test module:
tests/test_transformations.py

Purpose:
Validates pure transformation helpers that mirror the Spark transformations used
in the TravelOps Silver and Gold datasets.

Inputs:
Small in-memory Python values.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests perform no writes and can run repeatedly in CI.
"""

from datetime import date

import pytest

from travelops.transformations import is_final_status, normalize_status, stay_nights


def test_normalize_status_handles_case_and_blanks() -> None:
    assert normalize_status(" Confirmed ") == "confirmed"
    assert normalize_status("") == "unknown"


def test_stay_nights_calculates_positive_window() -> None:
    assert stay_nights({"check_in": date(2026, 2, 1), "check_out": date(2026, 2, 4)}) == 3


def test_stay_nights_rejects_non_dates() -> None:
    with pytest.raises(TypeError):
        stay_nights({"check_in": "2026-02-01", "check_out": "2026-02-04"})


def test_terminal_booking_statuses_are_explicit() -> None:
    assert is_final_status("completed")
    assert not is_final_status("confirmed")

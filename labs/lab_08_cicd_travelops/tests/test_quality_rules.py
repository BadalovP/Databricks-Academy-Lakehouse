"""
LAB 08 - TravelOps

Test module:
tests/test_quality_rules.py

Purpose:
Verifies local quality-rule helpers before the same rules are deployed through
Lakeflow expectations.

Inputs:
Small in-memory Python records.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are side-effect free and safe to run repeatedly in PR and main workflows.
"""

from datetime import date

from travelops.quality_rules import has_valid_booking_dates, is_positive_money


def test_booking_dates_must_have_positive_stay_window() -> None:
    row = {"check_in": date(2026, 1, 5), "check_out": date(2026, 1, 9)}

    assert has_valid_booking_dates(row)


def test_booking_dates_reject_same_day_checkout() -> None:
    row = {"check_in": date(2026, 1, 5), "check_out": date(2026, 1, 5)}

    assert not has_valid_booking_dates(row)


def test_money_values_must_be_non_negative() -> None:
    assert is_positive_money("12.34")
    assert not is_positive_money("-0.01")

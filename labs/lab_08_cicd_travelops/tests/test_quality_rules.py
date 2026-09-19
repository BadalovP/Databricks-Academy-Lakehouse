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

from travelops.quality_rules import has_valid_booking_dates, is_positive_money, is_valid_booking_row


def test_booking_dates_must_have_positive_stay_window() -> None:
    row = {"check_in": date(2026, 1, 5), "check_out": date(2026, 1, 9)}

    assert has_valid_booking_dates(row)


def test_booking_dates_reject_same_day_checkout() -> None:
    row = {"check_in": date(2026, 1, 5), "check_out": date(2026, 1, 5)}

    assert not has_valid_booking_dates(row)


def test_money_values_must_be_non_negative() -> None:
    assert is_positive_money("12.34")
    assert not is_positive_money("-0.01")


def _valid_booking_row(**overrides: object) -> dict:
    row = {
        "booking_id": 1,
        "check_in": date(2026, 1, 5),
        "check_out": date(2026, 1, 9),
        "guests_count": 2,
        "total_amount": "305.74",
        "status": "confirmed",
    }
    row.update(overrides)
    return row


def test_valid_booking_row_passes() -> None:
    assert is_valid_booking_row(_valid_booking_row())


def test_booking_row_with_negative_amount_is_rejected() -> None:
    # This mirrors booking_updates_bronze rows that previously bypassed
    # BOOKING_EXPECTATIONS entirely and reached current_bookings_silver with
    # a negative booking_amount (e.g. -12.56).
    assert not is_valid_booking_row(_valid_booking_row(total_amount="-12.56"))


def test_booking_row_with_invalid_guest_count_is_rejected() -> None:
    assert not is_valid_booking_row(_valid_booking_row(guests_count=0))
    assert not is_valid_booking_row(_valid_booking_row(guests_count=21))


def test_booking_row_with_invalid_status_is_rejected() -> None:
    assert not is_valid_booking_row(_valid_booking_row(status="disputed"))


def test_booking_row_with_invalid_dates_is_rejected() -> None:
    assert not is_valid_booking_row(
        _valid_booking_row(check_in=date(2026, 1, 5), check_out=date(2026, 1, 5))
    )


def test_booking_row_missing_booking_id_is_rejected() -> None:
    assert not is_valid_booking_row(_valid_booking_row(booking_id=None))

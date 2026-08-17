"""
Lab 05 — unit tests for reusable Lakeflow expectation definitions.

These tests are intentionally pure pytest:
- no Spark session
- no Lakeflow pipeline
- no Databricks runtime dependency

They validate the reusable expectation configuration defined in:
    labs/lab_05_lakeflow/src/quality_rules.py

Run from the repository root with:

    python -m pytest -v \
      labs/lab_05_lakeflow/tests/test_quality_rules.py

The same rules will later be imported by pipeline/silver.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Make the Lab 05 project root importable when pytest is started from the
# repository root or directly from this tests directory.
# ---------------------------------------------------------------------------

LAB05_ROOT = Path(__file__).resolve().parents[1]

if str(LAB05_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB05_ROOT))


from src.quality_rules import (  # noqa: E402
    ALL_EXPECTATIONS,
    ALL_INFORMATION_EXPECTATIONS,
    ALL_STATUS_EXPECTATIONS,
    INFORMATION_DROP_EXPECTATIONS,
    INFORMATION_MONITOR_EXPECTATIONS,
    STATUS_DROP_EXPECTATIONS,
    STATUS_FAIL_EXPECTATIONS,
    STATUS_MONITOR_EXPECTATIONS,
    validate_expectation_dictionary,
)


def test_status_expectation_groups_are_not_empty() -> None:
    """Both status rule groups should contain production rules."""
    assert STATUS_MONITOR_EXPECTATIONS
    assert STATUS_DROP_EXPECTATIONS


def test_information_expectation_groups_are_not_empty() -> None:
    """Both station-information rule groups should contain production rules."""
    assert INFORMATION_MONITOR_EXPECTATIONS
    assert INFORMATION_DROP_EXPECTATIONS


def test_all_expectation_dictionaries_are_structurally_valid() -> None:
    """Every production expectation dictionary should pass shared validation."""
    dictionaries = [
        STATUS_MONITOR_EXPECTATIONS,
        STATUS_DROP_EXPECTATIONS,
        INFORMATION_MONITOR_EXPECTATIONS,
        INFORMATION_DROP_EXPECTATIONS,
        STATUS_FAIL_EXPECTATIONS,
    ]

    for expectations in dictionaries:
        validate_expectation_dictionary(expectations)


def test_expectation_names_are_unique_across_production_rules() -> None:
    """Production rule names should not silently overwrite one another."""
    rule_names = (
        list(STATUS_MONITOR_EXPECTATIONS)
        + list(STATUS_DROP_EXPECTATIONS)
        + list(INFORMATION_MONITOR_EXPECTATIONS)
        + list(INFORMATION_DROP_EXPECTATIONS)
    )

    assert len(rule_names) == len(set(rule_names))


def test_status_rules_include_required_join_key_rule() -> None:
    """
    station_id is the natural key used to join status with station metadata,
    so Silver status quality rules must explicitly protect it.
    """
    assert "status_station_id_present" in STATUS_DROP_EXPECTATIONS
    assert (
        STATUS_DROP_EXPECTATIONS["status_station_id_present"]
        == "station_id IS NOT NULL"
    )


def test_information_rules_include_required_join_key_rule() -> None:
    """The reference source must also protect station_id."""
    assert (
        "information_station_id_present"
        in INFORMATION_DROP_EXPECTATIONS
    )
    assert (
        INFORMATION_DROP_EXPECTATIONS[
            "information_station_id_present"
        ]
        == "station_id IS NOT NULL"
    )


def test_negative_operational_counts_are_rejected() -> None:
    """Status Silver rules must protect bike and dock counts."""
    bikes_rule = STATUS_DROP_EXPECTATIONS[
        "status_bikes_non_negative"
    ]
    docks_rule = STATUS_DROP_EXPECTATIONS[
        "status_docks_non_negative"
    ]

    assert "num_bikes_available" in bikes_rule
    assert ">= 0" in bikes_rule

    assert "num_docks_available" in docks_rule
    assert ">= 0" in docks_rule


def test_station_information_has_geographic_validation() -> None:
    """Reference data should enforce valid latitude/longitude ranges."""
    latitude_rule = INFORMATION_DROP_EXPECTATIONS[
        "information_latitude_valid"
    ]
    longitude_rule = INFORMATION_DROP_EXPECTATIONS[
        "information_longitude_valid"
    ]

    assert "-90" in latitude_rule
    assert "90" in latitude_rule

    assert "-180" in longitude_rule
    assert "180" in longitude_rule


def test_capacity_rule_does_not_allow_negative_values() -> None:
    """
    Capacity may be null and monitored separately, but a negative capacity
    must not pass the production rule.
    """
    capacity_rule = INFORMATION_DROP_EXPECTATIONS[
        "information_capacity_non_negative"
    ]

    assert "capacity" in capacity_rule
    assert ">= 0" in capacity_rule


def test_combined_expectation_views_are_consistent() -> None:
    """Convenience dictionaries should contain all production rules."""
    expected_status_count = (
        len(STATUS_MONITOR_EXPECTATIONS)
        + len(STATUS_DROP_EXPECTATIONS)
    )

    expected_information_count = (
        len(INFORMATION_MONITOR_EXPECTATIONS)
        + len(INFORMATION_DROP_EXPECTATIONS)
    )

    assert len(ALL_STATUS_EXPECTATIONS) == expected_status_count
    assert (
        len(ALL_INFORMATION_EXPECTATIONS)
        == expected_information_count
    )

    assert len(ALL_EXPECTATIONS) == (
        expected_status_count
        + expected_information_count
    )


@pytest.mark.parametrize(
    "invalid_expectations",
    [
        {},
        {"": "station_id IS NOT NULL"},
        {"rule_with_empty_expression": ""},
        {"rule_with_whitespace_expression": "   "},
    ],
)
def test_validator_rejects_invalid_expectation_configuration(
    invalid_expectations: dict[str, str],
) -> None:
    """Malformed rule dictionaries should fail before pipeline execution."""
    with pytest.raises(ValueError):
        validate_expectation_dictionary(
            invalid_expectations
        )

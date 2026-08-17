"""
Lab 05 — reusable Lakeflow expectation definitions.

IMPORTANT:
Expectation expressions are evaluated against the OUTPUT columns of the
Silver dataset functions.

Therefore the station_information rules use the Silver column names:
- station_name
- latitude
- longitude
- capacity
- station_id

rather than the raw Bronze nested-field names:
- name
- lat
- lon
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# station_status expectations
# ---------------------------------------------------------------------------

STATUS_MONITOR_EXPECTATIONS = {
    "status_last_reported_present":
        "last_reported IS NOT NULL",

    "status_renting_flag_present":
        "is_renting IS NOT NULL",

    "status_returning_flag_present":
        "is_returning IS NOT NULL",
}


STATUS_DROP_EXPECTATIONS = {
    "status_station_id_present":
        "station_id IS NOT NULL",

    "status_bikes_non_negative":
        "num_bikes_available IS NULL "
        "OR num_bikes_available >= 0",

    "status_docks_non_negative":
        "num_docks_available IS NULL "
        "OR num_docks_available >= 0",
}


# ---------------------------------------------------------------------------
# station_information expectations
#
# These expressions MUST use the output column names produced by
# station_information_silver().
# ---------------------------------------------------------------------------

INFORMATION_MONITOR_EXPECTATIONS = {
    "information_name_present":
        "station_name IS NOT NULL",

    "information_capacity_present":
        "capacity IS NOT NULL",
}


INFORMATION_DROP_EXPECTATIONS = {
    "information_station_id_present":
        "station_id IS NOT NULL",

    "information_latitude_valid":
        "latitude IS NULL "
        "OR (latitude >= -90 AND latitude <= 90)",

    "information_longitude_valid":
        "longitude IS NULL "
        "OR (longitude >= -180 AND longitude <= 180)",

    "information_capacity_non_negative":
        "capacity IS NULL OR capacity >= 0",
}


# ---------------------------------------------------------------------------
# Controlled strict-validation rule
# ---------------------------------------------------------------------------

STATUS_FAIL_EXPECTATIONS = {
    "status_station_id_required_strict":
        "station_id IS NOT NULL",
}


# ---------------------------------------------------------------------------
# Convenience collections used by tests / diagnostics
# ---------------------------------------------------------------------------

ALL_STATUS_EXPECTATIONS = {
    **STATUS_MONITOR_EXPECTATIONS,
    **STATUS_DROP_EXPECTATIONS,
}

ALL_INFORMATION_EXPECTATIONS = {
    **INFORMATION_MONITOR_EXPECTATIONS,
    **INFORMATION_DROP_EXPECTATIONS,
}

ALL_EXPECTATIONS = {
    **ALL_STATUS_EXPECTATIONS,
    **ALL_INFORMATION_EXPECTATIONS,
}


def validate_expectation_dictionary(
    expectations: dict[str, str],
) -> None:
    """Lightweight structural validation for an expectation dictionary."""
    if not expectations:
        raise ValueError(
            "Expectation dictionary must not be empty."
        )

    for rule_name, expression in expectations.items():
        if not isinstance(rule_name, str) or not rule_name.strip():
            raise ValueError(
                "Expectation names must be non-empty strings."
            )

        if not isinstance(expression, str) or not expression.strip():
            raise ValueError(
                f"Expectation {rule_name!r} "
                "must have a non-empty SQL expression."
            )

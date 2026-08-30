from demo2.governance import allowed_countries_for_user, filter_rows_for_user
from demo2.reconciliation import reconciliation_result
from demo2.transformations import scd2_version_for_date


def test_temporal_join_half_open_boundaries():
    versions = [
        {"version": "old", "__START_AT": 20260801, "__END_AT": 20260830},
        {"version": "new", "__START_AT": 20260830, "__END_AT": None},
    ]
    assert scd2_version_for_date(20260801, versions)["version"] == "old"
    assert scd2_version_for_date(20260830, versions)["version"] == "new"


def test_reconciliation_checks_all_required_invariants():
    result = reconciliation_result(
        bronze_count=100,
        valid_count=92,
        warn_count=2,
        quarantine_count=6,
        fact_count=94,
    )
    assert result["trusted_count"] == 94
    assert result["passed"]


def test_governance_is_fail_closed():
    mappings = [
        {"user_name": "admin@example.com", "country": None, "all_access": True},
        {"user_name": "pl@example.com", "country": "PL", "all_access": False},
    ]
    rows = [{"country": "PL"}, {"country": "DE"}]
    assert allowed_countries_for_user("missing@example.com", mappings) == set()
    assert filter_rows_for_user(rows, "missing@example.com", mappings) == []
    assert filter_rows_for_user(rows, "pl@example.com", mappings) == [{"country": "PL"}]
    assert filter_rows_for_user(rows, "admin@example.com", mappings) == rows

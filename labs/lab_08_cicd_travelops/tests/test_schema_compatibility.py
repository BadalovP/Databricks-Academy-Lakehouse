"""
LAB 08 - TravelOps

Test module:
tests/test_schema_compatibility.py

Purpose:
Regression checks that sql/ and dashboards/ assets reference only columns and
reconciliation_status category values that pipeline/gold.py actually produces.
These exist because sql/production_health.sql and
dashboards/travelops_operations.lvdash.json previously kept referencing
payment_mismatch_count and the old missing_payment category after
pipeline/gold.py's gold_production_health/gold_payment_reconciliation schema
changed, which pytest and bundle validate did not catch.

Inputs:
The actual sql/production_health.sql and
dashboards/travelops_operations.lvdash.json files, plus the
GOLD_PRODUCTION_HEALTH_COLUMNS / RECONCILIATION_STATUSES contract constants
in src/travelops/reconciliation.py.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are read-only and side-effect free.

Local unit tests vs Databricks integration:
These are text/JSON parsing checks against fixed contract constants; they do
not execute pipeline/gold.py (no pyspark dependency in this environment), so
they only catch drift between the contract constants and the SQL/dashboard
files, not drift between the contract constants and pipeline/gold.py itself.
Keeping GOLD_PRODUCTION_HEALTH_COLUMNS and RECONCILIATION_STATUSES in
src/travelops/reconciliation.py in sync with pipeline/gold.py remains a
manual discipline, matching this repo's existing pattern for
BOOKING_EXPECTATIONS/PAYMENT_EXPECTATIONS, which are duplicated between
pipeline/silver.py and src/travelops/quality_rules.py for the same reason
(pipeline/*.py cannot reliably import the travelops package at Lakeflow
runtime). Only an actual Databricks bundle run and dashboard/SQL execution
can prove pipeline/gold.py, the contract constants and these assets all
agree.
"""

import json
import re
from pathlib import Path

from travelops.reconciliation import GOLD_PRODUCTION_HEALTH_COLUMNS, RECONCILIATION_STATUSES

LAB_ROOT = Path(__file__).resolve().parent.parent


def _select_columns(sql_text: str) -> list[str]:
    match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_text, re.DOTALL | re.IGNORECASE)
    assert match, "Could not find a SELECT ... FROM clause in the SQL file"
    columns = [line.strip().rstrip(",") for line in match.group(1).splitlines()]
    return [c for c in columns if c]


def test_production_health_sql_only_selects_existing_columns() -> None:
    sql_text = (LAB_ROOT / "sql" / "production_health.sql").read_text()

    columns = _select_columns(sql_text)

    assert columns, "Expected sql/production_health.sql to select at least one column"
    unknown = [c for c in columns if c not in GOLD_PRODUCTION_HEALTH_COLUMNS]
    assert not unknown, (
        f"sql/production_health.sql references columns not in "
        f"GOLD_PRODUCTION_HEALTH_COLUMNS: {unknown}"
    )


def _dashboard() -> dict:
    dashboard_path = LAB_ROOT / "dashboards" / "travelops_operations.lvdash.json"
    return json.loads(dashboard_path.read_text())


def _find_widget(dashboard: dict, widget_name: str) -> dict:
    for page in dashboard["pages"]:
        for item in page.get("layout", []):
            widget = item["widget"]
            if widget["name"] == widget_name:
                return widget
    raise AssertionError(f"Could not find widget '{widget_name}' in the dashboard")


def test_dashboard_reconciliation_colors_match_known_statuses() -> None:
    widget = _find_widget(_dashboard(), "reconciliation-bars")

    mappings = widget["spec"]["encodings"]["color"]["scale"]["mappings"]
    mapped_statuses = {m["value"] for m in mappings}

    assert mapped_statuses == set(RECONCILIATION_STATUSES), (
        f"reconciliation-bars color mapping {mapped_statuses} does not match "
        f"RECONCILIATION_STATUSES {set(RECONCILIATION_STATUSES)}"
    )


def test_dashboard_payment_gap_kpi_is_informational_only() -> None:
    widget = _find_widget(_dashboard(), "kpi-payment-gaps")

    filter_expression = widget["queries"][0]["query"]["filters"][0]["expression"]

    # The gap KPI must count only the two informational, non-gating statuses,
    # not the amount-mismatch statuses that gate gold_production_health.
    assert "no_payment_record" in filter_expression
    assert "pending_or_failed_payment" in filter_expression
    assert "underpaid" not in filter_expression
    assert "overpaid" not in filter_expression


def test_dashboard_payment_mismatch_kpi_covers_only_amount_defects() -> None:
    widget = _find_widget(_dashboard(), "kpi-payment-mismatches")

    filter_expression = widget["queries"][0]["query"]["filters"][0]["expression"]

    assert "underpaid" in filter_expression
    assert "overpaid" in filter_expression
    assert "no_payment_record" not in filter_expression
    assert "pending_or_failed_payment" not in filter_expression


def test_dashboard_does_not_reference_removed_missing_payment_status() -> None:
    dashboard_text = (LAB_ROOT / "dashboards" / "travelops_operations.lvdash.json").read_text()

    assert "missing_payment" not in dashboard_text

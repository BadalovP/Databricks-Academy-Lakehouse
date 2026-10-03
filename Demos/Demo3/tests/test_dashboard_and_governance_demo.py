"""The published dashboard definition and the isolated governance demonstration."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import build_dashboard  # noqa: E402
import run_governance_demo  # noqa: E402

DASHBOARD = PROJECT_ROOT / "dashboards" / "urbanflow.lvdash.json"
MONTH = "urbanflow-hist-202401-full-gh-37140618864-1"
MONTH_WEATHER = "urbanflow-weather-202401-full-gh-37140618864-1"


def _definition() -> dict:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def test_committed_dashboard_is_exactly_what_the_generator_builds() -> None:
    rendered = json.dumps(build_dashboard.build(), indent=2, ensure_ascii=False) + "\n"
    assert DASHBOARD.read_text(encoding="utf-8") == rendered


def test_execution_ids_come_from_the_full_month_evidence() -> None:
    assert build_dashboard.execution_ids() == {
        "historical_execution_id": MONTH,
        "weather_execution_id": MONTH_WEATHER,
    }


def test_every_dataset_is_bound_to_one_explicit_full_month_execution() -> None:
    for dataset in _definition()["datasets"]:
        sql = "".join(dataset["queryLines"])
        assert ":historical_execution_id" not in sql and ":weather_execution_id" not in sql
        assert "devsample" not in sql or "LIKE 'urbanflow-" in sql, dataset["name"]
        for table, execution in (
            ("silver_historical_trips", MONTH),
            ("quarantine_historical_trips", MONTH),
            ("gold_daily_trip_demand", MONTH),
            ("gold_weather_demand", MONTH_WEATHER),
        ):
            reads = len(re.findall(rf"\.{table}\b", sql))
            assert sql.count(f"execution_id = '{execution}'") >= reads, (dataset["name"], table)


def test_dashboard_has_the_four_pages_and_their_honesty_labels() -> None:
    pages = {page["displayName"]: json.dumps(page) for page in _definition()["pages"]}
    assert list(pages) == [
        "Current Operations",
        "January 2024 Historical Demand",
        "Weather & Demand",
        "Data Quality & Reliability",
    ]
    assert "POINT-IN-TIME GBFS SNAPSHOT" in pages["Current Operations"]
    assert "NOT A HISTORICAL AVAILABILITY TREND" in pages["Current Operations"]
    assert "REAL JANUARY 2024 CITI BIKE MONTHLY ARCHIVE" in pages["January 2024 Historical Demand"]
    assert "not station-level weather" in pages["Weather & Demand"]
    assert "association, not causation" in pages["Weather & Demand"]
    assert "NOT a data-quality failure" in pages["Data Quality & Reliability"]
    assert "DEVELOPMENT" in pages["Data Quality & Reliability"]
    # No time axis on the snapshot page.
    assert '"temporal"' not in pages["Current Operations"]


def test_every_widget_reads_a_defined_dataset() -> None:
    definition = _definition()
    names = {dataset["name"] for dataset in definition["datasets"]}
    used = {
        query["query"]["datasetName"]
        for page in definition["pages"]
        for item in page["layout"]
        for query in item["widget"].get("queries", [])
    }
    assert used == names


def test_governance_demo_only_ever_changes_its_own_isolated_schema() -> None:
    demo = run_governance_demo.DEMO
    assert demo == "dbr_dev.parvinbadalov_urbanflow_governance_demo"
    statements = (
        run_governance_demo.SETUP + run_governance_demo.APPLY + run_governance_demo.ROLLBACK
    )
    for sql in statements:
        targets = re.findall(r"(?:SCHEMA IF NOT EXISTS|TABLE|FUNCTION)\s+([\w.]+)", sql)
        assert targets, sql
        assert targets[0].startswith(demo), sql
    # The validated schema is only ever read.
    for sql in statements:
        for write in re.findall(r"(?:ALTER|CREATE OR REPLACE) TABLE\s+([\w.]+)", sql):
            assert not write.startswith("dbr_dev.parvinbadalov_urbanflow."), sql
    assert not any("GRANT" in sql for sql in statements)


def test_every_applied_policy_has_a_rollback() -> None:
    applied = {re.sub(r"\s+SET\b.*", "", sql) for sql in run_governance_demo.APPLY}
    rolled = {re.sub(r"\s+(DROP)\b.*", "", sql) for sql in run_governance_demo.ROLLBACK}
    assert applied == rolled

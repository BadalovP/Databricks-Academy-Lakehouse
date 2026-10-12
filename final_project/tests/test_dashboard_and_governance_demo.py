"""The per-target dashboard definitions and the isolated governance demonstration."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import build_dashboard  # noqa: E402
import run_governance_demo  # noqa: E402

from urbanflow.landing import SAMPLE_HISTORICAL_ID, SAMPLE_WEATHER_ID  # noqa: E402

TARGETS = {
    "personal_dev": ("workspace", "urbanflow_final_dev", "urbanflow_final_lakeflow_dev"),
    "azure_prod": (
        "dbr_dev",
        "parvinbadalov_urbanflow_final_prod",
        "parvinbadalov_urbanflow_final_lakeflow_prod",
    ),
}
# Demo 3's validated schemas and pipeline must never appear in a Final Project dashboard.
DEMO3_NAMES = ("parvinbadalov_urbanflow.", "parvinbadalov_urbanflow_lakeflow.", "urbanflow_dev.")


def _definition(target: str) -> dict:
    return json.loads(build_dashboard.output_path(target).read_text(encoding="utf-8"))


def _sql(dataset: dict) -> str:
    return "".join(dataset["queryLines"])


def test_targets_come_from_the_bundle() -> None:
    assert {
        name: (v["catalog"], v["schema"], v["lakeflow_schema"])
        for name, v in build_dashboard.targets().items()
    } == TARGETS


@pytest.mark.parametrize("target", TARGETS)
def test_committed_dashboard_is_exactly_what_the_generator_builds(target: str) -> None:
    assert build_dashboard.output_path(target).read_text(
        encoding="utf-8"
    ) == build_dashboard.render(target)


def test_execution_ids_are_the_platform_job_defaults() -> None:
    assert build_dashboard.execution_ids() == {
        "historical_execution_id": SAMPLE_HISTORICAL_ID,
        "weather_execution_id": SAMPLE_WEATHER_ID,
    }


@pytest.mark.parametrize("target", TARGETS)
def test_every_dataset_reads_only_its_own_target(target: str) -> None:
    catalog, schema, lakeflow = TARGETS[target]
    others = [names for name, names in TARGETS.items() if name != target]
    for dataset in _definition(target)["datasets"]:
        sql = _sql(dataset)
        assert "${" not in sql, dataset["name"]
        references = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.]*\.[\w.]+)", sql)
        for reference in references:
            assert reference.startswith(
                (f"{catalog}.{schema}.", f"{catalog}.{lakeflow}.")
            ), reference
        for name in DEMO3_NAMES:
            assert f".{name}" not in sql and not sql.startswith(name), (dataset["name"], name)
        for _, other_schema, other_lakeflow in others:
            assert f".{other_schema}." not in sql and f".{other_lakeflow}." not in sql


@pytest.mark.parametrize("target", TARGETS)
def test_every_dataset_is_bound_to_one_explicit_execution(target: str) -> None:
    for dataset in _definition(target)["datasets"]:
        sql = _sql(dataset)
        assert ":historical_execution_id" not in sql and ":weather_execution_id" not in sql
        for table, execution in (
            ("silver_historical_trips", SAMPLE_HISTORICAL_ID),
            ("quarantine_historical_trips", SAMPLE_HISTORICAL_ID),
            ("gold_daily_trip_demand", SAMPLE_HISTORICAL_ID),
            ("gold_weather_demand", SAMPLE_WEATHER_ID),
        ):
            reads = len(re.findall(rf"\.{table}\b", sql))
            assert sql.count(f"execution_id = '{execution}'") >= reads, (dataset["name"], table)


def test_the_sample_executions_are_labelled_as_development_samples() -> None:
    sql = "".join(_sql(d) for d in _definition("personal_dev")["datasets"])
    assert f"LIKE '{SAMPLE_HISTORICAL_ID}%'" in sql
    assert f"LIKE '{SAMPLE_WEATHER_ID}%'" in sql


def test_dashboard_has_five_pages_and_their_honesty_labels() -> None:
    pages = {page["displayName"]: json.dumps(page) for page in _definition("azure_prod")["pages"]}
    assert list(pages) == [
        "Current Operations",
        "Historical Demand",
        "Weather & Demand",
        "Data Quality & Reliability",
        "Maintenance & Assistant",
    ]
    assert "POINT-IN-TIME GBFS SNAPSHOT" in pages["Current Operations"]
    assert "NOT A HISTORICAL AVAILABILITY TREND" in pages["Current Operations"]
    assert "40-ROW DEVELOPMENT SAMPLE" in pages["Historical Demand"]
    assert "not station-level weather" in pages["Weather & Demand"]
    assert "association, not causation" in pages["Weather & Demand"]
    assert "NOT a data-quality failure" in pages["Data Quality & Reliability"]
    assert "FICTIONAL, SYNTHETIC DATA" in pages["Maintenance & Assistant"]
    assert "live" in pages["Maintenance & Assistant"]
    assert '"temporal"' not in pages["Current Operations"]


def test_no_demo3_run_or_live_figure_is_presented() -> None:
    text = "".join(build_dashboard.output_path(t).read_text(encoding="utf-8") for t in TARGETS)
    for demo3 in ("96337578882467", "fb8a0b8a", "ba6710ed", "657", "374 rides", "3.43%",
                  "urbanflow-hist-202401-full", "urbanflow-20260929T195132Z"):  # fmt: skip
        assert demo3 not in text, demo3


@pytest.mark.parametrize("target", TARGETS)
def test_every_widget_reads_a_defined_dataset(target: str) -> None:
    definition = _definition(target)
    names = {dataset["name"] for dataset in definition["datasets"]}
    used = {
        query["query"]["datasetName"]
        for page in definition["pages"]
        for item in page["layout"]
        for query in item["widget"].get("queries", [])
    }
    assert used == names


def test_the_bundle_deploys_each_targets_own_dashboard_file() -> None:
    resource = yaml.safe_load(
        (PROJECT_ROOT / "resources" / "urbanflow_final.dashboard.yml").read_text(encoding="utf-8")
    )["resources"]["dashboards"]["urbanflow_final_dashboard"]
    assert resource["file_path"] == "../dashboards/urbanflow_final.${bundle.target}.lvdash.json"
    assert not list((PROJECT_ROOT / "dashboards").glob("urbanflow.lvdash.json"))


def test_unbound_placeholders_and_unsafe_names_are_refused() -> None:
    ids = build_dashboard.execution_ids()
    names = build_dashboard.targets()["personal_dev"]
    with pytest.raises(ValueError, match="placeholder"):
        build_dashboard.bind("SELECT * FROM ${catalog}.${other}.t", ids, names)
    with pytest.raises(ValueError, match="identifier"):
        build_dashboard.bind("SELECT 1", ids, {**names, "schema": "x; DROP"})


# --- governance demonstration ------------------------------------------------------------


@pytest.mark.parametrize("target", TARGETS)
def test_governance_demo_only_ever_changes_its_own_isolated_schema(target: str) -> None:
    catalog, schema, _ = TARGETS[target]
    plan = run_governance_demo.statements(target)
    demo = f"{catalog}.{schema}_governance_demo"
    assert plan["demo"] == demo
    for sql in plan["setup"] + plan["apply"] + plan["rollback"] + [plan["cleanup"]]:
        targets = re.findall(r"(?:SCHEMA IF NOT EXISTS|SCHEMA|TABLE|FUNCTION)\s+([\w.]+)", sql)
        assert targets, sql
        assert targets[0].startswith(demo), sql
        for write in re.findall(r"(?:ALTER|CREATE OR REPLACE) TABLE\s+([\w.]+)", sql):
            assert write.startswith(f"{demo}."), sql
    assert not any("GRANT" in sql for sql in plan["setup"] + plan["apply"])
    reads = re.findall(r"FROM ([\w.]+)", " ".join(plan["setup"]))
    assert all(r.startswith((f"{catalog}.{schema}.", f"{demo}.")) for r in reads), reads


def test_governance_demo_copies_only_the_default_sample_execution() -> None:
    setup = " ".join(run_governance_demo.statements("personal_dev")["setup"])
    assert f"execution_id = '{SAMPLE_HISTORICAL_ID}'" in setup
    assert "parvinbadalov_urbanflow." not in setup and "3ed106620db591d9" not in setup


def test_every_applied_policy_has_a_rollback() -> None:
    plan = run_governance_demo.statements("azure_prod")
    applied = {re.sub(r"\s+SET\b.*", "", sql) for sql in plan["apply"]}
    rolled = {re.sub(r"\s+(DROP)\b.*", "", sql) for sql in plan["rollback"]}
    assert applied == rolled


def test_governance_demo_is_a_dry_run_unless_confirmed(capsys) -> None:
    assert run_governance_demo.main(["--target", "personal_dev"]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "DRY_RUN"
    with pytest.raises(SystemExit):
        run_governance_demo.main(["--target", "personal_dev", "--execute"])

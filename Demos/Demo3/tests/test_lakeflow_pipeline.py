"""Tests for the Lakeflow pipeline declarations.

These modules cannot be imported here: `pyspark.pipelines` only exists inside a running
pipeline, and `spark` arrives as an injected global. So the checks below are deliberately
source-level and configuration-level. That is a real limitation and it is stated plainly
rather than worked around by importing a stub, which would only prove the stub works.

The most valuable thing these tests enforce is the no-duplication rule. A Lakeflow pipeline
that recomputes a business rule slightly differently from the notebook path is the most
expensive failure available in a medallion project, because both sides look correct in
isolation. So the pipeline modules must delegate to `urbanflow.*` and must NOT contain the
classification logic themselves.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = PROJECT_ROOT / "pipeline"
BRONZE = PIPELINE_DIR / "bronze.py"
SILVER = PIPELINE_DIR / "silver.py"
GOLD = PIPELINE_DIR / "gold.py"
PIPELINE_RESOURCE = PROJECT_ROOT / "resources" / "pipelines.yml"
MODULES = (BRONZE, SILVER, GOLD)


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tree(path: Path) -> ast.Module:
    return ast.parse(_source(path))


def _expectations_text(path: Path) -> str:
    """The declared expectation dictionaries only, so a comment cannot satisfy an assertion."""
    chunks = []
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if any(name.endswith("_EXPECTATIONS") for name in targets):
                chunks.append(ast.unparse(node.value))
    assert chunks, f"{path.name} declares no expectations"
    return "\n".join(chunks)


def _decorated_functions(path: Path) -> dict[str, list[str]]:
    """Map each function to the decorator names applied to it."""
    functions: dict[str, list[str]] = {}
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.FunctionDef):
            names = []
            for decorator in node.decorator_list:
                call = decorator.func if isinstance(decorator, ast.Call) else decorator
                names.append(ast.unparse(call))
            functions[node.name] = names
    return functions


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_every_pipeline_module_parses(module: Path) -> None:
    """A syntax error would only surface when the billable pipeline starts."""
    assert _tree(module).body


# --- the no-duplicated-business-logic rule --------------------------------------


def test_silver_delegates_to_the_shared_split_instead_of_restating_the_rules() -> None:
    source = _source(SILVER)

    assert "from urbanflow.silver import split_silver_and_quarantine" in source
    assert "split_silver_and_quarantine(" in source


@pytest.mark.parametrize(
    "fragment",
    [
        "is_installed",
        "is_renting",
        "is_returning",
        'LOW_BIKES_AND_DOCKS", F',  # a reimplemented when/otherwise chain
        "row_number",
    ],
)
def test_silver_does_not_reimplement_the_classification(fragment: str) -> None:
    """The rule lives in urbanflow.silver. A second copy here is the drift this prevents."""
    code = "\n".join(
        line for line in _source(SILVER).splitlines() if not line.strip().startswith("#")
    )
    assert fragment not in code


def test_gold_delegates_to_the_shared_gold_functions() -> None:
    source = _source(GOLD)

    assert "from urbanflow.gold import" in source
    for function in (
        "station_dimension",
        "station_availability_fact",
        "daily_station_summary",
        "shortage_indicators",
        "rebalancing_priority",
    ):
        assert f"{function}(" in source


def test_gold_does_not_reimplement_the_priority_arithmetic() -> None:
    code = "\n".join(
        line for line in _source(GOLD).splitlines() if not line.strip().startswith("#")
    )
    # The score is computed in urbanflow.gold; only the expectation references the columns.
    assert "severity_points + deficit_points + size_points" in code  # the expectation
    assert "F.when(both_short" not in code
    assert "greatest(" not in code


# --- expectations ----------------------------------------------------------------


def test_silver_declares_expectations_on_all_three_tables() -> None:
    functions = _decorated_functions(SILVER)

    for table in (
        "silver_station_status",
        "quarantine_station_status",
        "duplicate_station_status",
    ):
        assert table in functions, f"{table} is not declared"
        assert "dp.table" in functions[table]
        assert "dp.expect_all" in functions[table]


def test_gold_declares_expectations_on_every_derived_table() -> None:
    functions = _decorated_functions(GOLD)

    for table in (
        "dim_station_development_sample",
        "fact_station_availability",
        "gold_daily_station_summary",
        "gold_station_shortage",
        "gold_rebalancing_priority",
    ):
        assert "dp.table" in functions[table]
        assert "dp.expect_all" in functions[table], f"{table} has no expectations"


def test_expectations_never_drop_or_fail_rows() -> None:
    """A dropping expectation on Silver would delete the rows quarantine exists to preserve."""
    for module in (SILVER, GOLD):
        source = _source(module)
        assert "expect_all_or_drop" not in source
        assert "expect_all_or_fail" not in source


def test_the_out_of_service_defect_is_asserted_declaratively() -> None:
    """The first live Phase 2 run produced 746 shortage rows where the rule yields 657."""
    assert "OUT_OF_SERVICE" in _expectations_text(SILVER)
    gold = _expectations_text(GOLD)
    assert gold.count("availability_status <> 'OUT_OF_SERVICE'") >= 2


def test_one_snapshot_cannot_claim_to_be_a_trend() -> None:
    """`is_trend_capable` must stay consistent with the observation count."""
    source = _source(GOLD)

    assert "trend_claim_matches_observations" in source
    assert "observations_per_station > 1 AND is_trend_capable = true" in source


# --- the bundle resource ---------------------------------------------------------


def _pipeline_definition() -> dict:
    resource = yaml.safe_load(PIPELINE_RESOURCE.read_text(encoding="utf-8"))
    return resource["resources"]["pipelines"]["urbanflow_pipeline"]


def test_the_pipeline_is_triggered_and_never_continuous() -> None:
    """A continuous pipeline bills for as long as it runs."""
    definition = _pipeline_definition()

    assert definition["continuous"] is False
    assert "schedule" not in definition


def test_the_pipeline_has_no_development_flag_so_production_mode_accepts_it() -> None:
    """A development pipeline is rejected outright by a production-mode target."""
    assert "development" not in _pipeline_definition()


def test_every_pipeline_module_is_registered_as_a_library() -> None:
    """An unregistered module is simply never executed, with no error to notice."""
    libraries = {entry["file"]["path"] for entry in _pipeline_definition()["libraries"]}

    for module in MODULES:
        assert f"../pipeline/{module.name}" in libraries


def test_thresholds_come_from_configuration_not_from_two_places() -> None:
    configuration = _pipeline_definition()["configuration"]

    assert configuration["urbanflow.low_bike_threshold"] == "2"
    assert configuration["urbanflow.low_dock_threshold"] == "2"
    for module in (SILVER, GOLD):
        assert "urbanflow.low_bike_threshold" in _source(module)


def test_the_pipeline_configuration_holds_a_secret_name_never_a_secret_value() -> None:
    """The connection string is read through a scope at run time, never stored here."""
    configuration = _pipeline_definition()["configuration"]

    assert configuration["urbanflow.event_hubs_secret_name"] == "parvinbadalov-eventhub-cs"
    rendered = yaml.safe_dump(configuration)
    for marker in ("SharedAccessKey", "EntityPath", "sb://", "Endpoint="):
        assert marker not in rendered
    assert "dbutils.secrets.get" in _source(BRONZE)


def test_the_station_reference_path_is_configuration_not_a_hardcoded_catalog() -> None:
    """A dev-target run must read dev-target storage."""
    path = _pipeline_definition()["configuration"]["urbanflow.station_information_path"]

    assert "${var.catalog}" in path
    assert "${var.schema}" in path
    assert "${var.volume}" in path


def test_pipeline_code_cannot_bypass_the_isolated_target_schema() -> None:
    """Every managed output is an unqualified dp table resolved in lakeflow_schema."""
    for module in MODULES:
        source = _source(module)
        assert "dbr_dev.parvinbadalov_urbanflow" not in source
        assert ".saveAsTable(" not in source
        assert ".writeStream" not in source
        assert "spark.sql(" not in source

        for node in ast.walk(_tree(module)):
            if not isinstance(node, ast.Call) or ast.unparse(node.func) != "dp.table":
                continue
            name = next(
                (
                    keyword.value.value
                    for keyword in node.keywords
                    if keyword.arg == "name" and isinstance(keyword.value, ast.Constant)
                ),
                None,
            )
            assert name and "." not in name, f"{module.name} bypasses the pipeline target: {name}"

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = PROJECT_ROOT / "notebooks"
BRONZE_CONSUMER = NOTEBOOK_DIR / "03_eventhubs_to_bronze.py"
SUPERSEDED_CONSUMER = NOTEBOOK_DIR / "03_streaming.py"
NOTEBOOKS = sorted(path.relative_to(PROJECT_ROOT) for path in NOTEBOOK_DIR.glob("*.py"))
REQUIRED_GUIDANCE = (
    "**What:**",
    "**Why:**",
    "**Input:**",
    "**Output:**",
    "**Key concepts:**",
    "**Expected result:**",
    "**How to explain it to my supervisor:**",
    "**Rerun and cost considerations:**",
)
# Teaching cells must stay reviewable. 30 non-blank lines is the enforced ceiling; the target
# while writing a new cell is 5-25 lines.
MAX_CODE_CELL_LINES = 30
NOT_EXECUTED_PHRASES = ("has not been run", "has not been executed", "No live messages")
VALIDATED_LIVE_CLAIMS = {
    Path("notebooks/03_eventhubs_to_bronze.py"): ("2026-09-29", "2,520", "PASS"),
    Path("notebooks/04_bronze_to_silver.py"): ("2026-10-02", "2,520", "repeat"),
    Path("notebooks/05_silver_to_gold.py"): ("2026-10-02", "657", "89"),
    Path("notebooks/06_historical_trips.py"): (
        "2026-10-02",
        "40-ROW DEVELOPMENT SAMPLE",
        "40 landed",
    ),
    Path("notebooks/07_weather_enrichment.py"): (
        "2026-10-02",
        "48-HOUR WEATHER SAMPLE",
        "40 trips",
    ),
}


def _cells(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# Databricks notebook source")
    return text.split("# COMMAND ----------")


def _content_lines(cell: str) -> list[str]:
    return [
        line.strip()
        for line in cell.splitlines()
        if line.strip() and line.strip() != "# Databricks notebook source"
    ]


def _is_markdown(cell: str) -> bool:
    content_lines = _content_lines(cell)
    return bool(content_lines) and content_lines[0] == "# MAGIC %md"


def _code_cells(path: Path) -> list[tuple[int, str]]:
    return [(index, cell) for index, cell in enumerate(_cells(path)) if not _is_markdown(cell)]


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_every_code_cell_has_complete_preceding_markdown(notebook: Path) -> None:
    cells = _cells(PROJECT_ROOT / notebook)
    code_indexes = [index for index, _ in _code_cells(PROJECT_ROOT / notebook)]

    assert code_indexes, f"{notebook} must contain code cells"
    undocumented: list[int] = []
    for index in code_indexes:
        assert index > 0, f"{notebook} begins with an undocumented code cell"
        guidance = cells[index - 1]
        if not _is_markdown(guidance):
            undocumented.append(index + 1)
            continue
        for heading in REQUIRED_GUIDANCE:
            assert heading in guidance, f"Code cell {index + 1} in {notebook} is missing {heading}"
    assert (
        not undocumented
    ), f"Code cells {undocumented} in {notebook} are not immediately preceded by a Markdown cell"


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_markdown_cells_are_never_outnumbered_by_code_cells(notebook: Path) -> None:
    """A mechanical restatement of the one-Markdown-cell-per-code-cell teaching contract."""
    cells = _cells(PROJECT_ROOT / notebook)
    markdown_cells = [cell for cell in cells if _is_markdown(cell)]
    code_cells = _code_cells(PROJECT_ROOT / notebook)
    assert len(markdown_cells) > len(
        code_cells
    ), f"{notebook} has {len(code_cells)} code cells but only {len(markdown_cells)} Markdown cells"


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_code_cells_stay_short_enough_to_review(notebook: Path) -> None:
    oversized = {
        index + 1: len(_content_lines(cell))
        for index, cell in _code_cells(PROJECT_ROOT / notebook)
        if len(_content_lines(cell)) > MAX_CODE_CELL_LINES
    }
    assert not oversized, f"Oversized code cells in {notebook} (cell: lines): {oversized}"


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_notebook_has_intro_and_teaching_summary(notebook: Path) -> None:
    text = (PROJECT_ROOT / notebook).read_text(encoding="utf-8")
    assert "**Business context:**" in text
    assert "**Prerequisites:**" in text
    assert "**Learning objectives:**" in text
    assert "**Academy labs:**" in text
    assert "## What we learned" in text
    assert "**Common errors:**" in text
    assert "**Troubleshooting:**" in text
    assert "**Review questions:**" in text
    assert "**Presentation summary:**" in text


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_actual_validation_claim_matches_the_notebook_evidence(notebook: Path) -> None:
    """Executed and unexecuted notebooks must both state their evidence plainly."""
    lines = (PROJECT_ROOT / notebook).read_text(encoding="utf-8").splitlines()
    claims = [line for line in lines if "**Actual validation:**" in line]
    assert claims, f"{notebook} must state what was actually validated"
    for claim in claims:
        if notebook in VALIDATED_LIVE_CLAIMS:
            for fact in VALIDATED_LIVE_CLAIMS[notebook]:
                assert fact in claim, f"{notebook} is missing live evidence fact {fact!r}: {claim}"
        else:
            assert any(
                phrase in claim for phrase in NOT_EXECUTED_PHRASES
            ), f"{notebook} must say plainly that no live Azure run happened: {claim}"


def test_bronze_consumer_bounds_the_streaming_wait() -> None:
    """An unbounded wait would hold shared academy compute with no cost ceiling."""
    text = BRONZE_CONSUMER.read_text(encoding="utf-8")
    assert "await_bounded_completion" in text
    assert "timeout_seconds=stream_timeout_seconds" in text
    assert 'dbutils.widgets.text("stream_timeout_seconds"' in text
    # Only code matters here: the Markdown deliberately names the unbounded call it replaced.
    code = "\n".join(cell for _, cell in _code_cells(BRONZE_CONSUMER))
    assert "awaitTermination" not in code


def test_bronze_consumer_always_stops_only_its_active_query() -> None:
    text = BRONZE_CONSUMER.read_text(encoding="utf-8")
    assert "try:" in text
    assert "finally:" in text
    assert "if query.isActive:" in text
    assert "query.stop()" in text
    assert "query_inactive_after_cleanup = not query.isActive" in text


def test_bronze_consumer_gates_on_the_approved_cluster_allowlist() -> None:
    """The never-terminate denylist must not be reused as an approved-compute allowlist."""
    text = BRONZE_CONSUMER.read_text(encoding="utf-8")
    assert "from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS" in text
    assert "if cluster_id not in APPROVED_RUN_CLUSTER_IDS:" in text
    assert "not in PROTECTED_SHARED_CLUSTER_IDS" not in text


def test_bronze_consumer_filters_execution_id_without_sql_interpolation() -> None:
    """Injection safety must hold at the point of use, not because an earlier cell validated."""
    text = BRONZE_CONSUMER.read_text(encoding="utf-8")
    assert 'F.col("execution_id") == execution_id' in text
    assert '.where(f"' not in text
    assert "execution_id = '" not in text


def test_bronze_consumer_takes_its_storage_target_from_parameters() -> None:
    """A dev-target run must write dev-target objects, not the azure target's table."""
    text = BRONZE_CONSUMER.read_text(encoding="utf-8")
    for widget in ("catalog", "schema", "volume"):
        assert f'dbutils.widgets.text("{widget}", "")' in text
    assert 'target_catalog = dbutils.widgets.get("catalog").strip() or config.azure.catalog' in text
    assert 'target_table = f"{target_catalog}.{target_schema}.bronze_station_status"' in text
    assert 'volume_root = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}"' in text
    assert "volume_root=volume_root" in text


def test_job_passes_bundle_target_variables_to_the_notebook() -> None:
    job = yaml.safe_load((PROJECT_ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    definition = job["resources"]["jobs"]["urbanflow_bounded_stream_test"]
    defaults = {parameter["name"]: parameter["default"] for parameter in definition["parameters"]}
    assert defaults["catalog"] == "${var.catalog}"
    assert defaults["schema"] == "${var.schema}"
    assert defaults["volume"] == "${var.volume}"
    task = definition["tasks"][0]["notebook_task"]
    assert task["notebook_path"] == "../notebooks/03_eventhubs_to_bronze.py"
    for name in ("catalog", "schema", "volume", "stream_timeout_seconds"):
        assert task["base_parameters"][name] == "{{job.parameters." + name + "}}"
    # The notebook's own wait bound must finish inside the task timeout so evidence is written.
    assert float(defaults["stream_timeout_seconds"]) < definition["timeout_seconds"]


def test_phase2_job_is_one_unscheduled_dry_run_dag_on_existing_compute() -> None:
    resource = yaml.safe_load((PROJECT_ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    job = resource["resources"]["jobs"]["urbanflow_silver_gold_test"]
    defaults = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}

    assert defaults["run_transform"] == "false"
    assert defaults["source_execution_id"] == "urbanflow-20260929T195132Z-r3"
    assert "schedule" not in job
    assert [task["task_key"] for task in job["tasks"]] == [
        "bronze_to_silver",
        "silver_to_gold",
    ]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "bronze_to_silver"}]
    for task in job["tasks"]:
        assert task["existing_cluster_id"] == "${var.compute_cluster_id}"
        assert "new_cluster" not in task
        parameters = task["notebook_task"]["base_parameters"]
        for name in ("run_transform", "source_execution_id", "catalog", "schema", "volume"):
            assert parameters[name] == "{{job.parameters." + name + "}}"


def test_superseded_notebook_is_fail_closed_and_labelled() -> None:
    """03_streaming.py shares the Bronze table and checkpoint, so it must refuse to run."""
    text = SUPERSEDED_CONSUMER.read_text(encoding="utf-8")
    assert "SUPERSEDED" in text
    assert "03_eventhubs_to_bronze.py" in text
    assert "checkpoint" in text
    first_code_cell = _code_cells(SUPERSEDED_CONSUMER)[0][1]
    assert 'dbutils.widgets.dropdown("acknowledge_superseded"' in first_code_cell
    assert 'dbutils.widgets.get("acknowledge_superseded").lower() != "true"' in first_code_cell
    assert "raise RuntimeError(" in first_code_cell


@pytest.mark.parametrize(
    "job_name, gate, notebook",
    [
        (
            "urbanflow_historical_trips_test",
            "run_ingest",
            "../notebooks/06_historical_trips.py",
        ),
        (
            "urbanflow_weather_enrichment_test",
            "run_enrichment",
            "../notebooks/07_weather_enrichment.py",
        ),
    ],
)
def test_phase3_jobs_are_unscheduled_and_fail_closed(
    job_name: str, gate: str, notebook: str
) -> None:
    """A deployed Job must not be able to run itself, and must do nothing by default."""
    resource = yaml.safe_load((PROJECT_ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    job = resource["resources"]["jobs"][job_name]
    defaults = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}

    assert defaults[gate] == "false"
    assert defaults["execution_id"] == "NOT_APPROVED"
    assert "schedule" not in job
    assert "continuous" not in job
    assert job["max_concurrent_runs"] == 1
    task = job["tasks"][0]
    # Existing approved compute only: a new_cluster would create billable infrastructure.
    assert task["existing_cluster_id"] == "${var.compute_cluster_id}"
    assert "new_cluster" not in task
    assert task["notebook_task"]["notebook_path"] == notebook
    # Target isolation: a dev run must write dev objects.
    for name in ("catalog", "schema", "volume"):
        assert defaults[name] == "${var." + name + "}"
        assert task["notebook_task"]["base_parameters"][name] == "{{job.parameters." + name + "}}"


def test_the_historical_notebook_wait_bound_fits_inside_its_task_timeout() -> None:
    """A wait that outlives its task is killed before the evidence report is written."""
    resource = yaml.safe_load((PROJECT_ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8"))
    job = resource["resources"]["jobs"]["urbanflow_historical_trips_test"]
    defaults = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}

    assert float(defaults["stream_timeout_seconds"]) < job["tasks"][0]["timeout_seconds"]
    assert job["tasks"][0]["timeout_seconds"] <= job["timeout_seconds"]


def test_the_weather_notebook_never_downloads_an_archive_or_starts_compute() -> None:
    """Phase 3 added two notebooks; neither may acquire data or compute on its own."""
    for name in ("06_historical_trips.py", "07_weather_enrichment.py"):
        code = "\n".join(cell for _, cell in _code_cells(NOTEBOOK_DIR / name))
        for forbidden in ("clusters.create", "clusters.start", "clusters.restart", "pip install"):
            assert forbidden not in code, f"{name} contains {forbidden}"
        # Both must gate on the approved-cluster allowlist before any action.
        assert "APPROVED_RUN_CLUSTER_IDS" in code


def test_the_historical_notebook_reads_the_persisted_dimension_not_a_landed_feed() -> None:
    """A read-only preflight found `landing/station_information` has never existed.

    The earlier version of this cell loaded that path, so the run would have failed at the
    join step AFTER writing the Bronze trips table, leaving a partial result. Reading the
    dimension table Gold already produced removes the dependency entirely.
    """
    code = "\n".join(cell for _, cell in _code_cells(NOTEBOOK_DIR / "06_historical_trips.py"))

    assert "dim_station_development_sample" in code
    assert "spark.table(dimension_table)" in code
    assert "landing/station_information" not in code
    # And it must fail loudly rather than reporting every trip as unmatched.
    assert "does not exist" in code
    assert "is empty, so every trip would report as unmatched" in code

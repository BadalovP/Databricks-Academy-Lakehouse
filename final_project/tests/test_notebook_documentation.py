from __future__ import annotations

from pathlib import Path

import pytest

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
    "**How to explain it:**",
    "**Rerun and cost considerations:**",
)
# Teaching cells must stay reviewable. 30 non-blank lines is the enforced ceiling; the target
# while writing a new cell is 5-25 lines.
MAX_CODE_CELL_LINES = 30
NOT_EXECUTED_PHRASES = ("has not been run", "has not been executed", "No live messages")
# Final Project notebooks may claim a live result only with Final Project evidence. Demo 3's
# live runs belong to Demo 3. Entries are added here when a Final Project run is recorded.
VALIDATED_LIVE_CLAIMS: dict[Path, tuple[str, ...]] = {
    Path("notebooks/02_land_bounded_sources.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/04_bronze_to_silver.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/05_silver_to_gold.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/06_historical_trips.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/07_weather_enrichment.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/08_unified_preflight.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/09_station_source_check.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/10_unified_final_validation.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
    ),
    Path("notebooks/11_federation_cdc.py"): (
        "Final Project run 896549012431425",
        "Personal DEV on 2026-10-12",
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


@pytest.mark.parametrize(
    "notebook",
    [
        "03_eventhubs_to_bronze.py",
        "04_bronze_to_silver.py",
        "05_silver_to_gold.py",
        "06_historical_trips.py",
        "07_weather_enrichment.py",
        "08_unified_preflight.py",
    ],
)
def test_every_data_task_refuses_classic_clusters(notebook: str) -> None:
    """UrbanFlow 2.0 is serverless-only: no notebook may attach to a classic or shared cluster."""
    text = (NOTEBOOK_DIR / notebook).read_text(encoding="utf-8")
    assert "from urbanflow.automation import assert_serverless_compute" in text
    assert "assert_serverless_compute(spark.conf.get)" in text
    for forbidden in ("APPROVED_RUN_CLUSTER_IDS", "0702-132442-toro5spu", "0702-171207-xo9bbc0y"):
        assert forbidden not in text, (notebook, forbidden)


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


# Job structure, defaults, parameter wiring and timeouts are covered by tests/test_final_release.py.


def test_the_weather_notebook_never_downloads_an_archive_or_starts_compute() -> None:
    """Phase 3 added two notebooks; neither may acquire data or compute on its own."""
    for name in ("06_historical_trips.py", "07_weather_enrichment.py"):
        code = "\n".join(cell for _, cell in _code_cells(NOTEBOOK_DIR / name))
        for forbidden in ("clusters.create", "clusters.start", "clusters.restart", "pip install"):
            assert forbidden not in code, f"{name} contains {forbidden}"
        # Both must refuse classic clusters before any action.
        assert "assert_serverless_compute(spark.conf.get)" in code


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

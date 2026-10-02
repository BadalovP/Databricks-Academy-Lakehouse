"""Tests for the prepared SQL assets: dashboard queries, governance DDL and maintenance.

These files are the ones most likely to be copied into a SQL editor and run, so the checks
here are about what they would DO if that happened. The governance and maintenance files
contain genuinely dangerous statements - VACUUM destroys time travel, a row filter silently
changes what every other principal can see - and the project's position is that they stay
commented until separately approved. A test is the only thing that keeps that true as the files
are edited.

The dashboard files are checked for the opposite property: they must be pure reads, and they
must not quietly reach outside the UrbanFlow schema into another student's data.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"
SQL_FILES = sorted(SQL_DIR.glob("*.sql"))
DASHBOARD_FILES = sorted(SQL_DIR.glob("1*_dashboard_*.sql"))
GOVERNANCE = SQL_DIR / "20_governance_rls_cls.sql"
MAINTENANCE = SQL_DIR / "21_maintenance_optimize_vacuum.sql"

OUR_SCHEMA = "parvinbadalov_urbanflow"
# Other students' and other projects' schemas must never be referenced.
FOREIGN_SCHEMAS = ("demo1", "demo2", "demo2_olist", "lab07", "lab08", "lab09", "default")


def _active_sql(path: Path) -> str:
    """Only the statements that would actually execute; comment lines are stripped."""
    lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
    return "\n".join(lines)


def test_there_are_sql_assets_to_check() -> None:
    """A glob that silently matches nothing would make every test below vacuous."""
    assert SQL_FILES
    assert len(DASHBOARD_FILES) == 4
    assert GOVERNANCE.exists() and MAINTENANCE.exists()


# --- the dashboard queries are pure reads ---------------------------------------


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda p: p.name)
def test_dashboard_queries_only_read(path: Path) -> None:
    active = _active_sql(path).upper()

    for statement in (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "MERGE ",
        "DROP ",
        "TRUNCATE ",
        "ALTER ",
        "CREATE ",
        "GRANT ",
        "VACUUM",
        "OPTIMIZE ",
    ):
        assert statement not in active, f"{path.name} contains an active {statement.strip()}"


@pytest.mark.parametrize("path", DASHBOARD_FILES, ids=lambda p: p.name)
def test_dashboard_queries_stay_inside_the_urbanflow_schema(path: Path) -> None:
    """A stray schema name here would read another student's data."""
    active = _active_sql(path).lower()

    assert OUR_SCHEMA in active
    for schema in FOREIGN_SCHEMAS:
        assert f".{schema}." not in active, f"{path.name} references {schema}"


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_every_table_reference_is_fully_qualified(path: Path) -> None:
    """An unqualified name resolves against whatever the session default happens to be."""
    active = _active_sql(path)
    references = re.findall(r"\b(?:FROM|JOIN|INTO|TABLE)\s+([A-Za-z_][\w.`]*)", active)

    for reference in references:
        bare = reference.strip("`")
        if bare.lower() in {"values", "counts", "exploded"}:
            continue  # a CTE or inline VALUES alias, not a table
        assert bare.count(".") >= 2, f"{path.name} references {bare!r} without a full name"


def test_the_snapshot_limitation_is_stated_where_it_matters() -> None:
    """A point-in-time count presented as a trend is the main way this dashboard could mislead."""
    current = (SQL_DIR / "10_dashboard_current_operations.sql").read_text(encoding="utf-8")
    historical = (SQL_DIR / "11_dashboard_historical_demand.sql").read_text(encoding="utf-8")

    assert "not a trend" in current
    assert "reading_note" in current
    # The historical section is the one place a trend IS legitimate, and it says why.
    assert "genuine trend" in historical


def test_the_weather_section_states_its_resolution_and_refuses_to_predict() -> None:
    weather = (SQL_DIR / "12_dashboard_weather.sql").read_text(encoding="utf-8")

    assert "ONE COORDINATE" in weather
    assert "not per-station weather" in weather
    assert "not predictions" in weather or "not a fitted curve" in weather
    # Coverage must travel with the figures, or a half-covered chart looks complete.
    assert "weather_coverage" in weather


def test_historical_and_weather_queries_keep_execution_scopes_separate() -> None:
    """A future full archive must not be summed together with the 40-row sample."""
    historical = (SQL_DIR / "11_dashboard_historical_demand.sql").read_text(encoding="utf-8")
    weather = (SQL_DIR / "12_dashboard_weather.sql").read_text(encoding="utf-8")

    assert historical.count("40-ROW DEVELOPMENT SAMPLE") >= 6
    assert historical.count("execution_id") >= 12
    assert "OVER (PARTITION BY execution_id)" in historical
    assert "LIMIT 25" not in historical
    assert weather.count("48-HOUR WEATHER / 40-TRIP DEVELOPMENT SAMPLE") >= 4
    assert weather.count("execution_id") >= 8


def test_weather_notebook_selects_one_historical_execution() -> None:
    notebook = (PROJECT_ROOT / "notebooks" / "07_weather_enrichment.py").read_text(encoding="utf-8")
    jobs = (PROJECT_ROOT / "resources" / "jobs.yml").read_text(encoding="utf-8")

    assert 'dbutils.widgets.text("source_execution_id", "")' in notebook
    assert 'F.col("execution_id") == F.lit(source_execution_id)' in notebook
    assert 'source_execution_id: "{{job.parameters.source_execution_id}}"' in jobs


def test_the_quality_section_checks_the_reconciliation_identity() -> None:
    quality = (SQL_DIR / "13_dashboard_data_quality.sql").read_text(encoding="utf-8")

    assert "bronze_reconciles" in quality
    assert "silver_rows + quarantine_rows + duplicate_rows" in quality
    # Unassigned execution rows are what let 89 stale priority rows survive a correction.
    assert "unassigned_rows" in quality


# --- the dangerous statements stay commented ------------------------------------


def test_no_vacuum_is_ever_active() -> None:
    """VACUUM permanently destroys the files time travel and RESTORE depend on."""
    for path in SQL_FILES:
        assert "VACUUM" not in _active_sql(path).upper(), f"{path.name} has an active VACUUM"


def test_the_retention_safety_check_is_never_disabled() -> None:
    """Disabling it is what makes RETAIN 0 HOURS possible, and it is irreversible."""
    for path in SQL_FILES:
        active = _active_sql(path)
        assert "retentionDurationCheck" not in active
        assert "RETAIN 0 HOURS" not in active.upper()


@pytest.mark.parametrize(
    "fragment",
    ["SET ROW FILTER", "SET MASK", "DROP ROW FILTER", "DROP MASK", "RENAME COLUMN"],
)
def test_access_and_schema_changes_stay_commented(fragment: str) -> None:
    """Applying a filter or mask silently changes results for other principals."""
    assert fragment not in _active_sql(GOVERNANCE).upper()
    assert fragment not in _active_sql(MAINTENANCE).upper()


def test_no_grant_is_active() -> None:
    assert "GRANT " not in _active_sql(GOVERNANCE).upper()


def test_no_optimize_or_table_property_change_is_active() -> None:
    """These rewrite files or change reader/writer versions, sometimes one-way."""
    active = _active_sql(MAINTENANCE).upper()

    assert "OPTIMIZE " not in active
    assert "SET TBLPROPERTIES" not in active
    assert "CLUSTER BY" not in active
    assert "REORG TABLE" not in active
    assert "RESTORE TABLE" not in active


def test_the_only_active_statements_in_maintenance_are_inspections() -> None:
    """DESCRIBE reads metadata and changes nothing, which is the whole point of the file."""
    statements = [
        line.strip()
        for line in _active_sql(MAINTENANCE).splitlines()
        if line.strip() and not line.strip().startswith(")")
    ]

    assert statements
    for statement in statements:
        assert statement.upper().startswith("DESCRIBE"), f"unexpected active statement: {statement}"


def test_governance_active_statements_are_only_functions_and_inspections() -> None:
    """Creating a masking function changes nothing until it is APPLIED to a column."""
    active = _active_sql(GOVERNANCE)

    assert "CREATE OR REPLACE FUNCTION" in active
    # Every table-altering application is commented, so the functions sit unused until approved.
    assert "ALTER TABLE" not in active.upper()
    # The inspection queries must be live, since they are what proves nothing is applied.
    assert "information_schema.column_masks" in active
    assert "information_schema.row_filters" in active


def test_the_vacuum_warning_is_prominent_and_explains_the_irreversibility() -> None:
    text = MAINTENANCE.read_text(encoding="utf-8")
    warning = text[: text.index("-- 1.")]

    assert "VACUUM WARNING" in warning
    assert "time travel" in warning
    assert "no undo" in warning or "irreversible" in warning.lower()


def test_abac_is_described_without_claiming_a_working_policy() -> None:
    """The project has not confirmed the entitlement, so it must not claim one."""
    text = GOVERNANCE.read_text(encoding="utf-8")

    assert "ABAC" in text
    assert "NOT as a working policy" in text

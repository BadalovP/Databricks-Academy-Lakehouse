"""Static checks of SQL, notebook, configuration and CI assets."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from lab11.events import BRONZE_COLUMNS
from lab11.idempotency import SILVER_COLUMNS
from lab11.verification import SQL_DIR, split_statements

LAB = Path(__file__).resolve().parents[1]
WORKFLOW = LAB.parents[1] / ".github" / "workflows" / "lab11_ci.yml"
SQL_FILES = sorted(SQL_DIR.glob("*.sql"))
DESTRUCTIVE = re.compile(
    r"\b(DROP|TRUNCATE|DELETE\s+FROM|INSERT\s+OVERWRITE|CREATE\s+OR\s+REPLACE|ALTER\s+TABLE)\b",
    re.I,
)


def ddl_columns(table_suffix: str) -> dict[str, str]:
    ddl = (SQL_DIR / "00_create_tables.sql").read_text(encoding="utf-8")
    body = re.search(r"\$\{" + table_suffix + r"\}\s*\((.*?)\)\s*COMMENT", ddl, re.S).group(1)
    return dict(line.strip().rstrip(",").split() for line in body.strip().splitlines())


def test_bronze_ddl_is_exactly_the_producer_contract():
    assert ddl_columns("bronze") == BRONZE_COLUMNS


def test_silver_ddl_matches_the_merge_columns():
    assert list(ddl_columns("silver")) == list(SILVER_COLUMNS)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_sql_assets_are_non_destructive(path):
    code = "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )
    assert not DESTRUCTIVE.search(code), f"{path.name} contains a destructive statement"


def test_creation_is_idempotent():
    statements = split_statements((SQL_DIR / "00_create_tables.sql").read_text(encoding="utf-8"))
    assert len(statements) == 3
    assert all(re.match(r"CREATE (SCHEMA|TABLE) IF NOT EXISTS ", s) for s in statements)


def test_merge_only_updates_changed_rows_and_breaks_ties_deterministically():
    merge = (SQL_DIR / "01_deduplication.sql").read_text(encoding="utf-8")
    assert "ORDER BY produced_at, batch_id" in merge
    assert "WHEN MATCHED AND NOT (" in merge
    assert "WHEN NOT MATCHED THEN INSERT" in merge
    assert "THEN DELETE" not in merge.upper()


def test_validation_is_read_only():
    statements = split_statements((SQL_DIR / "02_validation.sql").read_text(encoding="utf-8"))
    assert len(statements) == 1 and statements[0].startswith("WITH ")
    assert not re.search(r"\b(MERGE|INSERT|UPDATE|CREATE)\b", statements[0], re.I)


def test_every_placeholder_is_known():
    known = {"catalog", "schema", "bronze", "silver", "run_id"}
    for path in SQL_FILES:
        assert set(re.findall(r"\$\{(\w+)\}", path.read_text(encoding="utf-8"))) <= known


def test_notebook_runs_the_same_sql_assets():
    notebook = (LAB / "notebooks" / "01_validate_zerobus.py").read_text(encoding="utf-8")
    assert notebook.startswith("# Databricks notebook source")
    assert '"01_deduplication.sql"' in notebook and '"02_validation.sql"' in notebook
    assert "bronze_iot_events" in notebook and "INSERT" not in notebook.upper()


def test_ci_needs_no_secrets_and_deploys_nothing():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "secrets." not in workflow
    assert "labs/lab_11_zerobus/**" in workflow
    for forbidden in (
        "--live",
        "--confirm",
        "setup_tables.py",
        "run_validation.py",
        "check_access.py",
        "databricks bundle deploy",
        "terraform apply",
    ):
        assert forbidden not in workflow
    producer_calls = [line for line in workflow.splitlines() if "run_producer.py" in line]
    assert producer_calls and all("--dry-run" in line for line in producer_calls)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_no_semicolons_inside_sql_string_literals(path):
    # split_statements splits on ';', so a ';' inside a literal would cut a statement in two.
    for literal in re.findall(r"'([^']*)'", path.read_text(encoding="utf-8")):
        assert ";" not in literal, f"{path.name}: ';' inside literal {literal!r}"

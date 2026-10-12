"""Local integration tests: the real SQL assets executed by DuckDB, compared with the Python spec.

DuckDB stands in for Databricks SQL here - same MERGE, QUALIFY and validation statements, rendered
from the files in sql/. This proves the deduplication logic, not Databricks or Zerobus behavior.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from lab11.events import BRONZE_COLUMNS
from lab11.idempotency import SILVER_COLUMNS, canonical_rows
from lab11.verification import expected_metrics, render_file, split_statements

duckdb = pytest.importorskip("duckdb")

SILVER_TYPES = {
    **{c: BRONZE_COLUMNS[c] for c in SILVER_COLUMNS if c in BRONZE_COLUMNS},
    "first_batch_id": "STRING",
    "first_produced_at": "TIMESTAMP",
    "delivery_count": "BIGINT",
}
TIMESTAMPS = {"event_timestamp", "produced_at", "first_produced_at"}


@pytest.fixture
def local(target):
    local_target = replace(target, catalog="memory", schema="lab11")
    con = duckdb.connect()
    con.execute("CREATE SCHEMA memory.lab11")
    # DDL derived from sql/00_create_tables.sql (whose column lists a static test pins).
    for statement in split_statements(render_file("00_create_tables.sql", local_target))[1:]:
        con.execute(statement.split("\nCOMMENT")[0])
    return con, local_target


def insert_bronze(con, target, records):
    columns = list(BRONZE_COLUMNS)
    values = ", ".join(
        f"make_timestamp(${i + 1})" if c in TIMESTAMPS else f"${i + 1}"
        for i, c in enumerate(columns)
    )
    for record in records:
        con.execute(
            f"INSERT INTO {target.bronze_table} ({', '.join(columns)}) VALUES ({values})",
            [record[c] for c in columns],
        )


def silver_rows(con, target):
    select = ", ".join(f"epoch_us({c}) AS {c}" if c in TIMESTAMPS else c for c in SILVER_COLUMNS)
    cursor = con.execute(f"SELECT {select} FROM {target.silver_table} ORDER BY event_id")
    names = [d[0] for d in cursor.description]
    return {row[0]: dict(zip(names, row, strict=True)) for row in cursor.fetchall()}


def run_merge(con, target) -> int:
    return con.execute(split_statements(render_file("01_deduplication.sql", target))[0]).fetchone()[
        0
    ]


def test_ddl_types_match_the_contract(local):
    con, target = local
    bronze = [r[0] for r in con.execute(f"DESCRIBE {target.bronze_table}").fetchall()]
    assert bronze == list(BRONZE_COLUMNS)
    silver = [r[0] for r in con.execute(f"DESCRIBE {target.silver_table}").fetchall()]
    assert silver == list(SILVER_COLUMNS) == list(SILVER_TYPES)


def test_sql_merge_matches_the_python_reference_and_is_idempotent(local, plan):
    con, target = local
    insert_bronze(con, target, plan.records)
    assert run_merge(con, target) == 24
    assert silver_rows(con, target) == canonical_rows(plan.records)
    assert run_merge(con, target) == 0  # unchanged Bronze: nothing rewritten
    assert silver_rows(con, target) == canonical_rows(plan.records)


def test_a_late_duplicate_updates_only_its_delivery_count(local, plan):
    con, target = local
    insert_bronze(con, target, plan.records)
    run_merge(con, target)
    late = {
        **plan.records[5],
        "batch_id": "late",
        "produced_at": plan.records[5]["produced_at"] + 10**9,
    }
    insert_bronze(con, target, [late])
    assert run_merge(con, target) == 1
    assert silver_rows(con, target)[late["event_id"]]["delivery_count"] == 2


def test_validation_sql_reconciles_the_run(local, plan):
    con, target = local
    insert_bronze(con, target, plan.records)
    run_merge(con, target)
    cursor = con.execute(split_statements(render_file("02_validation.sql", target, plan.run_id))[0])
    observed = dict(zip([d[0] for d in cursor.description], cursor.fetchone(), strict=True))
    assert observed == expected_metrics(plan)


def test_validation_sql_detects_a_missing_silver_row_and_a_conflict(local, plan):
    con, target = local
    insert_bronze(con, target, plan.records)
    run_merge(con, target)
    victim = plan.records[0]
    con.execute(f"DELETE FROM {target.silver_table} WHERE event_id = ?", [victim["event_id"]])
    insert_bronze(
        con, target, [{**victim, "batch_id": "conflict", "temperature": victim["temperature"] + 1}]
    )
    cursor = con.execute(split_statements(render_file("02_validation.sql", target, plan.run_id))[0])
    observed = dict(zip([d[0] for d in cursor.description], cursor.fetchone(), strict=True))
    assert observed["missing_in_silver"] == 1
    assert observed["conflicting_events"] == 1

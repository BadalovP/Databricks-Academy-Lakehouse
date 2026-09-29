"""Small, auditable Delta persistence helpers shared by Silver and Gold.

The helpers keep SQL identifier validation and MERGE rendering in tested Python.  They
do not start compute or create anything by themselves; a caller must deliberately call
``merge_delta_table`` from an approved Databricks notebook.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from typing import Any

_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_qualified_identifier(value: str) -> str:
    """Validate and quote a one-to-three-part Spark SQL identifier."""
    parts = value.split(".")
    if not 1 <= len(parts) <= 3 or any(not _PLAIN_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(
            "Delta table names must contain one to three plain identifier parts "
            f"(letters, digits and underscores), received {value!r}."
        )
    return ".".join(f"`{part}`" for part in parts)


def _quote_column(value: str) -> str:
    if not _PLAIN_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid Delta column name {value!r}.")
    return f"`{value}`"


def build_merge_sql(
    *,
    table_name: str,
    source_view: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
) -> str:
    """Render a null-safe Delta MERGE statement after validating every identifier."""
    if not columns:
        raise ValueError("At least one source column is required for a Delta MERGE.")
    if not key_columns:
        raise ValueError("At least one key column is required for a Delta MERGE.")
    missing = set(key_columns) - set(columns)
    if missing:
        raise ValueError(f"MERGE keys are absent from the source contract: {sorted(missing)}")

    target = quote_qualified_identifier(table_name)
    view = quote_qualified_identifier(source_view)
    quoted_columns = [_quote_column(column) for column in columns]
    quoted_keys = [_quote_column(column) for column in key_columns]
    non_keys = [column for column in quoted_columns if column not in quoted_keys]

    condition = " AND ".join(f"t.{column} <=> s.{column}" for column in quoted_keys)
    update = ", ".join(f"t.{column} = s.{column}" for column in non_keys)
    insert_columns = ", ".join(quoted_columns)
    insert_values = ", ".join(f"s.{column}" for column in quoted_columns)
    matched = f"WHEN MATCHED THEN UPDATE SET {update} " if update else ""
    return (
        f"MERGE INTO {target} AS t USING {view} AS s ON {condition} "
        f"{matched}WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )


def merge_delta_table(
    spark: Any,
    frame: Any,
    table_name: str,
    *,
    key_columns: Sequence[str],
) -> dict[str, int | bool]:
    """Create a managed Delta table when absent, then idempotently MERGE unique rows.

    Duplicate source keys are rejected instead of being silently collapsed.  This both
    prevents Delta's multiple-match error and ensures upstream deduplication remains an
    explicit, reconciled business rule.
    """
    quote_qualified_identifier(table_name)
    columns = tuple(frame.columns)
    for key in key_columns:
        _quote_column(key)
    missing = set(key_columns) - set(columns)
    if missing:
        raise ValueError(f"MERGE keys are absent from the source frame: {sorted(missing)}")

    source_rows = frame.count()
    unique_keys = frame.select(*key_columns).distinct().count()
    if source_rows != unique_keys:
        raise ValueError(
            f"Refusing MERGE into {table_name}: {source_rows - unique_keys} duplicate source keys."
        )

    existed = spark.catalog.tableExists(table_name)
    if not existed:
        frame.limit(0).write.format("delta").saveAsTable(table_name)
    rows_before = spark.table(table_name).count()

    digest = hashlib.sha256(table_name.encode("utf-8")).hexdigest()[:12]
    source_view = f"urbanflow_merge_source_{digest}"
    frame.createOrReplaceTempView(source_view)
    spark.sql(
        build_merge_sql(
            table_name=table_name,
            source_view=source_view,
            columns=columns,
            key_columns=key_columns,
        )
    )
    rows_after = spark.table(table_name).count()
    return {
        "table_existed": existed,
        "source_rows": source_rows,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "inserted_rows": rows_after - rows_before,
    }

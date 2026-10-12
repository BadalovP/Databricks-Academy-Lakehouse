"""Explicitly gated Delta Lake DDL and idempotent MERGE operations."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any, Protocol

from .models import Chunk, Document

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SparkLike(Protocol):
    def sql(self, query: str) -> Any: ...

    def createDataFrame(self, data: list[dict[str, Any]]) -> Any: ...  # noqa: N802


def qualified_name(catalog: str, schema: str, table: str) -> str:
    for value in (catalog, schema, table):
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")
    return ".".join(f"`{value}`" for value in (catalog, schema, table))


def create_lab_tables(
    spark: SparkLike,
    *,
    catalog: str,
    schema: str,
    tables: dict[str, str],
    allow_create: bool = False,
) -> None:
    """Create only Lab 12-owned schema/tables; never drop or replace anything."""
    if not allow_create:
        raise PermissionError("Table creation is disabled; pass allow_create=True after approval.")
    schema_name = qualified_name(catalog, schema, "placeholder").rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {qualified_name(catalog, schema, tables['documents'])} (
          document_id STRING NOT NULL, title STRING, source_name STRING, source_path STRING,
          content STRING, content_hash STRING, metadata_json STRING, ingested_at TIMESTAMP
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {qualified_name(catalog, schema, tables['chunks'])} (
          chunk_id STRING NOT NULL, document_id STRING NOT NULL, source_name STRING,
          section_heading STRING, section_ref STRING, ordinal INT, content STRING,
          content_hash STRING, metadata_json STRING, ingested_at TIMESTAMP
        ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {qualified_name(catalog, schema, tables['evaluation_results'])} (
          evaluation_id STRING, evaluated_at TIMESTAMP, variant STRING, question_id STRING,
          metrics_json STRING, error_status STRING
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {qualified_name(catalog, schema, tables['monitoring'])} (
          event_id STRING, query_timestamp TIMESTAMP, question_hash STRING, question_chars INT,
          model_id STRING, index_id STRING, retrieved_chunk_ids ARRAY<STRING>, latency_ms BIGINT,
          error_status STRING, evaluation_json STRING
        ) USING DELTA"""
    )


# Explicit schemas: Spark cannot infer a type from an empty list (e.g. no retrieved chunks).
DOCUMENT_SCHEMA = (
    "document_id STRING, title STRING, source_name STRING, source_path STRING, "
    "content STRING, content_hash STRING, metadata_json STRING"
)
CHUNK_SCHEMA = (
    "chunk_id STRING, document_id STRING, source_name STRING, section_heading STRING, "
    "section_ref STRING, ordinal INT, content STRING, content_hash STRING, metadata_json STRING"
)
EVALUATION_SCHEMA = (
    "evaluation_id STRING, evaluated_at STRING, variant STRING, question_id STRING, "
    "metrics_json STRING, error_status STRING"
)
MONITORING_SCHEMA = (
    "event_id STRING, query_timestamp STRING, question_hash STRING, question_chars INT, "
    "model_id STRING, index_id STRING, retrieved_chunk_ids ARRAY<STRING>, latency_ms BIGINT, "
    "error_status STRING, evaluation_json STRING"
)


# A metric nobody reported is never shown as 0: a zero-change rerun must be proven, not assumed.
UNAVAILABLE = "unavailable"
_STATEMENT_COLUMNS = {
    "inserted": "num_inserted_rows",
    "updated": "num_updated_rows",
    "deleted": "num_deleted_rows",
}
_HISTORY_KEYS = {
    "inserted": "numTargetRowsInserted",
    "updated": "numTargetRowsUpdated",
    "deleted": "numTargetRowsDeleted",
}


def merge_documents(
    spark: SparkLike,
    table_name: str,
    documents: Iterable[Document],
    *,
    allow_write: bool = False,
    prune_missing: bool = True,
) -> dict[str, Any]:
    rows = []
    for document in documents:
        row = asdict(document)
        row.pop("sections")
        row["metadata_json"] = _json(row.pop("metadata"))
        rows.append(row)
    return _merge_rows(
        spark, table_name, rows, "document_id", allow_write, DOCUMENT_SCHEMA, prune_missing
    )


def merge_chunks(
    spark: SparkLike,
    table_name: str,
    chunks: Iterable[Chunk],
    *,
    allow_write: bool = False,
    prune_missing: bool = True,
) -> dict[str, Any]:
    rows = []
    for chunk in chunks:
        row = asdict(chunk)
        row["metadata_json"] = _json(row.pop("metadata"))
        rows.append(row)
    return _merge_rows(
        spark, table_name, rows, "chunk_id", allow_write, CHUNK_SCHEMA, prune_missing
    )


def _merge_rows(
    spark: SparkLike,
    table_name: str,
    rows: list[dict[str, Any]],
    key: str,
    allow_write: bool,
    schema: str,
    prune_missing: bool,
) -> dict[str, Any]:
    """Upsert one full corpus snapshot and report what it changed.

    With ``prune_missing`` (the default) rows absent from the snapshot are deleted. Without it,
    editing or removing a document left its old chunks in the table - and, through Change Data
    Feed, in the AI Search index - so retrieval kept serving stale content. An empty snapshot
    returns before any MERGE, so it can never wipe the table.

    The report holds two independent sources: the row counts the MERGE statement returns, and
    the operation metrics of the Delta commit it produced (if it produced one).
    """
    if not allow_write:
        raise PermissionError("Delta writes are disabled; pass allow_write=True after approval.")
    if not rows:
        return {"skipped": "empty snapshot"}
    if not re.fullmatch(r"`[^`]+`\.`[^`]+`\.`[^`]+`", table_name):
        raise ValueError("table_name must come from qualified_name().")
    version_before = latest_version(spark, table_name)
    # Skip a MERGE that cannot change anything: it would still write an empty Delta version.
    # "Unchanged" uses the MERGE's own rule - same keys and content hashes - and an existing table
    # with every ingested_at populated; any doubt (no snapshot) falls through to the MERGE.
    try:
        current = table_snapshot(spark, table_name, key)
    except RuntimeError:
        current = None
    expected = _rows_fingerprint(rows, key)
    if (
        current is not None
        and current["row_count"] == len(rows)
        and current["distinct_keys"] == len(rows)
        and current["fingerprint"] == expected
        and current["null_ingested_at"] == 0
    ):
        return {"skipped": "table already matches the snapshot", "version": version_before}
    frame = spark.createDataFrame(rows, schema=schema)
    view = f"urbanflow_final_rag_merge_{key}"
    frame.createOrReplaceTempView(view)
    columns = list(rows[0])
    update = ", ".join(f"target.`{name}` = source.`{name}`" for name in columns)
    insert_names = ", ".join(f"`{name}`" for name in columns)
    insert_values = ", ".join(f"source.`{name}`" for name in columns)
    prune = "\n        WHEN NOT MATCHED BY SOURCE THEN DELETE" if prune_missing else ""
    result = spark.sql(
        f"""MERGE INTO {table_name} target USING {view} source
        ON target.`{key}` = source.`{key}`
        WHEN MATCHED AND target.content_hash <> source.content_hash
          THEN UPDATE SET {update}, target.`ingested_at` = current_timestamp()
        WHEN NOT MATCHED
          THEN INSERT ({insert_names}, `ingested_at`) VALUES ({insert_values}, current_timestamp()){prune}"""
    )
    return {
        "statement": statement_metrics(result),
        "history": history_metrics(spark, table_name, version_before),
    }


def statement_metrics(result: Any) -> dict[str, Any]:
    """Row counts returned by the MERGE statement itself (one row on Databricks)."""
    metrics: dict[str, Any] = dict.fromkeys(_STATEMENT_COLUMNS, UNAVAILABLE)
    row = _first(result)
    if row is None:
        return metrics
    for name, column in _STATEMENT_COLUMNS.items():
        value = _field(row, column)
        if value is not None:
            metrics[name] = int(value)
    return metrics


def latest_version(spark: SparkLike, table_name: str) -> int | None:
    row = _first(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1"))
    version = _field(row, "version") if row is not None else None
    return int(version) if version is not None else None


def history_metrics(
    spark: SparkLike, table_name: str, version_before: int | None
) -> dict[str, Any]:
    """Operation metrics of the commit the MERGE produced.

    Delta may not write a new version when nothing changes; then there is no commit to read and
    every metric is reported as unavailable, never as zero.
    """
    report: dict[str, Any] = {
        "version_before": version_before,
        **dict.fromkeys(_HISTORY_KEYS, UNAVAILABLE),
    }
    row = _first(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1"))
    version = _field(row, "version") if row is not None else None
    if version is None:
        report["commit"] = "history unavailable"
        return report
    report["version_after"] = int(version)
    if version_before is not None and int(version) <= version_before:
        report["commit"] = "no new Delta version"
        return report
    operation = _field(row, "operation")
    if operation != "MERGE":
        report["commit"] = f"latest commit is {operation}, not MERGE"
        return report
    report["commit"] = "MERGE"
    operation_metrics = _field(row, "operationMetrics") or {}
    for name, key in _HISTORY_KEYS.items():
        value = operation_metrics.get(key)
        if value not in (None, ""):
            report[name] = int(value)
    return report


def table_snapshot(spark: SparkLike, table_name: str, key: str) -> dict[str, Any]:
    """Count, key uniqueness and an order-independent fingerprint of (key, content_hash)."""
    if not re.fullmatch(r"`[^`]+`\.`[^`]+`\.`[^`]+`", table_name):
        raise ValueError("table_name must come from qualified_name().")
    if not _IDENTIFIER.fullmatch(key):
        raise ValueError(f"Unsafe column name: {key!r}")
    row = _first(
        spark.sql(
            f"""SELECT count(*) AS row_count, count(DISTINCT `{key}`) AS distinct_keys,
            sha2(concat_ws(',', sort_array(collect_list(concat(`{key}`, ':', content_hash)))), 256)
              AS fingerprint,
            CAST(max(ingested_at) AS STRING) AS max_ingested_at,
            count_if(ingested_at IS NULL) AS null_ingested_at
            FROM {table_name}"""
        )
    )
    if row is None:
        raise RuntimeError(f"No snapshot returned for {table_name}.")
    return {
        name: _field(row, name)
        for name in (
            "row_count",
            "distinct_keys",
            "fingerprint",
            "max_ingested_at",
            "null_ingested_at",
        )
    }


def snapshot_fingerprint(items: Iterable[Document | Chunk], key: str) -> str:
    """The same fingerprint as ``table_snapshot`` computed locally from the corpus."""
    return _fingerprint(f"{getattr(item, key)}:{item.content_hash}" for item in items)


def _rows_fingerprint(rows: list[dict[str, Any]], key: str) -> str:
    return _fingerprint(f"{row[key]}:{row['content_hash']}" for row in rows)


def _fingerprint(pairs: Iterable[str]) -> str:
    import hashlib

    return hashlib.sha256(",".join(sorted(pairs)).encode("utf-8")).hexdigest()


def change_data_feed_enabled(spark: SparkLike, table_name: str) -> bool:
    rows = _collect(spark.sql(f"SHOW TBLPROPERTIES {table_name} ('delta.enableChangeDataFeed')"))
    return any(str(_field(row, "value")).lower() == "true" for row in rows)


def _collect(result: Any) -> list[Any]:
    return list(result.collect()) if hasattr(result, "collect") else []


def _first(result: Any) -> Any:
    rows = _collect(result)
    return rows[0] if rows else None


def _field(row: Any, name: str) -> Any:
    """Read a column from a Spark Row or a dict; a missing column is None."""
    try:
        return row[name]
    except (KeyError, ValueError, IndexError, TypeError):
        return None


def append_rows(
    spark: SparkLike,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    schema: str,
    timestamp_column: str,
    allow_write: bool = False,
) -> int:
    """Append evaluation or monitoring rows; ISO-8601 strings are cast to TIMESTAMP."""
    if not allow_write:
        raise PermissionError("Delta writes are disabled; pass allow_write=True after approval.")
    if not rows:
        return 0
    if not re.fullmatch(r"`[^`]+`\.`[^`]+`\.`[^`]+`", table_name):
        raise ValueError("table_name must come from qualified_name().")
    frame = spark.createDataFrame(rows, schema=schema)
    view = "urbanflow_final_rag_append_rows"
    frame.createOrReplaceTempView(view)
    columns = [part.strip().split(" ", 1)[0] for part in _split_schema(schema)]
    selected = ", ".join(
        f"CAST(`{name}` AS TIMESTAMP) AS `{name}`" if name == timestamp_column else f"`{name}`"
        for name in columns
    )
    names = ", ".join(f"`{name}`" for name in columns)
    spark.sql(f"INSERT INTO {table_name} ({names}) SELECT {selected} FROM {view}")
    return len(rows)


def _split_schema(schema: str) -> list[str]:
    """Split a DDL schema on top-level commas only (ARRAY<...> may contain commas)."""
    parts, depth, current = [], 0, ""
    for character in schema:
        depth += character == "<"
        depth -= character == ">"
        if character == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += character
    parts.append(current)
    return [part for part in parts if part.strip()]


def _json(value: Any) -> str:
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"))

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


def _spark_type_sql(field: Any) -> str:
    """Render one Spark StructField's type for an ALTER TABLE ADD COLUMNS clause."""
    return field.dataType.simpleString()


def evolve_delta_schema(spark: Any, frame: Any, table_name: str) -> dict[str, Any]:
    """Add columns the source has and the existing Delta table lacks. Nothing else.

    Delta MERGE does NOT evolve the target schema by default, so merging a source that
    gained a column into an older table fails when the statement references the missing
    target column. Confirmed against the live table: `silver_station_status` was created
    with 27 columns before `is_operational` existed, and the corrected Silver contract has
    28.

    This is deliberately narrow and auditable rather than switching on
    `spark.databricks.delta.schema.autoMerge.enabled`, which would silently absorb ANY
    future drift - including a typo'd or accidentally removed column - across every table
    the session touches. Here each added column is named in the returned report, so a run
    log shows exactly what changed.

    Columns are only ever ADDED. Nothing is dropped, renamed or retyped, so the operation
    cannot destroy existing data.
    """
    quote_qualified_identifier(table_name)
    if not spark.catalog.tableExists(table_name):
        return {"table_existed": False, "added_columns": [], "migrated": False}

    existing = {field.name for field in spark.table(table_name).schema.fields}
    additions = [field for field in frame.schema.fields if field.name not in existing]
    for field in additions:
        _quote_column(field.name)

    if additions:
        clauses = ", ".join(
            f"{_quote_column(field.name)} {_spark_type_sql(field)}" for field in additions
        )
        spark.sql(f"ALTER TABLE {quote_qualified_identifier(table_name)} ADD COLUMNS ({clauses})")
    return {
        "table_existed": True,
        "added_columns": [field.name for field in additions],
        "migrated": bool(additions),
    }


def unassigned_execution_rows(spark: Any, table_name: str, execution_column: str) -> int:
    """Target rows carrying no execution id at all.

    These are invisible to every execution-scoped operation: in SQL, `NULL = 'run-1'`
    evaluates to NULL rather than true, so such a row is never selected by a scope
    predicate and never deleted by one.
    """
    if not spark.catalog.tableExists(table_name):
        return 0
    return spark.table(table_name).where(f"{_quote_column(execution_column)} IS NULL").count()


def backfill_execution_id(
    spark: Any,
    table_name: str,
    *,
    lineage: Any,
    key_columns: Sequence[str],
    execution_column: str,
) -> dict[str, Any]:
    """Assign an execution id to legacy rows that predate the column, using proven lineage.

    This exists because of a concrete live hazard. `gold_rebalancing_priority` was created
    with 746 rows before `execution_id` was part of its projection. Adding the column gives
    every one of those rows NULL, and a NULL can never satisfy an execution-scoped delete
    predicate, so the stale rows would survive the correction *and* the scope check would
    still report success - it compares only the rows that do carry our execution id.

    The backfill is deliberately evidence-driven rather than convenient. Each legacy row
    takes its execution id from a `lineage` frame keyed by the same business grain, so no
    row is assigned to the current run merely because the current run happens to be the one
    executing. Three conditions must hold or nothing is written:

    - the lineage frame carries no NULL execution id, so it cannot launder one NULL into
      another;
    - every lineage key maps to exactly ONE execution id, so an ambiguous key is refused
      instead of arbitrarily resolved;
    - EVERY legacy row is attributable, so a table of unknown provenance stops the run and
      reports the uncertainty instead of being quietly adopted.

    Rows that already carry an execution id are never touched: the `IS NULL` predicate in
    the MATCHED clause means another execution's rows cannot be reassigned.
    """
    quote_qualified_identifier(table_name)
    quoted_execution = _quote_column(execution_column)
    for key in key_columns:
        _quote_column(key)
    if not key_columns:
        raise ValueError("At least one key column is required to attribute legacy rows.")

    if not spark.catalog.tableExists(table_name):
        return {
            "table_existed": False,
            "null_rows_before": 0,
            "attributable_rows": 0,
            "unattributable_rows": 0,
            "backfilled_rows": 0,
            "null_rows_after": 0,
        }

    target_columns = {field.name for field in spark.table(table_name).schema.fields}
    if execution_column not in target_columns:
        raise ValueError(
            f"{table_name} has no {execution_column!r} column yet; run evolve_delta_schema "
            "before attempting to attribute legacy rows."
        )

    null_rows_before = unassigned_execution_rows(spark, table_name, execution_column)
    if not null_rows_before:
        return {
            "table_existed": True,
            "null_rows_before": 0,
            "attributable_rows": 0,
            "unattributable_rows": 0,
            "backfilled_rows": 0,
            "null_rows_after": 0,
        }

    lineage_columns = set(lineage.columns)
    missing = (set(key_columns) | {execution_column}) - lineage_columns
    if missing:
        raise ValueError(f"Lineage frame is missing columns: {sorted(missing)}")

    evidence = lineage.select(*key_columns, execution_column).distinct()
    if evidence.where(f"{quoted_execution} IS NULL").count():
        raise ValueError(
            f"Refusing to attribute legacy rows in {table_name}: the lineage frame itself "
            f"contains rows with a NULL {execution_column}."
        )
    distinct_keys = evidence.select(*key_columns).distinct().count()
    distinct_pairs = evidence.count()
    if distinct_pairs != distinct_keys:
        raise ValueError(
            f"Refusing to attribute legacy rows in {table_name}: {distinct_pairs - distinct_keys} "
            "lineage keys map to more than one execution id, so provenance is ambiguous."
        )

    legacy = spark.table(table_name).where(f"{quoted_execution} IS NULL").select(*key_columns)
    keys = list(key_columns)
    attributable = legacy.join(evidence.select(*key_columns), keys, "inner").count()
    unattributable = legacy.join(evidence.select(*key_columns), keys, "left_anti").count()
    if unattributable:
        raise ValueError(
            f"Refusing to attribute legacy rows in {table_name}: provenance could not be "
            f"established for {unattributable} of {null_rows_before} rows without an "
            f"{execution_column}. Establish their lineage before correcting this table."
        )

    digest = hashlib.sha256(f"{table_name}|lineage".encode("utf-8")).hexdigest()[:12]
    lineage_view = f"urbanflow_lineage_{digest}"
    evidence.createOrReplaceTempView(lineage_view)

    target = quote_qualified_identifier(table_name)
    view = quote_qualified_identifier(lineage_view)
    condition = " AND ".join(f"t.{_quote_column(key)} <=> l.{_quote_column(key)}" for key in keys)
    spark.sql(
        f"MERGE INTO {target} AS t USING {view} AS l ON {condition} "
        f"WHEN MATCHED AND t.{quoted_execution} IS NULL "
        f"THEN UPDATE SET t.{quoted_execution} = l.{quoted_execution}"
    )
    null_rows_after = unassigned_execution_rows(spark, table_name, execution_column)
    return {
        "table_existed": True,
        "null_rows_before": null_rows_before,
        "attributable_rows": attributable,
        "unattributable_rows": 0,
        "backfilled_rows": null_rows_before - null_rows_after,
        "null_rows_after": null_rows_after,
    }


def verify_execution_scope(
    spark: Any,
    frame: Any,
    table_name: str,
    *,
    key_columns: Sequence[str],
    execution_column: str,
    execution_id: str,
) -> dict[str, Any]:
    """Audit the WHOLE table after a scoped replacement, not just the scoped subset.

    `scope_matches_source` on its own is not enough, and the legacy priority table is the
    proof: with 746 rows carrying a NULL execution id, a MERGE updates the 657 that still
    qualify, leaves the other 89 untouched because NULL never matches the scope predicate,
    and the scoped comparison then reports 657 against 657 - a pass, with 89 wrong rows
    still in the table. So this checks membership in BOTH directions and counts the rows
    that belong to no execution at all.
    """
    quote_qualified_identifier(table_name)
    quoted_execution = _quote_column(execution_column)
    if not re.fullmatch(r"[A-Za-z0-9._:+-]+", execution_id):
        raise ValueError(f"Unsafe execution id {execution_id!r}.")
    keys = list(key_columns)

    table = spark.table(table_name)
    scoped = table.where(f"{quoted_execution} = '{execution_id}'")
    scope_rows = scoped.count()
    source_rows = frame.count()
    missing = frame.select(*keys).join(scoped.select(*keys), keys, "left_anti").count()
    unexpected = scoped.select(*keys).join(frame.select(*keys), keys, "left_anti").count()
    null_rows = table.where(f"{quoted_execution} IS NULL").count()
    other_rows = table.where(
        f"{quoted_execution} IS NOT NULL AND {quoted_execution} <> '{execution_id}'"
    ).count()
    passed = missing == 0 and unexpected == 0 and null_rows == 0 and scope_rows == source_rows
    return {
        "total_rows": table.count(),
        "scope_rows": scope_rows,
        "source_rows": source_rows,
        "missing_from_table": missing,
        "unexpected_in_table": unexpected,
        "unassigned_rows": null_rows,
        "other_execution_rows": other_rows,
        "scope_matches_source": scope_rows == source_rows,
        "no_unassigned_rows": null_rows == 0,
        "status": "PASS" if passed else "FAIL",
    }


def replace_execution_scope(
    spark: Any,
    frame: Any,
    table_name: str,
    *,
    key_columns: Sequence[str],
    execution_column: str,
    execution_id: str,
) -> dict[str, Any]:
    """Make one execution's rows in a derived table exactly match the source.

    UPDATE and INSERT alone cannot correct a derived table whose row SET shrank. When a
    station stops qualifying as a shortage, plain MERGE leaves the old row behind forever.
    Confirmed live: `gold_station_shortage` and `gold_rebalancing_priority` each hold 746
    rows, roughly 89 of which are out-of-service stations that the corrected rule no longer
    classifies as actionable.

    The delete is scoped twice over: `WHEN NOT MATCHED BY SOURCE` only considers target
    rows, and the additional `execution_column = execution_id` predicate means rows
    belonging to any other execution can never be touched. The whole thing is one Delta
    MERGE, so it is atomic - there is no window where the table is empty or half-written.

    An EMPTY source is therefore meaningful and supported: it removes every stale
    actionable row for that execution and leaves other executions alone.
    """
    quote_qualified_identifier(table_name)
    columns = tuple(frame.columns)
    if execution_column not in columns:
        raise ValueError(
            f"{execution_column!r} must be part of the source so the delete can be scoped "
            f"to one execution; source has {sorted(columns)}."
        )
    if not re.fullmatch(r"[A-Za-z0-9._:+-]+", execution_id):
        raise ValueError(f"Unsafe execution id {execution_id!r}.")
    missing = set(key_columns) - set(columns)
    if missing:
        raise ValueError(f"MERGE keys are absent from the source frame: {sorted(missing)}")

    source_rows = frame.count()
    unique_keys = frame.select(*key_columns).distinct().count()
    if source_rows != unique_keys:
        raise ValueError(
            f"Refusing to replace {table_name}: {source_rows - unique_keys} duplicate source keys."
        )
    foreign = frame.where(f"{_quote_column(execution_column)} <> '{execution_id}'").count()
    if foreign:
        raise ValueError(
            f"Refusing to replace {table_name}: {foreign} source rows belong to another execution."
        )

    existed = spark.catalog.tableExists(table_name)
    if not existed:
        frame.limit(0).write.format("delta").saveAsTable(table_name)
    rows_before = spark.table(table_name).count()

    # Fail closed on rows that belong to no execution. A NULL execution id cannot satisfy
    # the scope predicate below, so such a row would survive the delete while the scoped
    # comparison still reported success. Refusing here, before anything is written, is the
    # guarantee that a legacy row can never silently escape the correction again.
    unassigned = unassigned_execution_rows(spark, table_name, execution_column)
    if unassigned:
        raise ValueError(
            f"Refusing to replace {table_name}: {unassigned} target rows have no "
            f"{execution_column}, and an execution-scoped delete can never match them. "
            "Attribute them with backfill_execution_id and verified lineage first."
        )

    other_rows_before = (
        spark.table(table_name)
        .where(
            f"{_quote_column(execution_column)} IS NOT NULL "
            f"AND {_quote_column(execution_column)} <> '{execution_id}'"
        )
        .count()
    )
    scope_before = _scope_count(spark, table_name, execution_column, execution_id)
    # Counted before the MERGE so the number is exact rather than inferred from
    # before/after arithmetic, which cannot separate deletes from inserts.
    stale_rows = _stale_scope_count(
        spark, frame, table_name, key_columns, execution_column, execution_id
    )

    digest = hashlib.sha256(f"{table_name}|scope".encode("utf-8")).hexdigest()[:12]
    source_view = f"urbanflow_scope_source_{digest}"
    frame.createOrReplaceTempView(source_view)

    target = quote_qualified_identifier(table_name)
    view = quote_qualified_identifier(source_view)
    quoted_columns = [_quote_column(column) for column in columns]
    quoted_keys = [_quote_column(column) for column in key_columns]
    non_keys = [column for column in quoted_columns if column not in quoted_keys]
    condition = " AND ".join(f"t.{column} <=> s.{column}" for column in quoted_keys)
    update = ", ".join(f"t.{column} = s.{column}" for column in non_keys)
    matched = f"WHEN MATCHED THEN UPDATE SET {update} " if update else ""
    scope = f"t.{_quote_column(execution_column)} = '{execution_id}'"
    spark.sql(
        f"MERGE INTO {target} AS t USING {view} AS s ON {condition} "
        f"{matched}"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(quoted_columns)}) "
        f"VALUES ({', '.join(f's.{c}' for c in quoted_columns)}) "
        f"WHEN NOT MATCHED BY SOURCE AND {scope} THEN DELETE"
    )
    rows_after = spark.table(table_name).count()
    scope_after = _scope_count(spark, table_name, execution_column, execution_id)
    # The whole table is audited, not just our slice, so a row belonging to no execution or
    # a key the corrected source never produced shows up as a FAIL instead of hiding behind
    # a scoped count that happens to agree.
    verification = verify_execution_scope(
        spark,
        frame,
        table_name,
        key_columns=key_columns,
        execution_column=execution_column,
        execution_id=execution_id,
    )
    return {
        "table_existed": existed,
        "source_rows": source_rows,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "scope_rows_before": scope_before,
        "scope_rows_after": scope_after,
        "stale_rows_removed": stale_rows,
        "scope_matches_source": scope_after == source_rows,
        "other_execution_rows_before": other_rows_before,
        "other_execution_rows_after": verification["other_execution_rows"],
        "other_executions_preserved": other_rows_before == verification["other_execution_rows"],
        "verification": verification,
    }


def _scope_count(spark: Any, table_name: str, execution_column: str, execution_id: str) -> int:
    if not spark.catalog.tableExists(table_name):
        return 0
    return (
        spark.table(table_name)
        .where(f"{_quote_column(execution_column)} = '{execution_id}'")
        .count()
    )


def _stale_scope_count(
    spark: Any,
    frame: Any,
    table_name: str,
    key_columns: Sequence[str],
    execution_column: str,
    execution_id: str,
) -> int:
    """Rows already in this execution's scope that the corrected source no longer produces."""
    if not spark.catalog.tableExists(table_name):
        return 0
    scoped = spark.table(table_name).where(f"{_quote_column(execution_column)} = '{execution_id}'")
    return scoped.join(frame.select(*key_columns), list(key_columns), "left_anti").count()


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

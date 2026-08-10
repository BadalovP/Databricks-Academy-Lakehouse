"""Reusable Delta MERGE helpers for Lab 04.

The functions in this module contain no workspace paths or table names. A
notebook supplies those values explicitly, which keeps the merge logic usable
from interactive notebooks, Databricks Jobs, and tests.

The SCD Type 2 helper deliberately materializes its change plan as Delta data
instead of calling DataFrame.persist(). This works on Databricks serverless
compute and prevents recalculation after the first MERGE changes the target.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _active_spark() -> SparkSession:
    """Return the active Spark session."""

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError("An active Spark session is required.")

    return spark


def _column(alias: str, name: str) -> str:
    """Build a safely quoted SQL column reference."""

    escaped_name = name.replace("`", "``")
    return f"{alias}.`{escaped_name}`"


def _merge_condition(keys: Sequence[str]) -> str:
    """Build the equality condition used by Delta MERGE."""

    if not keys:
        raise ValueError("At least one MERGE key is required.")

    return " AND ".join(
        f"{_column('target', key)} = {_column('source', key)}"
        for key in keys
    )


def add_record_hash(
    df: DataFrame,
    tracked_columns: Sequence[str],
    *,
    output_column: str = "source_record_hash",
) -> DataFrame:
    """Attach a deterministic SHA-256 hash for change detection."""

    if not tracked_columns:
        raise ValueError("At least one tracked column is required.")

    missing = sorted(set(tracked_columns) - set(df.columns))

    if missing:
        raise ValueError(
            "Hash columns are missing: " + ", ".join(missing)
        )

    values = [
        F.coalesce(
            F.col(name).cast("string"),
            F.lit("<NULL>"),
        )
        for name in tracked_columns
    ]

    return df.withColumn(
        output_column,
        F.sha2(F.concat_ws("||", *values), 256),
    )


def deduplicate_latest(
    df: DataFrame,
    business_keys: Sequence[str],
    order_columns: Sequence[str],
) -> DataFrame:
    """Keep one deterministic latest record per business key."""

    required = set(business_keys) | set(order_columns)
    missing = sorted(required - set(df.columns))

    if missing:
        raise ValueError(
            "Deduplication columns are missing: "
            + ", ".join(missing)
        )

    if not business_keys or not order_columns:
        raise ValueError(
            "Business keys and ordering columns must not be empty."
        )

    window = Window.partitionBy(*business_keys).orderBy(
        *[
            F.col(name).desc_nulls_last()
            for name in order_columns
        ]
    )

    return (
        df
        .withColumn(
            "_merge_dedup_rank",
            F.row_number().over(window),
        )
        .filter(F.col("_merge_dedup_rank") == 1)
        .drop("_merge_dedup_rank")
    )


def classify_merge_actions(
    source_df: DataFrame,
    target_table: str,
    *,
    business_keys: Sequence[str],
    hash_column: str = "source_record_hash",
) -> DataFrame:
    """Classify source rows as INSERT, UPDATE, or UNCHANGED."""

    spark = _active_spark()

    required = set(business_keys) | {hash_column}
    missing = sorted(required - set(source_df.columns))

    if missing:
        raise ValueError(
            "MERGE classification columns are missing: "
            + ", ".join(missing)
        )

    target_projection = [
        F.col(key).alias(f"_target_{key}")
        for key in business_keys
    ] + [
        F.col(hash_column).alias("_target_record_hash")
    ]

    target_df = spark.table(target_table).select(
        *target_projection
    )

    join_condition = None

    for key in business_keys:
        comparison = (
            F.col(f"source.`{key}`")
            == F.col(f"target.`_target_{key}`")
        )

        join_condition = (
            comparison
            if join_condition is None
            else join_condition & comparison
        )

    return (
        source_df.alias("source")
        .join(
            target_df.alias("target"),
            join_condition,
            "left",
        )
        .select(
            *[
                F.col(f"source.`{name}`").alias(name)
                for name in source_df.columns
            ],
            F.when(
                F.col(
                    f"target.`_target_{business_keys[0]}`"
                ).isNull(),
                F.lit("INSERT"),
            )
            .when(
                ~F.col(
                    f"source.`{hash_column}`"
                ).eqNullSafe(
                    F.col("target._target_record_hash")
                ),
                F.lit("UPDATE"),
            )
            .otherwise(F.lit("UNCHANGED"))
            .alias("merge_action"),
        )
    )


def merge_upsert(
    source_df: DataFrame,
    target_table: str,
    *,
    business_keys: Sequence[str],
    update_columns: Sequence[str] | None = None,
    insert_columns: Sequence[str] | None = None,
    hash_column: str | None = "source_record_hash",
    update_expressions: Mapping[str, str] | None = None,
    insert_expressions: Mapping[str, str] | None = None,
) -> None:
    """Run an idempotent Delta upsert.

    Existing rows are updated only when hash_column changes.
    Set hash_column=None when every matched row should be updated.
    """

    update_names = list(
        update_columns or source_df.columns
    )
    insert_names = list(
        insert_columns or source_df.columns
    )

    required = (
        set(business_keys)
        | set(update_names)
        | set(insert_names)
    )

    if hash_column:
        required.add(hash_column)

    missing = sorted(required - set(source_df.columns))

    if missing:
        raise ValueError(
            "MERGE source columns are missing: "
            + ", ".join(missing)
        )

    update_values = {
        name: _column("source", name)
        for name in update_names
    }

    insert_values = {
        name: _column("source", name)
        for name in insert_names
    }

    update_values.update(update_expressions or {})
    insert_values.update(insert_expressions or {})

    builder = (
        DeltaTable
        .forName(_active_spark(), target_table)
        .alias("target")
        .merge(
            source_df.alias("source"),
            _merge_condition(business_keys),
        )
    )

    if hash_column:
        change_condition = (
            f"NOT ({_column('target', hash_column)} <=> "
            f"{_column('source', hash_column)})"
        )

        builder = builder.whenMatchedUpdate(
            condition=change_condition,
            set=update_values,
        )
    else:
        builder = builder.whenMatchedUpdate(
            set=update_values
        )

    (
        builder
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
def merge_scd_type1(
    source_df: DataFrame,
    target_table: str,
    *,
    business_key: str,
    mutable_columns: Sequence[str],
    insert_columns: Sequence[str] | None = None,
    hash_column: str = "source_record_hash",
    updated_at_column: str = "updated_at",
) -> None:
    """Apply SCD Type 1 by overwriting changed attributes."""

    update_expressions = {
        updated_at_column: "current_timestamp()"
    }

    insert_expressions = {
        updated_at_column: "current_timestamp()"
    }

    merge_upsert(
        source_df,
        target_table,
        business_keys=[business_key],
        update_columns=list(mutable_columns) + [hash_column],
        insert_columns=insert_columns,
        hash_column=hash_column,
        update_expressions=update_expressions,
        insert_expressions=insert_expressions,
    )


def classify_scd2_changes(
    source_df: DataFrame,
    target_table: str,
    *,
    business_key: str,
    hash_column: str = "source_record_hash",
    version_column: str = "version_number",
    current_column: str = "is_current",
    effective_from_column: str = "effective_from",
) -> DataFrame:
    """Attach SCD Type 2 action and prior-version metadata."""

    spark = _active_spark()

    required = {
        business_key,
        hash_column,
    }

    missing = sorted(required - set(source_df.columns))

    if missing:
        raise ValueError(
            "SCD2 source columns are missing: "
            + ", ".join(missing)
        )

    current_target_df = (
        spark.table(target_table)
        .filter(F.col(current_column))
        .select(
            F.col(business_key).alias(
                "_target_business_key"
            ),
            F.col(hash_column).alias(
                "_target_record_hash"
            ),
            F.col(version_column).alias(
                "previous_version_number"
            ),
            F.col(effective_from_column).alias(
                "previous_effective_from"
            ),
        )
    )

    return (
        source_df.alias("source")
        .join(
            current_target_df.alias("target"),
            (
                F.col(f"source.`{business_key}`")
                == F.col("target._target_business_key")
            ),
            "left",
        )
        .select(
            *[
                F.col(f"source.`{name}`").alias(name)
                for name in source_df.columns
            ],
            F.col("target.previous_version_number"),
            F.col("target.previous_effective_from"),
            F.when(
                F.col(
                    "target._target_business_key"
                ).isNull(),
                F.lit("INSERT"),
            )
            .when(
                ~F.col(
                    f"source.`{hash_column}`"
                ).eqNullSafe(
                    F.col("target._target_record_hash")
                ),
                F.lit("UPDATE"),
            )
            .otherwise(F.lit("UNCHANGED"))
            .alias("merge_action"),
        )
    )


def apply_scd_type2_plan(
    classified_df: DataFrame,
    target_table: str,
    *,
    plan_path: str,
    business_key: str,
    effective_at_column: str,
    insert_values: Mapping[str, str],
    hash_column: str = "source_record_hash",
    version_column: str = "version_number",
    current_column: str = "is_current",
    effective_from_column: str = "effective_from",
    effective_to_column: str = "effective_to",
    updated_at_column: str = "updated_at",
) -> dict[str, int]:
    """Close changed rows and insert new SCD2 versions.

    The change plan is written to a Delta path before either MERGE.
    This makes the two-step operation compatible with serverless
    compute and prevents the input from being recalculated.
    """

    required = {
        business_key,
        hash_column,
        effective_at_column,
        "merge_action",
        "previous_version_number",
    }

    missing = sorted(
        required - set(classified_df.columns)
    )

    if missing:
        raise ValueError(
            "SCD2 plan columns are missing: "
            + ", ".join(missing)
        )

    (
        classified_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(plan_path)
    )

    frozen_plan_df = (
        _active_spark()
        .read
        .format("delta")
        .load(plan_path)
    )

    changed_df = frozen_plan_df.filter(
        F.col("merge_action") == "UPDATE"
    )

    new_versions_df = (
        frozen_plan_df
        .filter(
            F.col("merge_action").isin(
                "INSERT",
                "UPDATE",
            )
        )
        .withColumn(
            version_column,
            (
                F.coalesce(
                    F.col("previous_version_number"),
                    F.lit(0),
                )
                + 1
            ).cast("int"),
        )
        .withColumn(
            effective_from_column,
            F.col(effective_at_column),
        )
    )

    closed_rows = changed_df.count()
    inserted_rows = new_versions_df.count()

    current_match = (
        f"{_column('target', business_key)} = "
        f"{_column('source', business_key)} "
        f"AND {_column('target', current_column)} = true"
    )

    changed_condition = (
        f"NOT ({_column('target', hash_column)} <=> "
        f"{_column('source', hash_column)})"
    )

    (
        DeltaTable
        .forName(_active_spark(), target_table)
        .alias("target")
        .merge(
            changed_df.alias("source"),
            current_match,
        )
        .whenMatchedUpdate(
            condition=changed_condition,
            set={
                effective_to_column: _column(
                    "source",
                    effective_at_column,
                ),
                current_column: "false",
                updated_at_column: "current_timestamp()",
            },
        )
        .execute()
    )

    values = dict(insert_values)

    values.setdefault(
        version_column,
        _column("source", version_column),
    )

    values.setdefault(
        effective_from_column,
        _column("source", effective_from_column),
    )

    values.setdefault(
        effective_to_column,
        "CAST(NULL AS TIMESTAMP)",
    )

    values.setdefault(
        current_column,
        "true",
    )

    values.setdefault(
        updated_at_column,
        "current_timestamp()",
    )

    (
        DeltaTable
        .forName(_active_spark(), target_table)
        .alias("target")
        .merge(
            new_versions_df.alias("source"),
            current_match,
        )
        .whenNotMatchedInsert(values=values)
        .execute()
    )

    return {
        "closed_rows": closed_rows,
        "inserted_rows": inserted_rows,
    }


def latest_merge_metrics(
    target_table: str,
) -> dict[str, Any]:
    """Return metrics from the latest Delta table commit."""

    row = (
        _active_spark()
        .sql(
            f"DESCRIBE HISTORY {target_table} LIMIT 1"
        )
        .select(
            "operation",
            "operationMetrics",
        )
        .first()
    )

    return {
        "operation": row["operation"],
        **dict(row["operationMetrics"] or {}),
    }    
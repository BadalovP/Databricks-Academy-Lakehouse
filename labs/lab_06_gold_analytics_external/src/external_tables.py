"""Helpers for writing and registering shared external Delta tables."""

from delta.tables import DeltaTable


def ensure_target_schema(spark, config) -> None:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config.target_schema_fqn}"
    )


def _quoted_path(path: str) -> str:
    return path.replace("'", "''")


def describe_table_location(spark, table_name: str) -> str:
    row = (
        spark.sql(f"DESCRIBE DETAIL {table_name}")
        .select("location")
        .first()
    )
    return str(row["location"]).rstrip("/")


def write_external_delta(
    spark,
    df,
    config,
    table_name: str,
) -> str:
    """Overwrite one shared Delta path and register it in the current workspace."""
    ensure_target_schema(spark, config)

    short_name = config.short_name(table_name)
    path = config.table_path(short_name)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )

    escaped = _quoted_path(path)

    if not spark.catalog.tableExists(table_name):
        spark.sql(
            f"CREATE TABLE {table_name} "
            f"USING DELTA LOCATION '{escaped}'"
        )

    actual = describe_table_location(spark, table_name)
    expected = path.rstrip("/")

    if actual != expected:
        raise RuntimeError(
            f"{table_name} is registered at {actual}, "
            f"but External V2 expects {expected}. "
            "Use the dedicated V2 schema or fix the registration."
        )

    spark.sql(f"REFRESH TABLE {table_name}")
    return path


def register_external_delta(
    spark,
    config,
    short_name: str,
) -> tuple[str, str, int]:
    """Register an existing Delta path without rebuilding its data."""
    ensure_target_schema(spark, config)

    table_name = config.table(short_name)
    path = config.table_path(short_name)

    if not DeltaTable.isDeltaTable(spark, path):
        raise FileNotFoundError(
            f"Shared Delta table does not exist at: {path}. "
            "Build External V2 in the source workspace first."
        )

    escaped = _quoted_path(path)

    if not spark.catalog.tableExists(table_name):
        spark.sql(
            f"CREATE TABLE {table_name} "
            f"USING DELTA LOCATION '{escaped}'"
        )

    actual = describe_table_location(spark, table_name)
    expected = path.rstrip("/")

    if actual != expected:
        raise RuntimeError(
            f"Existing table {table_name} points to {actual}; "
            f"expected {expected}."
        )

    spark.sql(f"REFRESH TABLE {table_name}")
    row_count = spark.table(table_name).count()

    return table_name, expected, row_count

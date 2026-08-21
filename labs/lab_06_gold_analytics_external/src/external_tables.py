"""Serverless-safe external Delta table helpers for Lab 06 External V2.

Important:
- No REFRESH TABLE
- No CACHE TABLE / UNCACHE TABLE
- Writes Delta files to an explicit external location
- Registers Unity Catalog tables with USING DELTA LOCATION
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def normalize_location(value: str) -> str:
    """Normalize a storage location for safe equality checks."""
    return str(value).strip().rstrip("/").lower()


def registered_table_location(
    spark: SparkSession,
    table_name: str,
) -> Optional[str]:
    """Return the registered Delta location, or None when table is absent."""
    if not spark.catalog.tableExists(table_name):
        return None

    row = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    if row is None:
        return None

    return row.asDict().get("location")


def validate_registered_location(
    spark: SparkSession,
    table_name: str,
    expected_location: str,
) -> str:
    """Raise when a table is not registered at the expected external location."""
    actual_location = registered_table_location(spark, table_name)

    if actual_location is None:
        raise RuntimeError(
            f"{table_name} is not registered in Unity Catalog."
        )

    if normalize_location(actual_location) != normalize_location(expected_location):
        raise RuntimeError(
            f"{table_name} location mismatch. "
            f"Expected={expected_location}; actual={actual_location}"
        )

    return actual_location


def register_external_delta_table(
    spark: SparkSession,
    table_name: str,
    location: str,
) -> None:
    """Register an existing Delta location without any cache refresh command."""
    existing_location = registered_table_location(spark, table_name)

    if existing_location is not None:
        if normalize_location(existing_location) != normalize_location(location):
            raise RuntimeError(
                f"{table_name} is already registered at {existing_location}, "
                f"but this run expects {location}."
            )
        return

    escaped_location = location.replace("'", "''")

    spark.sql(
        f"""
        CREATE TABLE {table_name}
        USING DELTA
        LOCATION '{escaped_location}'
        """
    )

    validate_registered_location(
        spark,
        table_name,
        location,
    )


def overwrite_external_delta(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    location: str,
) -> None:
    """Overwrite Delta data and register the external table safely on Serverless."""
    existing_location = registered_table_location(spark, table_name)

    if existing_location is not None:
        if normalize_location(existing_location) != normalize_location(location):
            raise RuntimeError(
                f"{table_name} is already registered at {existing_location}, "
                f"but this run expects {location}."
            )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(location)
    )

    register_external_delta_table(
        spark,
        table_name,
        location,
    )

    # Deliberately no REFRESH TABLE. Serverless does not support it.
    validate_registered_location(
        spark,
        table_name,
        location,
    )


# Friendly aliases for compatibility with earlier Lab 06 drafts.
write_external_delta = overwrite_external_delta
ensure_external_table = register_external_delta_table
get_registered_table_location = registered_table_location

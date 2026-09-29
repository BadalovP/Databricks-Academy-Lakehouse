"""Physical Bronze-to-Silver layer: explicit contract, quarantine, dedup, MERGE.

This is the Spark/Delta counterpart to `medallion.py`. `medallion.py` holds the same
rules as pure Python so they can be unit tested without a cluster; this module applies
them to real DataFrames and persists the result.

The split is deliberate: the business rules live in one place conceptually, but a rule
expressed as a Spark `Column` cannot be unit tested without a session, so the pure
version stays the reference implementation and the Spark version is verified against
real local Spark in `tests/test_silver.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# The Silver contract. Anything not listed here does not reach Silver, which is what
# makes the layer a contract rather than a filtered copy of Bronze.
SILVER_COLUMNS: tuple[str, ...] = (
    "event_id",
    "execution_id",
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "num_bikes_disabled",
    "num_docks_disabled",
    "num_ebikes_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
    "source_last_updated",
    "collected_at",
    "observed_at",
    "capacity_estimate",
    "availability_status",
    "is_low_bikes",
    "is_low_docks",
    "passed_contract",
    "source_url",
    "gbfs_version",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
)

# A row failing any of these is quarantined rather than silently dropped, so the
# reconciliation bronze == silver + quarantine + duplicates always holds.
CONTRACT_RULES: tuple[str, ...] = (
    "MISSING_EVENT_ID",
    "MISSING_STATION_ID",
    "PARSE_ERROR",
    "MISSING_AVAILABILITY",
    "NEGATIVE_AVAILABILITY",
    "INVALID_STATION_STATE",
    "MISSING_SOURCE_TIMESTAMP",
    "UNKNOWN_STATION_ID",
)


@dataclass(frozen=True)
class SilverSplit:
    """The two persisted outputs plus the duplicates that were removed."""

    silver: Any
    quarantine: Any
    duplicates: Any


def _contract_violations(*, check_known_station: bool = False) -> Any:
    """Build the array of rule names each row violates, empty when the row is valid."""
    from pyspark.sql import functions as F

    checks = [
        (F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""), "MISSING_EVENT_ID"),
        (F.col("station_id").isNull() | (F.trim(F.col("station_id")) == ""), "MISSING_STATION_ID"),
        (F.coalesce(F.col("parse_error"), F.lit(False)), "PARSE_ERROR"),
        (
            F.col("num_bikes_available").isNull()
            | F.col("num_docks_available").isNull()
            | F.col("num_bikes_disabled").isNull()
            | F.col("num_docks_disabled").isNull()
            | F.col("num_ebikes_available").isNull(),
            "MISSING_AVAILABILITY",
        ),
        (
            (F.col("num_bikes_available") < 0)
            | (F.col("num_docks_available") < 0)
            | (F.col("num_bikes_disabled") < 0)
            | (F.col("num_docks_disabled") < 0)
            | (F.col("num_ebikes_available") < 0),
            "NEGATIVE_AVAILABILITY",
        ),
        (
            ~F.coalesce(F.col("is_installed").isin(0, 1), F.lit(False))
            | ~F.coalesce(F.col("is_renting").isin(0, 1), F.lit(False))
            | ~F.coalesce(F.col("is_returning").isin(0, 1), F.lit(False)),
            "INVALID_STATION_STATE",
        ),
        (
            F.col("source_last_updated").isNull() | (F.col("source_last_updated") <= 0),
            "MISSING_SOURCE_TIMESTAMP",
        ),
    ]
    if check_known_station:
        checks.append(
            (
                F.col("station_id").isNotNull()
                & (F.trim(F.col("station_id")) != "")
                & F.col("_known_station_id").isNull(),
                "UNKNOWN_STATION_ID",
            )
        )
    flags = [F.when(condition, F.lit(name)) for condition, name in checks]
    return F.array_except(F.array(*flags), F.array(F.lit(None).cast("string")))


def add_contract_result(
    bronze: Any,
    *,
    known_station_ids: Any | None = None,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> Any:
    """Attach contract violations, derived timestamps and the availability classification.

    Kept separate from the split so a notebook can show the annotated rows before
    anything is routed, which makes the quarantine decision inspectable rather than
    implicit.
    """
    from pyspark.sql import functions as F

    annotated = bronze
    if known_station_ids is not None:
        known = known_station_ids.select(
            F.col("station_id").alias("_known_station_id")
        ).dropDuplicates(["_known_station_id"])
        annotated = annotated.join(
            F.broadcast(known),
            annotated["station_id"] == known["_known_station_id"],
            "left",
        )
    annotated = annotated.withColumn(
        "failed_rules",
        _contract_violations(check_known_station=known_station_ids is not None),
    )
    annotated = annotated.withColumn("passed_contract", F.size("failed_rules") == 0)

    # The feed's own reading time, as a real timestamp rather than an epoch integer.
    annotated = annotated.withColumn(
        "observed_at", F.timestamp_seconds(F.col("source_last_updated"))
    )
    # Docks plus bikes is the closest capacity estimate available from status alone;
    # station_information carries the authoritative capacity and is joined in Gold.
    annotated = annotated.withColumn(
        "capacity_estimate",
        F.coalesce(F.col("num_bikes_available"), F.lit(0))
        + F.coalesce(F.col("num_docks_available"), F.lit(0))
        + F.coalesce(F.col("num_bikes_disabled"), F.lit(0))
        + F.coalesce(F.col("num_docks_disabled"), F.lit(0)),
    )
    annotated = annotated.withColumn(
        "is_low_bikes", F.col("num_bikes_available") <= F.lit(low_bike_threshold)
    )
    annotated = annotated.withColumn(
        "is_low_docks", F.col("num_docks_available") <= F.lit(low_dock_threshold)
    )
    annotated = annotated.withColumn(
        "availability_status",
        F.when(F.col("is_low_bikes") & F.col("is_low_docks"), F.lit("LOW_BIKES_AND_DOCKS"))
        .when(F.col("is_low_bikes"), F.lit("LOW_BIKES"))
        .when(F.col("is_low_docks"), F.lit("LOW_DOCKS"))
        .otherwise(F.lit("HEALTHY")),
    )
    return annotated.drop("_known_station_id")


def split_silver_and_quarantine(
    bronze: Any,
    *,
    known_station_ids: Any | None = None,
    low_bike_threshold: int = 2,
    low_dock_threshold: int = 2,
) -> SilverSplit:
    """Route Bronze rows into Silver, Quarantine and removed duplicates.

    First arrival wins for a repeated event_id, because a repeat is a redelivery of the
    same observation rather than a new reading. The later copies are returned instead of
    dropped so the row counts still add up.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    annotated = add_contract_result(
        bronze,
        known_station_ids=known_station_ids,
        low_bike_threshold=low_bike_threshold,
        low_dock_threshold=low_dock_threshold,
    )
    quarantine = annotated.where(~F.col("passed_contract"))
    valid = annotated.where(F.col("passed_contract"))

    # Deterministic tie-break so a rerun on the same data makes the same choice:
    # earliest Kafka offset is the true first arrival.
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("ingested_at").asc_nulls_last(),
        F.col("raw_json").asc_nulls_last(),
    )
    ranked = valid.withColumn("_arrival", F.row_number().over(ordering))
    silver = ranked.where(F.col("_arrival") == 1).drop("_arrival")
    duplicates = ranked.where(F.col("_arrival") > 1).drop("_arrival")

    return SilverSplit(
        silver=silver.select(*SILVER_COLUMNS),
        quarantine=quarantine,
        duplicates=duplicates,
    )


def merge_silver(
    spark: Any, silver: Any, table_name: str, *, key: str = "event_id"
) -> dict[str, int | bool]:
    """Idempotently MERGE Silver rows into the target Delta table by event_id.

    MERGE rather than append is what makes a rerun safe: replaying the same bounded
    snapshot updates the matching rows instead of doubling the table. The table is
    created on first use so the notebook needs no separate DDL step.

    Returns the row counts before and after, so a caller can prove idempotency by
    running it twice and seeing `after` stay equal.
    """
    if not table_name.strip():
        raise ValueError("table_name must be non-empty.")
    if key not in SILVER_COLUMNS:
        raise ValueError(f"{key!r} is not part of the Silver contract.")

    from urbanflow.persistence import merge_delta_table

    return merge_delta_table(spark, silver, table_name, key_columns=(key,))


def persist_silver_outputs(
    spark: Any,
    split: SilverSplit,
    *,
    silver_table: str,
    quarantine_table: str,
    duplicate_table: str,
) -> dict[str, dict[str, int | bool]]:
    """Persist all three reconciled Silver outcomes with idempotent Delta MERGEs."""
    from urbanflow.persistence import merge_delta_table

    lineage_key = ("kafka_topic", "kafka_partition", "kafka_offset")
    return {
        "silver": merge_delta_table(spark, split.silver, silver_table, key_columns=("event_id",)),
        "quarantine": merge_delta_table(
            spark, split.quarantine, quarantine_table, key_columns=lineage_key
        ),
        "duplicates": merge_delta_table(
            spark, split.duplicates, duplicate_table, key_columns=lineage_key
        ),
    }


def freshness(
    silver: Any, *, now_epoch_seconds: int, max_age_seconds: int = 3600
) -> dict[str, Any]:
    """Report how stale the newest observation is, without failing the pipeline itself.

    Freshness is reported rather than enforced because a stale feed is an operational
    fact about Citi Bike, not a defect in this code; the caller decides what to do.
    """
    from pyspark.sql import functions as F

    row = silver.select(
        F.min("source_last_updated").alias("oldest"),
        F.max("source_last_updated").alias("newest"),
        F.count("*").alias("rows"),
    ).collect()[0]
    newest = row["newest"]
    age = None if newest is None else int(now_epoch_seconds) - int(newest)
    return {
        "rows": int(row["rows"]),
        "oldest_source_last_updated": None if row["oldest"] is None else int(row["oldest"]),
        "newest_source_last_updated": None if newest is None else int(newest),
        "age_seconds": age,
        "max_age_seconds": int(max_age_seconds),
        "is_fresh": age is not None and 0 <= age <= int(max_age_seconds),
    }


def reconcile_silver(bronze: Any, split: SilverSplit) -> dict[str, Any]:
    """Prove every Bronze row was accounted for, and that Silver event IDs are unique.

    A count match alone is not enough: the same total could hide a row moving from
    Silver to Quarantine, so the distinct event-ID count is checked too.
    """
    bronze_rows = bronze.count()
    silver_rows = split.silver.count()
    quarantine_rows = split.quarantine.count()
    duplicate_rows = split.duplicates.count()
    distinct_silver_ids = split.silver.select("event_id").distinct().count()
    accounted = silver_rows + quarantine_rows + duplicate_rows
    return {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "duplicate_rows": duplicate_rows,
        "accounted_rows": accounted,
        "distinct_silver_event_ids": distinct_silver_ids,
        "counts_reconcile": accounted == bronze_rows,
        "silver_ids_unique": distinct_silver_ids == silver_rows,
        "status": (
            "PASS" if accounted == bronze_rows and distinct_silver_ids == silver_rows else "FAIL"
        ),
    }

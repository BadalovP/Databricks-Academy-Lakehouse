"""Auto Loader ingestion for official Citi Bike historical trip archives.

The one detail that matters most here is the join key. Historical trip CSVs put values
like "7407.13" in `start_station_id`. That is NOT the current GBFS `station_id`, which is
a UUID such as "66dd9fa6-0aca-11e7-82f6-3863bb44ef7c"; it is the GBFS `short_name`.
Joining trips straight onto `station_id` silently matches nothing and produces an empty
result that looks like a data problem rather than a join bug, so
`join_trips_to_stations` joins on `short_name` and reports the match rate.

Verified against the real archive (see data/samples/historical_trips_202401_sample.metadata.json):
all 40 sampled rows matched a current GBFS short_name.
"""

from __future__ import annotations

from typing import Any

from urbanflow.reporting import sanitize_path_token

# Rescued data goes in its own column rather than failing the read, so a column the
# official archive adds later is captured instead of discarded. This is the Lab 3
# schema-evolution and rescued-data requirement.
RESCUED_COLUMN = "_rescued_data"
SCHEMA_EVOLUTION_MODES = frozenset({"rescue", "addNewColumns", "failOnNewColumns", "none"})
MIN_TRIP_SECONDS = 60
MAX_TRIP_HOURS = 24

TRIP_COLUMNS: tuple[str, ...] = (
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
)


def trip_schema() -> Any:
    """Explicit trip contract, so Auto Loader never has to guess a type.

    Station IDs are typed as strings on purpose. "7407.13" looks numeric and would be
    inferred as a double, which silently destroys the trailing-zero forms and breaks the
    join to GBFS short_name.
    """
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("ride_id", T.StringType(), True),
            T.StructField("rideable_type", T.StringType(), True),
            T.StructField("started_at", T.TimestampType(), True),
            T.StructField("ended_at", T.TimestampType(), True),
            T.StructField("start_station_name", T.StringType(), True),
            T.StructField("start_station_id", T.StringType(), True),
            T.StructField("end_station_name", T.StringType(), True),
            T.StructField("end_station_id", T.StringType(), True),
            T.StructField("start_lat", T.DoubleType(), True),
            T.StructField("start_lng", T.DoubleType(), True),
            T.StructField("end_lat", T.DoubleType(), True),
            T.StructField("end_lng", T.DoubleType(), True),
            T.StructField("member_casual", T.StringType(), True),
        ]
    )


def autoloader_options(
    *,
    schema_location: str,
    rescue_column: str = RESCUED_COLUMN,
    schema_evolution_mode: str = "rescue",
    explicit_schema: bool = True,
) -> dict[str, str]:
    """Auto Loader options for the historical trip CSVs.

    ``cloudFiles.schemaLocation`` is separate from the streaming checkpoint.  Databricks
    does not allow ``addNewColumns`` when ``DataStreamReader.schema`` supplies the schema,
    so the production contract uses ``rescue``.  The separate educational evolution
    reader below uses schema hints and ``addNewColumns`` without calling ``.schema()``.
    """
    if not schema_location.strip():
        raise ValueError("schema_location must be non-empty; it is the schema checkpoint.")
    if schema_evolution_mode not in SCHEMA_EVOLUTION_MODES:
        raise ValueError(f"Unsupported Auto Loader schema evolution mode: {schema_evolution_mode}")
    if explicit_schema and schema_evolution_mode == "addNewColumns":
        raise ValueError(
            "Auto Loader does not allow addNewColumns with an explicit reader schema; "
            "use schema hints for the evolution demonstration."
        )
    options = {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.rescuedDataColumn": rescue_column,
        "cloudFiles.inferColumnTypes": "false",
        "header": "true",
        "cloudFiles.schemaEvolutionMode": schema_evolution_mode,
    }
    return options


def read_trips_autoloader(spark: Any, path: str, *, schema_location: str) -> Any:
    """Build the fixed-contract Auto Loader stream with unexpected fields rescued."""
    if not path.strip():
        raise ValueError("path must be non-empty.")
    return (
        spark.readStream.format("cloudFiles")
        .options(
            **autoloader_options(
                schema_location=schema_location,
                schema_evolution_mode="rescue",
                explicit_schema=True,
            )
        )
        .schema(trip_schema())
        .load(path)
    )


def trip_schema_hints() -> str:
    """Known types for the additive-evolution lesson; station identifiers stay strings."""
    return (
        "ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP, "
        "start_station_name STRING, start_station_id STRING, end_station_name STRING, "
        "end_station_id STRING, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, "
        "end_lng DOUBLE, member_casual STRING"
    )


def read_trips_autoloader_with_evolution(spark: Any, path: str, *, schema_location: str) -> Any:
    """Build the educational additive-evolution reader without starting a query.

    Schema hints preserve known types while leaving Auto Loader in charge of adding new
    columns.  Auto Loader intentionally stops once on a new column; a Job retry or manual
    rerun then reads with the widened schema.
    """
    if not path.strip():
        raise ValueError("path must be non-empty.")
    options = autoloader_options(
        schema_location=schema_location,
        schema_evolution_mode="addNewColumns",
        explicit_schema=False,
    )
    options["cloudFiles.schemaHints"] = trip_schema_hints()
    return spark.readStream.format("cloudFiles").options(**options).load(path)


def trip_duration_seconds() -> Any:
    """Trip length in seconds, null when either endpoint is missing."""
    from pyspark.sql import functions as F

    return F.col("ended_at").cast("long") - F.col("started_at").cast("long")


def validate_historical_trips(
    trips: Any,
    *,
    min_trip_seconds: int = MIN_TRIP_SECONDS,
    max_trip_hours: int = MAX_TRIP_HOURS,
) -> Any:
    """Attach explicit quality failures while preserving every input row.

    The duration bounds are not arbitrary. Citi Bike's own documentation says trips under a
    minute are usually false starts where a rider undocks and immediately redocks, and a trip
    running longer than a day is almost always an unreturned bike rather than a real ride.
    Both are flagged rather than deleted, so the counts stay auditable; `INVALID_TRIP_INTERVAL`
    already covers a missing or reversed interval, and the duration rules deliberately fire
    only when the interval itself is sound, so one bad row raises one finding and not three.
    """
    from pyspark.sql import functions as F

    if min_trip_seconds < 0:
        raise ValueError("min_trip_seconds must not be negative.")
    if max_trip_hours <= 0:
        raise ValueError("max_trip_hours must be positive.")

    duration = trip_duration_seconds()
    sound_interval = (
        F.col("started_at").isNotNull()
        & F.col("ended_at").isNotNull()
        & (F.col("ended_at") >= F.col("started_at"))
    )
    checks = [
        (F.col("ride_id").isNull() | (F.trim("ride_id") == ""), "MISSING_RIDE_ID"),
        (
            F.col("start_station_id").isNull() | (F.trim("start_station_id") == ""),
            "MISSING_START_STATION_ID",
        ),
        (
            F.col("started_at").isNull()
            | F.col("ended_at").isNull()
            | (F.col("ended_at") < F.col("started_at")),
            "INVALID_TRIP_INTERVAL",
        ),
        (
            ~F.coalesce(F.col("member_casual").isin("member", "casual"), F.lit(False)),
            "INVALID_RIDER_TYPE",
        ),
        (sound_interval & (duration < F.lit(min_trip_seconds)), "TRIP_TOO_SHORT"),
        (sound_interval & (duration > F.lit(max_trip_hours * 3600)), "TRIP_TOO_LONG"),
    ]
    if RESCUED_COLUMN in trips.columns:
        checks.append((F.col(RESCUED_COLUMN).isNotNull(), "RESCUED_DATA_PRESENT"))
    failures = F.array_except(
        F.array(*[F.when(condition, F.lit(name)) for condition, name in checks]),
        F.array(F.lit(None).cast("string")),
    )
    return trips.withColumn("failed_rules", failures).withColumn(
        "passed_contract", F.size("failed_rules") == 0
    )


def historical_quality_report(validated: Any) -> dict[str, Any]:
    """Return bounded counts for valid, rejected, rescued, and duplicate trip IDs."""
    from pyspark.sql import functions as F

    row = validated.select(
        F.count("*").alias("rows"),
        F.sum(F.col("passed_contract").cast("int")).alias("valid"),
        F.sum((~F.col("passed_contract")).cast("int")).alias("rejected"),
        F.sum((F.col("ride_id").isNotNull() & (F.trim(F.col("ride_id")) != "")).cast("int")).alias(
            "nonblank_ride_ids"
        ),
        F.countDistinct(
            F.when(
                F.col("ride_id").isNotNull() & (F.trim(F.col("ride_id")) != ""),
                F.col("ride_id"),
            )
        ).alias("distinct_ride_ids"),
        F.sum(F.array_contains("failed_rules", "RESCUED_DATA_PRESENT").cast("int")).alias(
            "rescued"
        ),
    ).collect()[0]
    total = int(row["rows"])
    distinct = int(row["distinct_ride_ids"])
    nonblank = int(row["nonblank_ride_ids"] or 0)
    return {
        "rows": total,
        "valid_rows": int(row["valid"] or 0),
        "rejected_rows": int(row["rejected"] or 0),
        "rescued_rows": int(row["rescued"] or 0),
        "duplicate_ride_ids": nonblank - distinct,
        "counts_reconcile": int(row["valid"] or 0) + int(row["rejected"] or 0) == total,
    }


def start_historical_available_now(
    trips: Any,
    *,
    table_name: str,
    checkpoint_location: str,
) -> Any:
    """Start one bounded Auto Loader write. Call only after explicit live approval."""
    from urbanflow.persistence import quote_qualified_identifier

    quote_qualified_identifier(table_name)
    if not checkpoint_location.strip():
        raise ValueError("checkpoint_location must be non-empty.")
    return (
        trips.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(table_name)
    )


def join_trips_to_stations(trips: Any, dimension: Any) -> Any:
    """Join historical trips to the current station dimension via short_name.

    The join is `trips.start_station_id == dimension.station_short_name`, which is the
    correct correspondence. A left join keeps unmatched trips so the match rate stays
    measurable: stations are renamed and retired over time, so a real archive legitimately
    contains stations that no longer exist in the current feed, and dropping them would
    hide that rather than report it.
    """
    from pyspark.sql import functions as F

    right = dimension.select(
        F.col("station_short_name").alias("_join_short_name"),
        F.col("station_id").alias("start_station_uuid"),
        F.col("station_name").alias("current_station_name"),
    )
    joined = trips.join(right, trips["start_station_id"] == right["_join_short_name"], "left").drop(
        "_join_short_name"
    )
    return joined.withColumn("station_matched", F.col("start_station_uuid").isNotNull())


def trip_join_match_rate(joined: Any) -> dict[str, Any]:
    """Report how many trips found a current station, so a bad join cannot pass unnoticed.

    A zero match rate is the signature of joining on the wrong key, which is exactly the
    mistake this module exists to prevent; reporting the rate makes that failure loud.
    """
    from pyspark.sql import functions as F

    row = joined.select(
        F.count("*").alias("trips"),
        F.sum(F.col("station_matched").cast("int")).alias("matched"),
    ).collect()[0]
    trips = int(row["trips"])
    matched = int(row["matched"] or 0)
    return {
        "trips": trips,
        "matched_to_current_station": matched,
        "unmatched": trips - matched,
        "match_rate": round(matched / trips, 4) if trips else 0.0,
    }


def historical_landing_paths(
    volume_root: str, *, execution_id: str, landing_subdir: str = ""
) -> dict[str, str]:
    """Landing, schema and checkpoint locations inside the EXISTING UrbanFlow Volume.

    No new storage account, container or external location is involved - these are
    subdirectories of the managed Volume this project already created. The schema location
    and the streaming checkpoint are deliberately separate directories: Auto Loader keeps the
    inferred schema in one and Structured Streaming keeps offsets in the other, and sharing a
    path between them corrupts both.

    `landing_subdir` isolates one source set from another. It is optional so the already-validated
    40-row sample keeps its original landing, schema and checkpoint paths. A full archive supplies
    a value such as `202401-full`, which gives it a separate landing directory, Auto Loader schema
    location and checkpoint namespace without moving or re-reading the sample.
    """
    if not volume_root.strip():
        raise ValueError("volume_root must be non-empty.")
    execution = sanitize_path_token(execution_id, label="execution_id")
    subdir = str(landing_subdir).strip()
    if subdir:
        subdir = sanitize_path_token(subdir, label="landing_subdir")
    root = volume_root.rstrip("/")
    source_namespace = "historical_trips" + (f"/{subdir}" if subdir else "")
    return {
        "landing": f"{root}/landing/{source_namespace}",
        "schema": f"{root}/schemas/{source_namespace}",
        "checkpoint": f"{root}/checkpoints/{source_namespace}/{execution}",
        "archive": f"{root}/landing/{source_namespace}/_archive",
    }


def with_trip_lineage(trips: Any, *, execution_id: str, include_source_file: bool = True) -> Any:
    """Stamp each trip with the execution that loaded it and the file it came from.

    `_metadata.file_path` is Spark's own file-level metadata column, so the source file is
    recorded without parsing paths by hand. It only exists on file-based reads, which is why
    it is optional: the same function then works on a synthetic frame in a test.
    """
    from pyspark.sql import functions as F

    if not execution_id.strip():
        raise ValueError("execution_id must be non-empty.")
    stamped = trips.withColumn("execution_id", F.lit(execution_id)).withColumn(
        "ingested_at", F.current_timestamp()
    )
    if include_source_file:
        stamped = stamped.withColumn("source_file", F.col("_metadata.file_path"))
    return stamped


def split_trips_and_quarantine(
    trips: Any,
    *,
    min_trip_seconds: int = 60,
    max_trip_hours: int = 24,
) -> dict[str, Any]:
    """Route every trip to exactly one of valid, quarantine or duplicate.

    This mirrors the Silver split for station status, and the reason is the same: a row that
    fails a rule must be *recorded* rather than dropped, or the counts stop reconciling and
    nobody can tell a quality problem from a lost row.

    Deduplication is deliberate about which copy wins. `ride_id` is the archive's own unique
    key, so a repeat is a redelivery of the same ride; ordering by `started_at` then `ended_at`
    then `ride_id` makes the surviving copy deterministic rather than whichever partition
    happened to arrive first. Blank ride IDs are NOT deduplicated against each other - they go
    to quarantine as MISSING_RIDE_ID, because treating every blank as "the same ride" would
    collapse unrelated trips into one.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    validated = validate_historical_trips(
        trips, min_trip_seconds=min_trip_seconds, max_trip_hours=max_trip_hours
    )
    passing = validated.where(F.col("passed_contract"))
    quarantine = validated.where(~F.col("passed_contract"))

    ordering = Window.partitionBy("ride_id").orderBy(
        F.col("started_at").asc_nulls_last(),
        F.col("ended_at").asc_nulls_last(),
        F.col("ride_id").asc_nulls_last(),
    )
    ranked = passing.withColumn("_copy", F.row_number().over(ordering))
    valid = ranked.where(F.col("_copy") == 1).drop("_copy")
    duplicates = (
        ranked.where(F.col("_copy") > 1)
        .withColumn("failed_rules", F.array(F.lit("DUPLICATE_RIDE_ID")))
        .withColumn("passed_contract", F.lit(False))
        .drop("_copy")
    )
    return {"valid": valid, "quarantine": quarantine, "duplicates": duplicates}


def reconcile_historical(
    *,
    landed_rows: int,
    valid_rows: int,
    quarantine_rows: int,
    duplicate_rows: int,
    match_rate: dict[str, Any],
    demand_rows: int,
) -> dict[str, Any]:
    """Prove every landed trip has exactly one accountable outcome.

    The identity that must hold is landed == valid + quarantine + duplicates. A match rate
    below 1.0 is NOT a failure: stations are renamed and retired, so a real archive contains
    stations absent from the current feed. A match rate of exactly ZERO is the signature of
    joining on the wrong key, which is the mistake this module exists to prevent, so it is
    reported as a failure whenever there were trips to match.
    """
    accounted = valid_rows + quarantine_rows + duplicate_rows
    trips_joined = int(match_rate.get("trips", 0))
    rate = float(match_rate.get("match_rate", 0.0))
    join_collapsed = trips_joined > 0 and rate == 0.0
    counts_reconcile = accounted == landed_rows
    return {
        "landed_rows": landed_rows,
        "valid_rows": valid_rows,
        "quarantine_rows": quarantine_rows,
        "duplicate_rows": duplicate_rows,
        "accounted_rows": accounted,
        "counts_reconcile": counts_reconcile,
        "match_rate": rate,
        "join_produced_no_matches": join_collapsed,
        "demand_rows": demand_rows,
        "status": "PASS" if counts_reconcile and not join_collapsed else "FAIL",
    }


def persist_historical_outputs(
    spark: Any,
    *,
    valid: Any,
    quarantine: Any,
    duplicates: Any,
    daily_demand: Any,
    table_names: dict[str, str],
    execution_id: str,
) -> dict[str, dict[str, Any]]:
    """Persist the historical tables idempotently, migrating schemas first.

    `ride_id` is unique inside an archive, while `execution_id` isolates the committed 40-row
    sample from a later full-month load that contains the same rides. The valid trip table MERGEs
    on both columns so one execution can never steal another execution's lineage. Daily demand is
    execution-scoped rather than merged: if a rerun of the same archive produces fewer rows for a
    day - because a trip moved to quarantine, say - the old aggregate row has to go, exactly as
    with the Gold shortage list.
    """
    from urbanflow.persistence import (
        evolve_delta_schema,
        merge_delta_table,
        replace_execution_scope,
    )

    required = {"trips", "quarantine", "duplicates", "daily_demand"}
    missing = required - set(table_names)
    if missing:
        raise ValueError(f"Missing historical table names: {sorted(missing)}")

    migrations = {
        name: evolve_delta_schema(spark, frame, table_names[name])
        for name, frame in (
            ("trips", valid),
            ("quarantine", quarantine),
            ("duplicates", duplicates),
            ("daily_demand", daily_demand),
        )
    }
    results: dict[str, dict[str, Any]] = {
        "trips": merge_delta_table(
            spark,
            valid,
            table_names["trips"],
            key_columns=("execution_id", "ride_id"),
        ),
        "quarantine": merge_delta_table(
            spark,
            quarantine,
            table_names["quarantine"],
            key_columns=("execution_id", "ride_id", "started_at"),
        ),
        "duplicates": merge_delta_table(
            spark,
            duplicates,
            table_names["duplicates"],
            key_columns=("execution_id", "ride_id", "started_at", "ended_at"),
        ),
        "daily_demand": replace_execution_scope(
            spark,
            daily_demand,
            table_names["daily_demand"],
            key_columns=("station_short_name", "trip_date"),
            execution_column="execution_id",
            execution_id=execution_id,
        ),
    }
    for name, migration in migrations.items():
        results[name]["schema_migration"] = migration
    return results


def rider_mix_summary(joined: Any) -> Any:
    """Member versus casual split per station, the simplest real demand comparison."""
    from pyspark.sql import functions as F

    return (
        joined.groupBy("start_station_id")
        .agg(
            F.count("*").alias("trips"),
            F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).alias("member_trips"),
            F.sum(F.when(F.col("member_casual") == "casual", 1).otherwise(0)).alias("casual_trips"),
        )
        .withColumn(
            "member_share",
            F.when(
                F.col("trips") > 0, F.round(F.col("member_trips") / F.col("trips"), 4)
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumnRenamed("start_station_id", "station_short_name")
    )


def trip_duration_profile(trips: Any) -> dict[str, Any]:
    """Duration statistics in minutes, so an implausible archive is visible at a glance."""
    from pyspark.sql import functions as F

    minutes = trip_duration_seconds() / 60.0
    row = trips.select(
        F.count("*").alias("trips"),
        F.round(F.min(minutes), 2).alias("min_minutes"),
        F.round(F.max(minutes), 2).alias("max_minutes"),
        F.round(F.avg(minutes), 2).alias("avg_minutes"),
        F.round(F.percentile_approx(minutes, 0.5), 2).alias("median_minutes"),
    ).collect()[0]
    return {
        "trips": int(row["trips"]),
        "min_minutes": row["min_minutes"],
        "max_minutes": row["max_minutes"],
        "avg_minutes": row["avg_minutes"],
        "median_minutes": row["median_minutes"],
    }


DEMAND_COLUMNS: tuple[str, ...] = (
    "execution_id",
    "start_station_uuid",
    "station_short_name",
    "trip_date",
    "trips_started",
    "member_trips",
    "casual_trips",
    "avg_trip_minutes",
)


def daily_trip_demand(joined: Any, *, execution_id: str | None = None) -> Any:
    """Trips started per station per day: genuine historical demand from real archives.

    Unlike the availability summary, this IS backed by history, because one monthly archive
    contains weeks of real rides. This is the honest counterpart to the one-snapshot
    availability tables, and the dashboard presents it as the trend evidence precisely because
    the availability side cannot be.

    `execution_id` is always present so the output schema is stable whether or not a caller
    supplies one; a null column would otherwise appear and disappear, and the scoped
    replacement that writes this table needs the column to exist.
    """
    from pyspark.sql import functions as F

    return (
        joined.withColumn("trip_date", F.to_date("started_at"))
        .groupBy("start_station_uuid", "start_station_id", "trip_date")
        .agg(
            F.count("*").alias("trips_started"),
            F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).alias("member_trips"),
            F.sum(F.when(F.col("member_casual") == "casual", 1).otherwise(0)).alias("casual_trips"),
            F.round(F.avg(trip_duration_seconds() / 60.0), 2).alias("avg_trip_minutes"),
        )
        .withColumnRenamed("start_station_id", "station_short_name")
        .withColumn(
            "execution_id",
            F.lit(execution_id) if execution_id else F.lit(None).cast("string"),
        )
        .select(*DEMAND_COLUMNS)
    )

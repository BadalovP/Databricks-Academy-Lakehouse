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

# Rescued data goes in its own column rather than failing the read, so a column the
# official archive adds later is captured instead of discarded. This is the Lab 3
# schema-evolution and rescued-data requirement.
RESCUED_COLUMN = "_rescued_data"
SCHEMA_EVOLUTION_MODES = frozenset({"rescue", "addNewColumns", "failOnNewColumns", "none"})

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


def validate_historical_trips(trips: Any) -> Any:
    """Attach explicit quality failures while preserving every input row."""
    from pyspark.sql import functions as F

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


def daily_trip_demand(joined: Any) -> Any:
    """Trips started per station per day: genuine historical demand from real archives.

    Unlike the availability summary, this IS backed by history, because one monthly
    archive contains weeks of real rides.
    """
    from pyspark.sql import functions as F

    return (
        joined.withColumn("trip_date", F.to_date("started_at"))
        .groupBy("start_station_uuid", "start_station_id", "trip_date")
        .agg(
            F.count("*").alias("trips_started"),
            F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).alias("member_trips"),
            F.sum(F.when(F.col("member_casual") == "casual", 1).otherwise(0)).alias("casual_trips"),
            F.round(
                F.avg((F.col("ended_at").cast("long") - F.col("started_at").cast("long")) / 60.0),
                2,
            ).alias("avg_trip_minutes"),
        )
        .withColumnRenamed("start_station_id", "station_short_name")
    )

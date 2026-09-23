"""
LAB 09 - Databricks REST API Automation

Module:
pipeline/silver.py

Purpose:
Applies Silver hard-validity rules to lab09_taxi_bronze and splits the
result into two disjoint outputs instead of relying on
@dp.expect_all_or_drop, whose dropped rows cannot be counted afterward:

  lab09_taxi_silver      -- rows that pass every hard-validity rule
  lab09_taxi_quarantine  -- rows that fail at least one rule, carrying a
                            failed_rules array naming every rule violated

Both outputs are built from the same tagged read of bronze
(_tagged_and_zoned_bronze), partitioned only by whether failed_rules is
empty. That makes the reconciliation invariant

    bronze_rows == silver_valid_rows + quarantine_rows

true by construction; notebooks/01_reconcile_counts.py asserts it.

Hard-validity rules (Phase 8):
  INVALID_FARE            fare_amount is null or <= 0
  INVALID_DISTANCE        trip_distance is null or <= 0
  INVALID_DATETIME_ORDER  dropoff_datetime <= pickup_datetime (or either null)
  INVALID_MONTH           the pickup month does not match the month encoded
                           in the source filename (derived from
                           _metadata.file_path via bronze's
                           _lab09_source_file column)

passenger_count is deliberately NOT a hard-validity rule: a null or
non-positive passenger_count only sets passenger_count_warning=true and
never contributes to quarantine.

Zone validation is observability, not a hard-validity rule: pickup/dropoff
location IDs are left-joined against the taxi zone lookup reference file.
Unknown zones (including TLC's own 264/265 "Unknown"/"N/A" codes) are kept
and their borough/zone are coalesced to the literal string "UNKNOWN" rather
than dropped, with an accompanying *_zone_known boolean. No location ID
range is hardcoded as a validity test -- only the lookup join result.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEFAULT_REFERENCE_CSV_PATH = (
    "/Volumes/dbr_dev/parvinbadalov/lab09_landing/reference/taxi_zone_lookup.csv"
)

FAILED_RULE_MONTH_PATTERN = r"yellow_tripdata_(\d{4}-\d{2})\.parquet"


def _reference_csv_path() -> str:
    return spark.conf.get("lab09.reference_csv_path", DEFAULT_REFERENCE_CSV_PATH)


def _read_zone_lookup():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(_reference_csv_path())
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
        )
    )


def _join_zone(df, location_col: str, prefix: str):
    """Left-join `df[location_col]` against the zone lookup, flag unknowns, never drop rows."""
    zones = F.broadcast(_read_zone_lookup())
    joined = df.join(zones, df[location_col] == zones["location_id"], "left")
    return (
        joined.withColumn(f"{prefix}_zone_known", F.col("location_id").isNotNull())
        .withColumn(f"{prefix}_borough", F.coalesce(F.col("borough"), F.lit("UNKNOWN")))
        .withColumn(f"{prefix}_zone", F.coalesce(F.col("zone"), F.lit("UNKNOWN")))
        .drop("location_id", "borough", "zone")
    )


def _tagged_and_zoned_bronze():
    df = spark.read.table("lab09_taxi_bronze")

    df = df.withColumn(
        "_source_month", F.regexp_extract(F.col("_lab09_source_file"), FAILED_RULE_MONTH_PATTERN, 1)
    )
    df = df.withColumn("_pickup_month", F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"))

    rules = {
        "INVALID_FARE": F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0),
        "INVALID_DISTANCE": F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0),
        "INVALID_DATETIME_ORDER": (
            F.col("tpep_pickup_datetime").isNull()
            | F.col("tpep_dropoff_datetime").isNull()
            | (F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"))
        ),
        "INVALID_MONTH": (
            (F.col("_source_month") == "")
            | F.col("_pickup_month").isNull()
            | (F.col("_pickup_month") != F.col("_source_month"))
        ),
    }

    tagged = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    df = df.withColumn("failed_rules", F.array_except(tagged, F.array(F.lit(None).cast("string"))))
    df = df.withColumn(
        "passenger_count_warning",
        F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0),
    )

    df = _join_zone(df, "PULocationID", "pickup")
    df = _join_zone(df, "DOLocationID", "dropoff")
    return df.drop("_source_month", "_pickup_month")


@dp.materialized_view(
    name="lab09_taxi_silver",
    comment="NYC TLC Yellow Taxi rows that pass every Silver hard-validity rule.",
)
def lab09_taxi_silver():
    df = _tagged_and_zoned_bronze()
    return df.filter(F.size(F.col("failed_rules")) == 0).drop("failed_rules")


@dp.materialized_view(
    name="lab09_taxi_quarantine",
    comment="NYC TLC Yellow Taxi rows failing at least one Silver hard-validity rule, with reasons.",
)
def lab09_taxi_quarantine():
    df = _tagged_and_zoned_bronze()
    return df.filter(F.size(F.col("failed_rules")) > 0)

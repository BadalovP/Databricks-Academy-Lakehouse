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
No location ID range is hardcoded as a validity test -- only the lookup
join result, and specifically the *values* it returns, decide
`*_zone_known` (see `_join_zone` below). Rows are never dropped for an
unknown zone.

A location ID can fail to be "known" two different ways, both retained and
both normalized to display value "UNKNOWN":
  - unmatched: no row in the lookup file has this LocationID at all.
  - TLC's own semantic placeholders: the lookup file DOES have a row (so a
    naive "did the join match" check alone would call this known), but its
    Borough or Zone value is itself TLC's own "Unknown"/"N/A" marker.
    Confirmed directly from the official taxi_zone_lookup.csv:
    LocationID 264 = Borough "Unknown", Zone "N/A"; LocationID 265 =
    Borough "N/A", Zone "Outside of NYC". Treating either of those as a
    genuinely "known" zone would be wrong even though the join succeeds.

This module runs inside Lakeflow and cannot be unit tested directly (it
references `spark` at runtime and is not importable outside a live Spark
session, like every other file under pipeline/). The zone-known decision
in `_join_zone` below is mirrored in plain Python in
`src/lab09/zone_lookup.py` (kept in sync by hand) and unit tested in
`tests/test_zone_lookup.py` -- that proves the classification logic in
isolation, not that Lakeflow resolves it identically live; see README.md
"Known limitations".
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEFAULT_REFERENCE_CSV_PATH = (
    "/Volumes/dbr_dev/parvinbadalov/lab09_landing/reference/taxi_zone_lookup.csv"
)

FAILED_RULE_MONTH_PATTERN = r"yellow_tripdata_(\d{4}-\d{2})\.parquet"

# TLC's own semantic "not a real zone" placeholder values, observed
# verbatim in the official taxi_zone_lookup.csv (LocationID 264 and 265).
# Matched case-insensitively against both Borough and Zone.
UNKNOWN_ZONE_MARKERS = {"unknown", "n/a", "na"}


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
    """Left-join `df[location_col]` against the zone lookup, flag unknowns, never drop rows.

    `*_zone_known` is true only for a row that both (a) matched a lookup
    entry and (b) that entry's Borough/Zone are not themselves TLC's own
    "Unknown"/"N/A" placeholders -- a successful join alone is not enough,
    since 264/265 always join successfully.
    """
    zones = F.broadcast(_read_zone_lookup())
    joined = df.join(zones, df[location_col] == zones["location_id"], "left")

    matched = F.col("location_id").isNotNull()
    normalized_borough = F.lower(F.trim(F.coalesce(F.col("borough"), F.lit(""))))
    normalized_zone = F.lower(F.trim(F.coalesce(F.col("zone"), F.lit(""))))
    is_semantic_unknown = normalized_borough.isin(*UNKNOWN_ZONE_MARKERS) | normalized_zone.isin(
        *UNKNOWN_ZONE_MARKERS
    )
    zone_known = matched & ~is_semantic_unknown

    return (
        joined.withColumn(f"{prefix}_zone_known", zone_known)
        .withColumn(
            f"{prefix}_borough", F.when(zone_known, F.col("borough")).otherwise(F.lit("UNKNOWN"))
        )
        .withColumn(f"{prefix}_zone", F.when(zone_known, F.col("zone")).otherwise(F.lit("UNKNOWN")))
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


# Both lab09_taxi_silver and lab09_taxi_quarantine are built from
# _tagged_and_zoned_bronze(), which reads lab09_taxi_bronze and retains its
# tpep_pickup_datetime/tpep_dropoff_datetime columns unmodified -- Spark
# infers these as TIMESTAMP_NTZ (no timezone) from the source parquet, and
# neither function casts or converts them (see module docstring: no
# timezone conversion is introduced here). Confirmed live (2026-09-25,
# personal-yahoo profile, pipeline lab09_taxi_pipeline_v2, update
# d6290227-9422-4e86-90f9-2eed6463fb62): creating lab09_taxi_quarantine
# failed with
#   [DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT] Your table schema requires
#   manually enablement of the following table feature(s): timestampNtz.
# because a materialized_view's table creation, unlike the bronze streaming
# table's, does not auto-enable this Delta protocol feature for a
# TIMESTAMP_NTZ column. table_properties={"delta.feature.timestampNtz":
# "supported"} declares it up front instead of requiring a manual
# `ALTER TABLE ... SET TBLPROPERTIES` after the fact. See README.md "Known
# limitations" for the live failure detail and the protocol-version
# tradeoff this carries.
_TIMESTAMP_NTZ_TABLE_PROPERTIES = {"delta.feature.timestampNtz": "supported"}


@dp.materialized_view(
    name="lab09_taxi_silver",
    comment="NYC TLC Yellow Taxi rows that pass every Silver hard-validity rule.",
    table_properties=_TIMESTAMP_NTZ_TABLE_PROPERTIES,
)
def lab09_taxi_silver():
    df = _tagged_and_zoned_bronze()
    return df.filter(F.size(F.col("failed_rules")) == 0).drop("failed_rules")


@dp.materialized_view(
    name="lab09_taxi_quarantine",
    comment="NYC TLC Yellow Taxi rows failing at least one Silver hard-validity rule, with reasons.",
    table_properties=_TIMESTAMP_NTZ_TABLE_PROPERTIES,
)
def lab09_taxi_quarantine():
    df = _tagged_and_zoned_bronze()
    return df.filter(F.size(F.col("failed_rules")) > 0)

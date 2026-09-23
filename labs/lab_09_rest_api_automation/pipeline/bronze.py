"""
LAB 09 - Databricks REST API Automation

Module:
pipeline/bronze.py

Purpose:
Reads NYC TLC Yellow Taxi monthly Parquet files landed under the Lab 9
Unity Catalog Volume via Auto Loader and materializes them, unmodified
except for ingestion metadata, as lab09_taxi_bronze. The source file's
workspace path is preserved as `_lab09_source_file` so silver.py can derive
each row's encoded source month from `_metadata.file_path` (Phase 8).

Input:
/Volumes/<catalog>/<schema>/<volume>/trips/*.parquet

Output:
lab09_taxi_bronze

Idempotency:
The Files API landing layer (src/lab09/landing.py) never re-uploads a month
that is already present, and Auto Loader's checkpoint means an unchanged
set of already-ingested files is not reprocessed on subsequent pipeline
updates -- new months simply appear as new files under the same directory.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEFAULT_LANDING_TRIPS_PATH = "/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips/"


def _landing_trips_path() -> str:
    return spark.conf.get("lab09.landing_trips_path", DEFAULT_LANDING_TRIPS_PATH)


@dp.table(
    name="lab09_taxi_bronze",
    comment="Raw NYC TLC Yellow Taxi trip records ingested by Auto Loader from the Lab 9 landing volume.",
)
def lab09_taxi_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", "*.parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(_landing_trips_path())
        .withColumn("_lab09_ingested_at", F.current_timestamp())
        .withColumn("_lab09_source_file", F.col("_metadata.file_path"))
    )

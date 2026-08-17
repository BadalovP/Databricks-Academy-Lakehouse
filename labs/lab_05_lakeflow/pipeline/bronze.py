"""
Lab 05 — Lakeflow Bronze layer.

Defines:
1. station_status_bronze      — streaming table via Auto Loader
2. station_information_bronze — batch materialized view

Important Lakeflow note:
Pipeline source files are evaluated by Databricks and do not reliably expose
the normal Python __file__ variable. The pipeline root configured by root_path
is automatically added to sys.path, so project modules can be imported
directly from src/.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import (
    STATION_INFORMATION_BRONZE,
    STATION_STATUS_BRONZE,
    load_config,
)


config = load_config(spark)


@dp.table(
    name=STATION_STATUS_BRONZE,
    comment=(
        "Raw Citi Bike GBFS station_status snapshots ingested "
        "incrementally with Auto Loader. One row represents one "
        "raw GBFS source document."
    ),
    table_properties={
        "quality": "bronze",
        "lab": "05",
        "source": "citibike_gbfs_station_status",
    },
)
def station_status_bronze() -> DataFrame:
    """
    Incrementally ingest timestamped station_status JSON snapshots.

    Lakeflow manages checkpoint/schema state. Auto Loader infers the JSON
    structure from the source files; inferColumnTypes is enabled so nested
    numeric and boolean GBFS fields retain useful Spark types.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(config.station_status_landing_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modification_time"
            ),
            F.current_timestamp().alias("_bronze_ingested_at"),
        )
    )


@dp.materialized_view(
    name=STATION_INFORMATION_BRONZE,
    comment=(
        "Raw Citi Bike GBFS station_information reference "
        "document loaded as a batch materialized view."
    ),
    table_properties={
        "quality": "bronze",
        "lab": "05",
        "source": "citibike_gbfs_station_information",
    },
)
def station_information_bronze() -> DataFrame:
    """Load the current station_information reference JSON."""
    return (
        spark.read
        .option("multiLine", "true")
        .json(config.station_information_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modification_time"
            ),
            F.current_timestamp().alias("_bronze_refreshed_at"),
        )
    )

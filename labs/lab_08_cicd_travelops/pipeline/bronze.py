"""
LAB 08 - TravelOps

Module:
pipeline/bronze.py

Purpose:
Defines the TravelOps Bronze Lakeflow datasets that incrementally ingest
deterministic raw Parquet files from the target-specific Unity Catalog Volume.

Responsibilities:
- read raw files with Auto Loader
- preserve source columns without business enrichment
- add ingestion metadata useful for troubleshooting and lineage

Inputs:
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/bookings
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/booking_updates
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/payments
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/users
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/properties
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/reviews
- /Volumes/<raw_volume_catalog>/<raw_volume_schema>/<raw_volume>/raw/destinations

Outputs:
- bookings_bronze
- booking_updates_bronze
- payments_bronze
- users_bronze
- properties_bronze
- reviews_bronze
- destinations_bronze

Idempotency:
Lakeflow manages checkpointing. The seed notebook writes deterministic source
paths, so repeated deployments do not create duplicate business records.

Environment behavior:
Raw Volume catalog, schema and name are read from pipeline configuration
injected by the Databricks Asset Bundle target. They intentionally differ from
the Lakeflow target schema for Personal DEV and Personal PROD.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _raw_path(folder: str) -> str:
    """Return the target-specific raw folder for a source entity."""

    catalog = spark.conf.get("travelops.raw_volume_catalog")
    schema = spark.conf.get("travelops.raw_volume_schema")
    volume = spark.conf.get("travelops.raw_volume_name")
    return f"/Volumes/{catalog}/{schema}/{volume}/raw/{folder}"


def _private_bronze() -> bool:
    """Return whether Bronze should be a private pipeline intermediate."""

    return spark.conf.get("travelops.private_bronze", "false").lower() == "true"


def _read_raw_parquet(folder: str):
    """Read a raw Parquet folder with Auto Loader and add ingestion metadata."""

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.partitionColumns", "")
        .load(_raw_path(folder))
        .withColumn("_travelops_ingested_at", F.current_timestamp())
        .withColumn("_travelops_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bookings_bronze",
    comment="Raw Wanderbricks bookings ingested by Auto Loader.",
    private=_private_bronze(),
)
def bookings_bronze():
    return _read_raw_parquet("bookings")


@dp.table(
    name="booking_updates_bronze",
    comment="Raw Wanderbricks booking update log ingested by Auto Loader.",
    private=_private_bronze(),
)
def booking_updates_bronze():
    return _read_raw_parquet("booking_updates")


@dp.table(
    name="payments_bronze",
    comment="Raw Wanderbricks payment events ingested by Auto Loader.",
    private=_private_bronze(),
)
def payments_bronze():
    return _read_raw_parquet("payments")


@dp.table(
    name="users_bronze",
    comment="Raw Wanderbricks users ingested by Auto Loader.",
    private=_private_bronze(),
)
def users_bronze():
    return _read_raw_parquet("users")


@dp.table(
    name="properties_bronze",
    comment="Raw Wanderbricks properties ingested by Auto Loader.",
    private=_private_bronze(),
)
def properties_bronze():
    return _read_raw_parquet("properties")


@dp.table(
    name="reviews_bronze",
    comment="Raw Wanderbricks reviews ingested by Auto Loader.",
    private=_private_bronze(),
)
def reviews_bronze():
    return _read_raw_parquet("reviews")


@dp.table(
    name="destinations_bronze",
    comment="Raw Wanderbricks destinations ingested by Auto Loader.",
    private=_private_bronze(),
)
def destinations_bronze():
    return _read_raw_parquet("destinations")

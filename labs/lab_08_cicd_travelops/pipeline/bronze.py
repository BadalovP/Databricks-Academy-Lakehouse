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
Lakeflow manages checkpointing. As of the ingestion-idempotency fix in
`notebooks/00_seed_raw_data.ipynb`, the seed notebook writes each raw table
to a single Parquet file at a path derived from `seed_limit`, instead of
letting Spark allocate a fresh, uniquely-named file on every write. Auto
Loader's default `cloudFiles.allowOverwrites=false` means it will not
reprocess a path it has already ingested, even if that path is later
overwritten with new bytes, so a rerun with an unchanged `seed_limit` should
no longer be re-ingested as new. This is based on Auto Loader's documented
behavior and has not been empirically validated by an actual run in this
repository — see the notebook's Idempotency note and
`evidence/lab08_production_remediation_plan.md` for the validation
procedure and residual limitations (it does not cover every possible
content change, and it does not retroactively deduplicate Bronze rows
already accumulated from runs before this fix shipped). Silver
(`pipeline/silver.py`) still deduplicates as a defense-in-depth backstop for
those residual cases — `current_bookings_silver` keeps only the latest row
per `booking_id`, and `payments_silver` deduplicates on a composite business
key — but Silver deduplication does not by itself prevent Bronze from
accumulating duplicate rows; it only prevents those duplicates from
corrupting Silver/Gold results.

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

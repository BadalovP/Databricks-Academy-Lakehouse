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
Lakeflow manages checkpointing. `notebooks/00_seed_raw_data.ipynb` treats
each raw table's seed as an immutable, write-once snapshot of a fixed
sample dataset (`samples.wanderbricks`): it writes one Parquet file named
from an explicit, human-controlled identity (`SEED_VERSION` and
`seed_limit`) and the sampled row count, skips writing entirely when a file
for that exact identity and row count already exists, and raises instead of
writing anything if the row count differs under an unchanged identity
(treated as an unexpected upstream change, not something to silently
absorb). Auto Loader's default `cloudFiles.allowOverwrites=false` means it
will not reprocess a path it has already ingested, so a rerun with
unchanged content should no longer be re-ingested as new. This is based on
Auto Loader's documented behavior and has not been empirically validated by
an actual run in this repository — see the notebook's architecture decision
and `evidence/lab08_production_remediation_plan.md` for the validation
procedure and residual limitations (row count is multiplicity-aware but not
a full content check — a same-row-count in-place value edit is not
detected; it does not retroactively deduplicate Bronze rows already
accumulated from runs before this fix shipped; and it never deletes an
existing file, by design, to avoid ever leaving the raw Volume without
valid input). Silver (`pipeline/silver.py`) still deduplicates as a
defense-in-depth backstop for residual cases — `current_bookings_silver`
keeps only the latest row per `booking_id`, `payments_silver`/
`reviews_silver` deduplicate on composite business keys, and
`properties_silver`/`destinations_silver`/`users_silver` deduplicate on
their primary key — but Silver deduplication does not by itself prevent
Bronze from accumulating duplicate rows; it only prevents those duplicates
from corrupting Silver/Gold results.

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

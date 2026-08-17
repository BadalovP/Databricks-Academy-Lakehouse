"""
Lab 05 — Lakeflow Silver layer.

This version uses the ACTUAL Citi Bike GBFS fields observed in the current
station_status and station_information feeds.

Observed station_status fields include:
- station_id
- num_bikes_available
- num_bikes_disabled
- num_docks_available
- num_docks_disabled
- num_ebikes_available
- num_scooters_available
- num_scooters_unavailable
- is_installed
- is_renting
- is_returning
- last_reported
- vehicle_types_available

Observed station_information fields include:
- station_id
- name
- short_name
- lat
- lon
- region_id
- capacity
- rental_uris

Provider fields that are NOT currently present are intentionally not referenced.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import (
    STATION_INFORMATION_BRONZE,
    STATION_INFORMATION_SILVER,
    STATION_STATUS_BRONZE,
    STATION_STATUS_ENRICHED_SILVER,
    STATION_STATUS_SILVER,
)
from src.quality_rules import (
    INFORMATION_DROP_EXPECTATIONS,
    INFORMATION_MONITOR_EXPECTATIONS,
    STATUS_DROP_EXPECTATIONS,
    STATUS_MONITOR_EXPECTATIONS,
)


@dp.table(
    name=STATION_STATUS_SILVER,
    comment=(
        "Validated Citi Bike station status observations. "
        "One row represents one station in one source snapshot."
    ),
    table_properties={
        "quality": "silver",
        "lab": "05",
        "entity": "station_status",
    },
)
@dp.expect_all(STATUS_MONITOR_EXPECTATIONS)
@dp.expect_all_or_drop(STATUS_DROP_EXPECTATIONS)
def station_status_silver() -> DataFrame:
    """Flatten streaming station_status Bronze documents."""
    bronze_df = spark.readStream.table(
        STATION_STATUS_BRONZE
    )

    return (
        bronze_df
        .select(
            F.col("last_updated").alias(
                "snapshot_last_updated_epoch"
            ),
            F.col("ttl").alias(
                "snapshot_ttl_seconds"
            ),
            F.col("version").alias(
                "gbfs_version"
            ),
            F.explode("data.stations").alias(
                "station"
            ),
            "_source_file",
            "_source_file_name",
            "_source_file_modification_time",
            "_bronze_ingested_at",
        )
        .select(
            F.col("station.station_id").alias(
                "station_id"
            ),
            F.col(
                "station.num_bikes_available"
            ).alias(
                "num_bikes_available"
            ),
            F.col(
                "station.num_bikes_disabled"
            ).alias(
                "num_bikes_disabled"
            ),
            F.col(
                "station.num_docks_available"
            ).alias(
                "num_docks_available"
            ),
            F.col(
                "station.num_docks_disabled"
            ).alias(
                "num_docks_disabled"
            ),
            F.col(
                "station.num_ebikes_available"
            ).alias(
                "num_ebikes_available"
            ),
            F.col(
                "station.num_scooters_available"
            ).alias(
                "num_scooters_available"
            ),
            F.col(
                "station.num_scooters_unavailable"
            ).alias(
                "num_scooters_unavailable"
            ),
            F.col(
                "station.vehicle_types_available"
            ).alias(
                "vehicle_types_available"
            ),
            F.col(
                "station.is_installed"
            ).alias(
                "is_installed"
            ),
            F.col(
                "station.is_renting"
            ).alias(
                "is_renting"
            ),
            F.col(
                "station.is_returning"
            ).alias(
                "is_returning"
            ),
            F.col(
                "station.last_reported"
            ).alias(
                "last_reported"
            ),
            "snapshot_last_updated_epoch",
            "snapshot_ttl_seconds",
            "gbfs_version",
            "_source_file",
            "_source_file_name",
            "_source_file_modification_time",
            "_bronze_ingested_at",
        )
        .withColumn(
            "last_reported_at",
            F.to_timestamp(
                F.from_unixtime(
                    "last_reported"
                )
            ),
        )
        .withColumn(
            "snapshot_last_updated_at",
            F.to_timestamp(
                F.from_unixtime(
                    "snapshot_last_updated_epoch"
                )
            ),
        )
        .withColumn(
            "status_record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("_source_file"),
                    F.col("station_id"),
                ),
                256,
            ),
        )
    )


@dp.materialized_view(
    name=STATION_INFORMATION_SILVER,
    comment=(
        "Validated Citi Bike station reference information. "
        "One row represents one station."
    ),
    table_properties={
        "quality": "silver",
        "lab": "05",
        "entity": "station_information",
    },
)
@dp.expect_all(
    INFORMATION_MONITOR_EXPECTATIONS
)
@dp.expect_all_or_drop(
    INFORMATION_DROP_EXPECTATIONS
)
def station_information_silver() -> DataFrame:
    """Flatten the current station_information reference document."""
    bronze_df = spark.read.table(
        STATION_INFORMATION_BRONZE
    )

    return (
        bronze_df
        .select(
            F.col("last_updated").alias(
                "reference_last_updated_epoch"
            ),
            F.col("ttl").alias(
                "reference_ttl_seconds"
            ),
            F.col("version").alias(
                "gbfs_version"
            ),
            F.explode("data.stations").alias(
                "station"
            ),
            "_source_file",
            "_source_file_name",
            "_source_file_modification_time",
            "_bronze_refreshed_at",
        )
        .select(
            F.col(
                "station.station_id"
            ).alias(
                "station_id"
            ),
            F.col(
                "station.name"
            ).alias(
                "station_name"
            ),
            F.col(
                "station.short_name"
            ).alias(
                "short_name"
            ),
            F.col(
                "station.lat"
            ).alias(
                "latitude"
            ),
            F.col(
                "station.lon"
            ).alias(
                "longitude"
            ),
            F.col(
                "station.region_id"
            ).alias(
                "region_id"
            ),
            F.col(
                "station.capacity"
            ).alias(
                "capacity"
            ),
            F.col(
                "station.rental_uris"
            ).alias(
                "rental_uris"
            ),
            "reference_last_updated_epoch",
            "reference_ttl_seconds",
            "gbfs_version",
            "_source_file",
            "_source_file_name",
            "_source_file_modification_time",
            "_bronze_refreshed_at",
        )
        .withColumn(
            "reference_last_updated_at",
            F.to_timestamp(
                F.from_unixtime(
                    "reference_last_updated_epoch"
                )
            ),
        )
    )


@dp.table(
    name=STATION_STATUS_ENRICHED_SILVER,
    comment=(
        "Validated streaming station status enriched with "
        "Citi Bike station metadata on station_id."
    ),
    table_properties={
        "quality": "silver",
        "lab": "05",
        "entity": "station_status_enriched",
    },
)
def station_status_enriched_silver() -> DataFrame:
    """Join streaming station observations to the static station reference."""
    status_df = (
        spark.readStream
        .table(
            STATION_STATUS_SILVER
        )
        .alias("status")
    )

    information_df = (
        spark.read
        .table(
            STATION_INFORMATION_SILVER
        )
        .alias("info")
    )

    return (
        status_df
        .join(
            information_df,
            on=(
                F.col("status.station_id")
                == F.col("info.station_id")
            ),
            how="left",
        )
        .select(
            F.col(
                "status.status_record_id"
            ),
            F.col(
                "status.station_id"
            ),
            F.col(
                "info.station_name"
            ),
            F.col(
                "info.short_name"
            ),
            F.col(
                "info.latitude"
            ),
            F.col(
                "info.longitude"
            ),
            F.col(
                "info.region_id"
            ),
            F.col(
                "info.capacity"
            ),
            F.col(
                "status.num_bikes_available"
            ),
            F.col(
                "status.num_bikes_disabled"
            ),
            F.col(
                "status.num_docks_available"
            ),
            F.col(
                "status.num_docks_disabled"
            ),
            F.col(
                "status.num_ebikes_available"
            ),
            F.col(
                "status.num_scooters_available"
            ),
            F.col(
                "status.num_scooters_unavailable"
            ),
            F.col(
                "status.is_installed"
            ),
            F.col(
                "status.is_renting"
            ),
            F.col(
                "status.is_returning"
            ),
            F.col(
                "status.last_reported"
            ),
            F.col(
                "status.last_reported_at"
            ),
            F.col(
                "status.snapshot_last_updated_epoch"
            ),
            F.col(
                "status.snapshot_last_updated_at"
            ),
            F.col(
                "status.snapshot_ttl_seconds"
            ),
            F.col(
                "status.gbfs_version"
            ),
            F.col(
                "status._source_file"
            ),
            F.col(
                "status._source_file_name"
            ),
            F.col(
                "status._source_file_modification_time"
            ),
            F.col(
                "status._bronze_ingested_at"
            ),
            F.col(
                "info._bronze_refreshed_at"
            ).alias(
                "_station_information_refreshed_at"
            ),
        )
        .withColumn(
            "bike_availability_pct",
            F.when(
                F.col("capacity") > 0,
                F.round(
                    (
                        F.col(
                            "num_bikes_available"
                        )
                        / F.col("capacity")
                    )
                    * 100,
                    2,
                ),
            ),
        )
        .withColumn(
            "dock_availability_pct",
            F.when(
                F.col("capacity") > 0,
                F.round(
                    (
                        F.col(
                            "num_docks_available"
                        )
                        / F.col("capacity")
                    )
                    * 100,
                    2,
                ),
            ),
        )
        .withColumn(
            "station_reference_matched",
            F.col(
                "station_name"
            ).isNotNull(),
        )
    )

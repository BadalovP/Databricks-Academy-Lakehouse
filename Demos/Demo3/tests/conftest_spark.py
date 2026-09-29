"""Shared local-Spark helpers for the Silver/Gold/historical tests.

Frames are built through Spark SQL `VALUES` rather than `createDataFrame`, because the
local Python worker path is unreliable on this development machine (it intermittently
fails with a socket error during serialization). A `VALUES` plan stays inside the JVM, so
these tests are both faster and stable.
"""

from __future__ import annotations

from typing import Any

BRONZE_DDL = (
    "event_id string, execution_id string, station_id string, num_bikes_available int, "
    "num_docks_available int, num_bikes_disabled int, num_docks_disabled int, "
    "num_ebikes_available int, is_installed int, is_renting int, is_returning int, "
    "last_reported bigint, source_last_updated bigint, collected_at string, "
    "source_url string, gbfs_version string, kafka_topic string, kafka_partition int, "
    "kafka_offset bigint, kafka_timestamp timestamp, ingested_at timestamp, "
    "raw_json string, parse_error boolean"
)


def bronze_row(
    *,
    event_id: str,
    station_id: str = "st-1",
    bikes: int = 10,
    docks: int = 10,
    source_last_updated: int = 1790711443,
    offset: int = 1,
    partition: int = 0,
    kafka_timestamp: str = "2026-09-29 19:51:40",
    parse_error: str = "false",
    execution_id: str = "run-A",
    bikes_disabled: int = 0,
    docks_disabled: int = 0,
    ebikes: int = 0,
    is_installed: int = 1,
    is_renting: int = 1,
    is_returning: int = 1,
) -> str:
    """One Bronze row as a SQL VALUES tuple, in BRONZE_DDL column order."""
    return (
        f"('{event_id}','{execution_id}','{station_id}',{bikes},{docks},"
        f"{bikes_disabled},{docks_disabled},{ebikes},{is_installed},{is_renting},{is_returning},"
        f"{source_last_updated},{source_last_updated},'2026-09-29T19:51:35Z',"
        f"'https://example/gbfs','2.3','parvinbadalov_evh',{partition},"
        f"{offset},TIMESTAMP'{kafka_timestamp}',TIMESTAMP'2026-09-29 19:51:45',"
        f"'{{}}',{parse_error})"
    )


def bronze_frame(spark: Any, rows: list[str]) -> Any:
    """Build a Bronze-shaped DataFrame from VALUES tuples, typed via BRONZE_DDL."""
    values = ", ".join(rows)
    raw = spark.sql(f"SELECT * FROM VALUES {values} AS t({_ddl_names(BRONZE_DDL)})")
    # Cast into the declared Bronze contract so the tests exercise the real types.
    casts = ", ".join(
        f"CAST({name} AS {dtype}) AS {name}" for name, dtype in _ddl_pairs(BRONZE_DDL)
    )
    raw.createOrReplaceTempView("urbanflow_test_bronze_raw")
    return spark.sql(f"SELECT {casts} FROM urbanflow_test_bronze_raw")


def _ddl_pairs(ddl: str) -> list[tuple[str, str]]:
    pairs = []
    for part in ddl.split(","):
        name, dtype = part.strip().split(" ", 1)
        pairs.append((name, dtype))
    return pairs


def _ddl_names(ddl: str) -> str:
    return ", ".join(name for name, _ in _ddl_pairs(ddl))


def station_information_frame(spark: Any) -> Any:
    """Two real-shaped GBFS stations: UUID station_id plus dotted short_name."""
    return spark.sql(
        "SELECT * FROM VALUES "
        "('66dd9fa6-0aca-11e7-82f6-3863bb44ef7c','6626.01','E 48 St & 5 Ave',"
        "40.76,-73.97,55,'71'), "
        "('0138452a-b9f4-4aee-80b0-7fae9a122ffe','7407.13','E 102 St & 1 Ave',"
        "40.78,-73.94,31,'71') "
        "AS t(station_id, short_name, name, lat, lon, capacity, region_id)"
    )

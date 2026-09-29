"""Spark Structured Streaming helpers for Event Hubs' Kafka endpoint."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import reduce
from pathlib import PurePosixPath
from typing import Any

REQUIRED_EVENT_FIELDS = (
    "event_id",
    "execution_id",
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
    "source_last_updated",
    "collected_at",
    "source_url",
)


@dataclass(frozen=True)
class StreamPaths:
    checkpoint: str
    report: str


def isolated_stream_paths(
    *,
    volume_root: str,
    checkpoint_subpath: str,
    report_subpath: str,
    execution_id: str,
) -> StreamPaths:
    """Build paths that cannot escape the configured Unity Catalog Volume."""
    root = PurePosixPath(volume_root)
    if not str(root).startswith("/Volumes/") or len(root.parts) < 5:
        raise ValueError("volume_root must identify a Unity Catalog Volume under /Volumes.")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", execution_id):
        raise ValueError(
            "execution_id may contain only letters, numbers, dot, underscore, or dash."
        )
    for subpath in (checkpoint_subpath, report_subpath):
        candidate = PurePosixPath(subpath)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError("Streaming subpaths must be relative and may not contain '..'.")
    checkpoint = root / checkpoint_subpath
    report = root / report_subpath / f"{execution_id}.bronze.json"
    return StreamPaths(checkpoint=str(checkpoint), report=str(report))


def station_status_schema() -> Any:
    """Explicit event contract; new unknown fields remain outside trusted columns."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("execution_id", T.StringType(), False),
            T.StructField("station_id", T.StringType(), False),
            T.StructField("num_bikes_available", T.IntegerType(), False),
            T.StructField("num_docks_available", T.IntegerType(), False),
            T.StructField("num_bikes_disabled", T.IntegerType(), True),
            T.StructField("num_docks_disabled", T.IntegerType(), True),
            T.StructField("num_ebikes_available", T.IntegerType(), True),
            T.StructField("is_installed", T.IntegerType(), False),
            T.StructField("is_renting", T.IntegerType(), False),
            T.StructField("is_returning", T.IntegerType(), False),
            T.StructField("last_reported", T.LongType(), False),
            T.StructField("source_last_updated", T.LongType(), False),
            T.StructField("collected_at", T.StringType(), False),
            T.StructField("source_url", T.StringType(), False),
            T.StructField("gbfs_version", T.StringType(), True),
        ]
    )


def event_hubs_kafka_options(
    *,
    namespace: str,
    connection_string: str,
    event_hub_name: str,
    consumer_group: str,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 5000,
) -> dict[str, str]:
    """Build Kafka options without logging or returning a redacted fake credential."""
    if not connection_string:
        raise ValueError("A non-empty Event Hubs connection string is required.")
    login = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": event_hub_name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": login,
        "kafka.group.id": consumer_group,
        "startingOffsets": starting_offsets,
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": str(max_offsets_per_trigger),
    }


def redacted_kafka_options(options: dict[str, str]) -> dict[str, str]:
    """Return safe diagnostic options without the SASL connection string."""
    redacted = dict(options)
    if "kafka.sasl.jaas.config" in redacted:
        redacted["kafka.sasl.jaas.config"] = "[REDACTED]"
    return redacted


def read_event_hubs_stream(spark: Any, options: dict[str, str]) -> Any:
    """Create the unresolved streaming DataFrame; no query starts here."""
    return spark.readStream.format("kafka").options(**options).load()


def parse_station_events(kafka_dataframe: Any) -> Any:
    """Parse JSON with the explicit schema and retain Kafka lineage metadata."""
    from pyspark.sql import functions as F

    parsed = kafka_dataframe.select(
        F.from_json(F.col("value").cast("string"), station_status_schema()).alias("event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.col("value").cast("string").alias("raw_json"),
    )
    # Spark's select() expands only "*" and "<struct>.*". A prefix pattern such as
    # "kafka_*" is read as a literal column name and fails analysis with
    # UNRESOLVED_COLUMN, so every Kafka lineage column is named explicitly here.
    flattened = parsed.select(
        "event.*",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        "raw_json",
    )
    missing_required = reduce(
        lambda left, right: left | right,
        (F.col(field).isNull() for field in REQUIRED_EVENT_FIELDS),
    )
    return flattened.withColumn("parse_error", missing_required)


def start_bronze_available_now(
    dataframe: Any,
    *,
    table_name: str,
    checkpoint_path: str,
) -> Any:
    """Start a bounded micro-batch query that stops after available data is consumed."""
    if not checkpoint_path.strip():
        raise ValueError("checkpoint_path must be non-empty.")
    return (
        dataframe.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table_name)
    )


def await_bounded_completion(query: Any, *, timeout_seconds: float) -> dict[str, Any]:
    """Wait for a streaming query for a finite time and report an honest timeout state."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive.")
    terminated = bool(query.awaitTermination(timeout_seconds))
    return {
        "terminated": terminated,
        "timed_out": not terminated,
        "timeout_seconds": timeout_seconds,
    }

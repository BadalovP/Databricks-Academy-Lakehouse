"""Spark Structured Streaming helpers for Event Hubs' Kafka endpoint."""

from __future__ import annotations

from typing import Any


def station_status_schema() -> Any:
    """Explicit event contract; new unknown fields remain outside trusted columns."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
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
    return parsed.select("event.*", "kafka_*", "ingested_at", "raw_json")


def start_bronze_available_now(
    dataframe: Any,
    *,
    table_name: str,
    checkpoint_path: str,
) -> Any:
    """Start a bounded micro-batch query that stops after available data is consumed."""
    return (
        dataframe.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table_name)
    )

# Databricks notebook source
"""UrbanFlow Bronze streaming source used by the Stage 2 notebook and future pipeline."""

from urbanflow.streaming import (
    event_hubs_kafka_options,
    parse_station_events,
    read_event_hubs_stream,
    start_bronze_available_now,
)


def build_bronze_station_stream(
    spark,
    *,
    namespace: str,
    connection_string: str,
    event_hub_name: str,
    consumer_group: str,
    max_offsets_per_trigger: int,
):
    """Return a parsed streaming DataFrame without starting billable work."""
    options = event_hubs_kafka_options(
        namespace=namespace,
        connection_string=connection_string,
        event_hub_name=event_hub_name,
        consumer_group=consumer_group,
        max_offsets_per_trigger=max_offsets_per_trigger,
    )
    return parse_station_events(read_event_hubs_stream(spark, options))


__all__ = ["build_bronze_station_stream", "start_bronze_available_now"]

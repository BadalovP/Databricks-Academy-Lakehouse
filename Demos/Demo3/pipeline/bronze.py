"""UrbanFlow Bronze Lakeflow declaration; it runs only when the pipeline is started."""

from pyspark import pipelines as dp

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


@dp.table(
    name="bronze_station_status",
    comment=(
        "Raw UrbanFlow station observations with Kafka partition, offset, broker timestamp, "
        "source JSON, and ingestion timestamp."
    ),
    table_properties={"quality": "bronze", "project": "urbanflow"},
)
def bronze_station_status():
    """Use Lakeflow-managed checkpoints and compute; GP1 and GP2 are never attached here."""
    secret_scope = spark.conf.get("urbanflow.secret_scope")
    secret_key = spark.conf.get("urbanflow.event_hubs_secret_name")
    connection_string = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    return build_bronze_station_stream(
        spark,
        namespace=spark.conf.get("urbanflow.event_hubs_namespace"),
        connection_string=connection_string,
        event_hub_name=spark.conf.get("urbanflow.event_hub_name"),
        consumer_group=spark.conf.get("urbanflow.consumer_group"),
        max_offsets_per_trigger=int(spark.conf.get("urbanflow.max_events_per_trigger")),
    )


__all__ = [
    "bronze_station_status",
    "build_bronze_station_stream",
    "start_bronze_available_now",
]

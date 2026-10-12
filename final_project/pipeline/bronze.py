"""UrbanFlow 2.0 Bronze declaration over this target's append-only Bronze Delta table."""

from pyspark import pipelines as dp


@dp.table(
    name="bronze_station_status",
    comment=("Pipeline-owned streaming copy of Final Project Bronze station observations."),
    table_properties={"quality": "bronze", "project": "urbanflow_final_project"},
)
def bronze_station_status():
    """Stream-read the configured append-only Bronze source into pipeline-owned storage."""
    source_table = spark.conf.get("urbanflow.station_bronze_source_table")  # noqa: F821
    return spark.readStream.table(source_table)  # noqa: F821


__all__ = ["bronze_station_status"]

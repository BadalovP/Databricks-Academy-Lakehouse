"""UrbanFlow Bronze Lakeflow declaration; it runs only when the pipeline is started.

The live Event Hubs messages that produced the validated station snapshot have expired from the
one-hour retention window. Re-publishing them would manufacture a second ingestion event and is
explicitly outside the Lakeflow comparison. The isolated pipeline therefore stream-reads the
existing append-only Bronze Delta table. Lakeflow still manages the streaming checkpoint and its
own target table, while the validated source remains read-only.
"""

from pyspark import pipelines as dp


@dp.table(
    name="bronze_station_status",
    comment=(
        "Isolated streaming copy of the validated UrbanFlow Bronze station observations, read "
        "from the approved main-schema Delta source without republishing Event Hubs."
    ),
    table_properties={"quality": "bronze", "project": "urbanflow"},
)
def bronze_station_status():
    """Stream-read the configured append-only Bronze source into pipeline-owned storage."""
    source_table = spark.conf.get("urbanflow.station_bronze_source_table")  # noqa: F821
    return spark.readStream.table(source_table)  # noqa: F821


__all__ = ["bronze_station_status"]

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from lab07.config import SCD_TRACKED_COLUMNS

catalog = spark.conf.get("lab07.catalog", "dbr_dev")
schema = spark.conf.get("lab07.schema", "parvinbadalov")
snapshot_count = int(spark.conf.get("lab07.snapshot_count", "3"))

feed = f"{catalog}.{schema}." "business_license_snapshot_feed"


def next_snapshot_and_version(latest_snapshot_version):
    """
    Return the next historical snapshot and its logical version.

    This follows the Databricks AUTO CDC FROM SNAPSHOT historical-source
    callback pattern: the callback returns either (DataFrame, version)
    or None when all snapshots have been processed.
    """
    version = 1 if latest_snapshot_version is None else int(latest_snapshot_version) + 1

    if version > snapshot_count:
        return None

    snapshot_df = (
        spark.read.table(feed)
        .filter(F.col("snapshot_version") == F.lit(version))
        .select(*SCD_TRACKED_COLUMNS)
    )

    return snapshot_df, version


dp.create_streaming_table("dim_license_scd2")

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_license_scd2",
    source=next_snapshot_and_version,
    keys=["license_number"],
    stored_as_scd_type=2,
)

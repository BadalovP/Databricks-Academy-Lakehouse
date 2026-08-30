"""Customer SCD Type 2 history from two full deterministic snapshots."""

from pyspark import pipelines as dp

from demo2.config import SCD2_TRACKED_COLUMNS

SNAPSHOT_VERSIONS = (20260801, 20260830)
catalog = spark.conf.get("demo2.catalog")
schema = spark.conf.get("demo2.schema")
volume_name = spark.conf.get("demo2.volume_name")
runtime_root = f"/Volumes/{catalog}/{schema}/{volume_name}/runtime"


def next_customer_snapshot(latest_snapshot_version):
    if latest_snapshot_version is None:
        version = SNAPSHOT_VERSIONS[0]
    else:
        remaining = [value for value in SNAPSHOT_VERSIONS if value > int(latest_snapshot_version)]
        if not remaining:
            return None
        version = min(remaining)
    version_text = str(version)
    snapshot_path = (
        f"{runtime_root}/source/customers/"
        f"customers_snapshot_{version_text[:4]}_{version_text[4:6]}_{version_text[6:]}.csv"
    )
    snapshot = (
        spark.read.option("header", True)
        .csv(snapshot_path)
        .select("customer_id", *SCD2_TRACKED_COLUMNS)
    )
    return snapshot, version


dp.create_streaming_table("dim_customer_scd2")

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_customer_scd2",
    source=next_customer_snapshot,
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_column_list=list(SCD2_TRACKED_COLUMNS),
)

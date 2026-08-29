from pyspark.sql import functions as F

from lab07.snapshot_policy import assert_unique_snapshot, canonical_snapshot
from tests.fixtures.late_arrivals import LATE_ARRIVALS
from tests.fixtures.snapshot_001 import SNAPSHOT_001
from tests.fixtures.snapshot_002 import SNAPSHOT_002

SNAPSHOT_SCHEMA = (
    "license_number long,license_id long,address string,date_issued string,"
    "_ingested_at string,id string"
)


def snapshot_df(spark, rows):
    return (
        spark.createDataFrame(rows, SNAPSHOT_SCHEMA)
        .withColumn("date_issued", F.to_timestamp("date_issued"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        .withColumn("license_status_change_date", F.lit(None).cast("timestamp"))
        .withColumn("license_start_date", F.col("date_issued"))
        .withColumn("payment_date", F.col("date_issued"))
        .withColumn("expiration_date", F.col("date_issued"))
    )


def test_latest_business_state_wins(spark):
    df = snapshot_df(spark, SNAPSHOT_001 + SNAPSHOT_002)
    s = canonical_snapshot(df)
    assert_unique_snapshot(s)
    rows = {row["license_number"]: row for row in s.collect()}
    assert rows[700001]["address"] == "110 STATE ST"
    assert rows[700002]["address"] == "210 MADISON ST"


def test_late_ingestion_does_not_override_newer_business_state(spark):
    s = canonical_snapshot(snapshot_df(spark, LATE_ARRIVALS))
    assert_unique_snapshot(s)
    row = s.first()
    assert row["address"] == "CURRENT BUSINESS STATE"
    assert row["id"] == "current"

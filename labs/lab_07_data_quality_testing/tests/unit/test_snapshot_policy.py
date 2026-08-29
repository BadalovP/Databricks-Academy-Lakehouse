from pyspark.sql import functions as F

from lab07.snapshot_policy import assert_unique_snapshot, canonical_snapshot


def test_latest_business_state_wins(spark):
    df = (
        spark.createDataFrame(
            [
                (700001, 1, "A", "2024-01-01", "2026-08-23", "e1"),
                (700001, 2, "B", "2025-01-01", "2026-08-23", "e2"),
            ],
            "license_number long,license_id long,address string,date_issued string,_ingested_at string,id string",
        )
        .withColumn("date_issued", F.to_timestamp("date_issued"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        .withColumn("license_status_change_date", F.lit(None).cast("timestamp"))
        .withColumn("license_start_date", F.col("date_issued"))
        .withColumn("payment_date", F.col("date_issued"))
        .withColumn("expiration_date", F.col("date_issued"))
    )
    s = canonical_snapshot(df)
    assert_unique_snapshot(s)
    assert s.first()["address"] == "B"

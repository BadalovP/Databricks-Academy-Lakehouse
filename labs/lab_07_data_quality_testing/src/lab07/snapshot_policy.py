from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def canonical_snapshot(df: DataFrame, cutoff=None, key_column="license_number") -> DataFrame:
    x = df.withColumn(
        "_business_effective_ts",
        F.coalesce(
            "license_status_change_date",
            "date_issued",
            "license_start_date",
            "payment_date",
            "expiration_date",
        ),
    ).filter(F.col(key_column).isNotNull())
    if cutoff is not None:
        x = x.filter(F.col("_business_effective_ts") <= F.to_timestamp(F.lit(str(cutoff))))
    w = Window.partitionBy(key_column).orderBy(
        F.col("_business_effective_ts").desc_nulls_last(),
        F.col("license_id").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("id").desc_nulls_last(),
    )
    return x.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")


def duplicate_snapshot_keys(df: DataFrame, key_column="license_number") -> DataFrame:
    return df.groupBy(key_column).count().filter("count>1")


def assert_unique_snapshot(df: DataFrame, key_column="license_number") -> None:
    if duplicate_snapshot_keys(df, key_column).limit(1).count():
        raise ValueError(f"Duplicate snapshot key: {key_column}")

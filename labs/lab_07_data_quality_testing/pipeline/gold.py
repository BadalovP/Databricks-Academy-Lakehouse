from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="license_quality_daily")
def quality_daily():
    classified_df = spark.read.table(
        "business_license_classified"
    )

    return (
        classified_df
        .groupBy("_ingestion_date")
        .agg(
            F.count("*").alias("total_rows"),
            F.sum(
                F.when(
                    F.col("_dq_status") != "QUARANTINE",
                    1,
                ).otherwise(0)
            ).alias("trusted_rows"),
            F.sum(
                F.when(
                    F.col("_dq_status") == "QUARANTINE",
                    1,
                ).otherwise(0)
            ).alias("quarantined_rows"),
            F.sum(
                F.when(
                    F.col("_dq_status") == "WARN",
                    1,
                ).otherwise(0)
            ).alias("warning_rows"),
            F.max("_ingested_at").alias(
                "latest_ingested_at"
            ),
            F.max("_source_dataset_updated_at").alias(
                "latest_source_updated_at"
            ),
        )
        .withColumn(
            "quality_score_pct",
            F.round(
                F.col("trusted_rows")
                / F.col("total_rows")
                * 100,
                2,
            ),
        )
        .withColumnRenamed(
            "_ingestion_date",
            "quality_date",
        )
    )


@dp.materialized_view(
    name="license_quality_by_dimension"
)
def by_dimension():
    return (
        spark.read.table(
            "business_license_classified"
        )
        .select(
            "_ingestion_date",
            "_dq_status",
            F.explode_outer(
                "_dq_dimensions"
            ).alias("dimension"),
        )
        .filter("dimension IS NOT NULL")
        .groupBy(
            "_ingestion_date",
            "dimension",
            "_dq_status",
        )
        .count()
        .withColumnRenamed(
            "_ingestion_date",
            "quality_date",
        )
    )


@dp.materialized_view(name="license_status_summary")
def status_summary():
    return (
        spark.read.table(
            "business_license_validated"
        )
        .groupBy("license_status")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct(
                "license_number"
            ).alias("distinct_licenses"),
        )
    )


@dp.materialized_view(name="license_volume_daily")
def volume_daily():
    return (
        spark.read.table(
            "business_license_validated"
        )
        .groupBy("_ingestion_date")
        .agg(
            F.count("*").alias("trusted_rows")
        )
        .withColumnRenamed(
            "_ingestion_date",
            "volume_date",
        )
    )

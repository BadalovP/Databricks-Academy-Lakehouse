"""Reusable transformation helpers for Lab 06 Gold Analytics."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_date_dimension(
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Generate an independent complete calendar dimension."""
    seed = spark.createDataFrame(
        [(start_date, end_date)],
        ["start_date", "end_date"],
    )

    return (
        seed
        .select(
            F.explode(
                F.sequence(
                    F.to_date("start_date"),
                    F.to_date("end_date"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("full_date")
        )
        .select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            (
                F.pmod(F.dayofweek("full_date") + F.lit(5), F.lit(7))
                + F.lit(1)
            ).alias("iso_day_of_week"),
            F.date_format("full_date", "yyyy-MM").alias("year_month"),
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        )
    )


def prepare_encounters(encounters_df: DataFrame) -> DataFrame:
    """Normalize encounter source columns and derive core measures."""
    return (
        encounters_df
        .select(
            F.col("Id").alias("encounter_id"),
            F.to_timestamp("START").alias("encounter_start_ts"),
            F.to_timestamp("STOP").alias("encounter_stop_ts"),
            F.col("PATIENT").alias("patient_id"),
            F.col("ORGANIZATION").alias("organization_id"),
            F.col("PROVIDER").alias("provider_id"),
            F.col("PAYER").alias("payer_id"),
            F.col("ENCOUNTERCLASS").alias("encounter_class"),
            F.col("CODE").alias("encounter_code"),
            F.col("DESCRIPTION").alias("encounter_description"),
            F.col("BASE_ENCOUNTER_COST").cast("decimal(18,2)").alias("base_encounter_cost"),
            F.col("TOTAL_CLAIM_COST").cast("decimal(18,2)").alias("total_claim_cost"),
            F.col("PAYER_COVERAGE").cast("decimal(18,2)").alias("payer_coverage"),
            F.col("REASONCODE").alias("reason_code"),
            F.col("REASONDESCRIPTION").alias("reason_description"),
        )
        .withColumn("encounter_date", F.to_date("encounter_start_ts"))
        .withColumn(
            "duration_minutes",
            (
                F.unix_timestamp("encounter_stop_ts")
                - F.unix_timestamp("encounter_start_ts")
            ) / F.lit(60.0),
        )
        .withColumn(
            "patient_responsibility",
            (
                F.col("total_claim_cost")
                - F.col("payer_coverage")
            ).cast("decimal(18,2)"),
        )
    )


def build_daily_encounters(fact_encounters_df: DataFrame) -> DataFrame:
    """Aggregate encounter facts to one row per encounter date."""
    return (
        fact_encounters_df
        .groupBy("date_key", "encounter_date")
        .agg(
            F.count("*").alias("encounter_count"),
            F.countDistinct("patient_key").alias("unique_patients"),
            F.countDistinct("organization_key").alias("organizations_active"),
            F.countDistinct("provider_key").alias("providers_active"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.sum("base_encounter_cost"), 2).alias("base_encounter_cost"),
            F.round(F.sum("total_claim_cost"), 2).alias("total_claim_cost"),
            F.round(F.sum("payer_coverage"), 2).alias("payer_coverage"),
            F.round(F.sum("patient_responsibility"), 2).alias("patient_responsibility"),
            F.sum(
                F.when(
                    F.lower("encounter_class") == "emergency",
                    1,
                ).otherwise(0)
            ).alias("emergency_encounters"),
        )
        .withColumn(
            "emergency_encounter_pct",
            F.round(
                F.col("emergency_encounters")
                / F.col("encounter_count")
                * F.lit(100.0),
                2,
            ),
        )
    )

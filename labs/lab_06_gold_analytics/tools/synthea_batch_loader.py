"""Lab 06 - Synthea monthly encounter batch loader.

Purpose
-------
Prepare the Synthea encounter source as month-partitioned landing data for Lab 06.

Design choices
--------------
- Reads from a parameterized Unity Catalog volume path.
- Does not use current_user() or workspace-specific paths.
- Uses one Spark write with partitionBy("encounter_month") instead of manually
  looping through months.
- Overwrites the preparation landing area as one idempotent source-preparation
  operation.
- Retry behavior belongs to the Databricks Job/task definition, not this script.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


DEFAULT_SOURCE_FILE = "encounters.csv"
DEFAULT_SOURCE_DIR = "source/csv"
DEFAULT_LANDING_DIR = "landing/encounters"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split Synthea encounters into monthly landing partitions."
    )

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume-name", required=True)

    parser.add_argument(
        "--source-file-name",
        default=DEFAULT_SOURCE_FILE,
    )
    parser.add_argument(
        "--source-dir",
        default=DEFAULT_SOURCE_DIR,
    )
    parser.add_argument(
        "--landing-dir",
        default=DEFAULT_LANDING_DIR,
    )

    return parser.parse_args()


def build_paths(args: argparse.Namespace) -> tuple[str, str]:
    volume_path = (
        f"/Volumes/{args.catalog}/{args.schema}/{args.volume_name}"
    )

    source_path = (
        f"{volume_path}/{args.source_dir}/{args.source_file_name}"
    )

    landing_path = (
        f"{volume_path}/{args.landing_dir}"
    )

    return source_path, landing_path


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    source_path, landing_path = build_paths(args)

    print("LAB 06 - SYNTHEA MONTHLY BATCH LOADER")
    print(f"Source  : {source_path}")
    print(f"Landing : {landing_path}")

    source_df = (
        spark.read
        .option("header", True)
        .csv(source_path)
    )

    required_columns = {
        "Id",
        "START",
        "STOP",
        "PATIENT",
        "ORGANIZATION",
        "PROVIDER",
        "PAYER",
    }

    missing_columns = sorted(
        required_columns - set(source_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "encounters.csv is missing required columns: "
            + ", ".join(missing_columns)
        )

    prepared_df = (
        source_df
        .withColumn(
            "_encounter_start_ts",
            F.to_timestamp("START"),
        )
        .withColumn(
            "encounter_month",
            F.date_format(
                F.col("_encounter_start_ts"),
                "yyyy-MM",
            ),
        )
    )

    source_count = prepared_df.count()

    invalid_start_count = (
        prepared_df
        .filter(F.col("_encounter_start_ts").isNull())
        .count()
    )

    if invalid_start_count:
        raise ValueError(
            f"{invalid_start_count} encounter rows have an invalid START timestamp."
        )

    duplicate_id_count = (
        prepared_df
        .groupBy("Id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_id_count:
        raise ValueError(
            f"{duplicate_id_count} duplicate encounter IDs were found."
        )

    month_profile_df = (
        prepared_df
        .groupBy("encounter_month")
        .agg(
            F.count("*").alias("encounter_count"),
            F.countDistinct("PATIENT").alias("unique_patients"),
        )
    )

    month_count = month_profile_df.count()

    min_max_row = (
        month_profile_df
        .agg(
            F.min("encounter_month").alias("min_month"),
            F.max("encounter_month").alias("max_month"),
        )
        .first()
    )

    output_df = prepared_df.drop("_encounter_start_ts")

    (
        output_df.write
        .mode("overwrite")
        .option("header", True)
        .partitionBy("encounter_month")
        .csv(landing_path)
    )

    reloaded_df = (
        spark.read
        .option("header", True)
        .option("basePath", landing_path)
        .csv(landing_path)
    )

    landing_count = reloaded_df.count()

    if landing_count != source_count:
        raise RuntimeError(
            "Landing reconciliation failed: "
            f"source={source_count:,}, landing={landing_count:,}"
        )

    print("")
    print("VALIDATION")
    print(f"Source encounter rows : {source_count:,}")
    print(f"Landing encounter rows: {landing_count:,}")
    print(f"Monthly partitions    : {month_count:,}")
    print(f"First month           : {min_max_row['min_month']}")
    print(f"Last month            : {min_max_row['max_month']}")
    print(f"Invalid START rows    : {invalid_start_count:,}")
    print(f"Duplicate IDs         : {duplicate_id_count:,}")
    print("")
    print("LAB 06 - MONTHLY ENCOUNTER BATCHING COMPLETE")


if __name__ == "__main__":
    main()

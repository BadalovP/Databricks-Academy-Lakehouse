# Purpose: Validate the published outputs after the Olist pipeline finishes.

import argparse

from pyspark.sql import SparkSession


def parse_arguments():
    """Read values supplied by the Lakeflow Job."""

    parser = argparse.ArgumentParser(
        description="Validate Demo2 Olist pipeline outputs."
    )

    parser.add_argument(
        "--catalog",
        default="dbr_dev",
    )
    parser.add_argument(
        "--schema",
        default="parvinbadalov",
    )
    parser.add_argument(
        "--expected-order-item-rows",
        type=int,
        default=112650,
    )
    parser.add_argument(
        "--expected-distinct-orders",
        type=int,
        default=98666,
    )
    parser.add_argument(
        "--expected-status-count",
        type=int,
        default=7,
    )
    parser.add_argument(
        "--expected-total-price",
        type=float,
        default=13591643.70,
    )
    parser.add_argument(
        "--expected-total-freight",
        type=float,
        default=2251909.54,
    )
    parser.add_argument(
        "--expected-total-value",
        type=float,
        default=15843553.24,
    )
    parser.add_argument(
        "--expected-quality-status",
        default="PASS",
    )

    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"Ignoring Databricks internal arguments: {unknown_args}")

    return args


args = parse_arguments()

spark = SparkSession.builder.getOrCreate()

table_prefix = f"{args.catalog}.{args.schema}"

print(f"Validating pipeline tables in: {table_prefix}")


# Validate the base table.
base_count = spark.table(
    f"{table_prefix}.learning_orders_base"
).count()

assert base_count == args.expected_order_item_rows, (
    f"Expected {args.expected_order_item_rows} base rows, "
    f"but found {base_count}"
)


# Validate the status aggregation.
status_count = spark.table(
    f"{table_prefix}.learning_orders_by_status"
).count()

assert status_count == args.expected_status_count, (
    f"Expected {args.expected_status_count} order statuses, "
    f"but found {status_count}"
)


# Validate the pipeline summary.
summary_df = spark.table(
    f"{table_prefix}.learning_pipeline_summary"
)

summary_count = summary_df.count()

assert summary_count == 1, (
    "learning_pipeline_summary must contain exactly one row, "
    f"but found {summary_count}"
)

summary = summary_df.first()

assert summary["order_item_rows"] == args.expected_order_item_rows, (
    f"Unexpected order_item_rows: {summary['order_item_rows']}"
)

assert summary["distinct_orders"] == args.expected_distinct_orders, (
    f"Unexpected distinct_orders: {summary['distinct_orders']}"
)

assert abs(
    float(summary["total_price"]) - args.expected_total_price
) < 0.01, (
    f"Unexpected total_price: {summary['total_price']}"
)

assert abs(
    float(summary["total_freight"]) - args.expected_total_freight
) < 0.01, (
    f"Unexpected total_freight: {summary['total_freight']}"
)

assert abs(
    float(summary["total_value"]) - args.expected_total_value
) < 0.01, (
    f"Unexpected total_value: {summary['total_value']}"
)


# Validate the final quality status.
quality_df = spark.table(
    f"{table_prefix}.learning_quality_status"
)

quality_count = quality_df.count()

assert quality_count == 1, (
    "learning_quality_status must contain exactly one row, "
    f"but found {quality_count}"
)

quality = quality_df.first()

assert quality["quality_status"] == args.expected_quality_status, (
    f"Expected quality status {args.expected_quality_status}, "
    f"but found {quality['quality_status']}"
)


print("All Demo2 Olist pipeline output validations passed.")

quality_df.show(truncate=False)
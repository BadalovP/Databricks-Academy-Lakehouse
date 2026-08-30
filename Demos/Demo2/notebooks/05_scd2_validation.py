# Databricks notebook source
"""Validate evolved schema, SCD2 history, surrogate keys, and temporal facts."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
validation_table = f"{catalog}.{schema}.demo2_validation_results"


def record(check_name, passed, details):
    spark.createDataFrame(
        [
            (
                run_id,
                check_name,
                bool(passed),
                json.dumps(details, sort_keys=True),
                datetime.now(timezone.utc),
            )
        ],
        "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
    ).write.mode("append").saveAsTable(validation_table)


bronze = spark.table(f"{catalog}.{schema}.demo2_orders_bronze")
evolved_columns = bronze.columns
v2_non_null = (
    bronze.filter("_source_batch_id = 'DEMO2_V2_SCHEMA_EVOLUTION'")
    .filter("sales_channel IS NOT NULL AND coupon_code IS NOT NULL")
    .count()
)
schema_passed = (
    all(name in evolved_columns for name in ("sales_channel", "coupon_code")) and v2_non_null == 100
)
record("schema_evolution", schema_passed, {"columns": evolved_columns, "v2_non_null": v2_non_null})

history = spark.table(f"{catalog}.{schema}.dim_customer_scd2")
history_counts = {
    row["customer_id"]: row["count"] for row in history.groupBy("customer_id").count().collect()
}
expected_changed = {"C001", "C003", "C006"}
scd_passed = all(history_counts.get(customer) == 2 for customer in expected_changed) and all(
    history_counts.get(customer) == 1 for customer in set(history_counts) - expected_changed
)
tracked_columns = {"customer_name", "email", "country", "city", "loyalty_tier"}
technical_absent = not {
    "snapshot_version",
    "_source_file",
    "_batch_loaded_at",
    "_ingested_at",
}.intersection(history.columns)
scd_passed = scd_passed and tracked_columns.issubset(history.columns) and technical_absent
record("scd2", scd_passed, {"history_counts": history_counts, "columns": history.columns})

facts = spark.table(f"{catalog}.{schema}.fact_order_lines")
trusted_count = spark.table(f"{catalog}.{schema}.demo2_orders_validated").count()
null_keys = facts.filter("customer_key IS NULL OR product_key IS NULL OR date_key IS NULL").count()
duplicate_fact_keys = facts.groupBy("order_line_id").count().filter("count > 1").count()
temporal_passed = facts.count() == trusted_count and null_keys == 0 and duplicate_fact_keys == 0
record(
    "temporal_fact",
    temporal_passed,
    {
        "fact_count": facts.count(),
        "trusted_count": trusted_count,
        "null_keys": null_keys,
        "duplicate_keys": duplicate_fact_keys,
    },
)

if not (schema_passed and scd_passed and temporal_passed):
    raise AssertionError("Schema evolution, SCD2, or temporal-fact validation failed")
print("Schema evolution, SCD2, and temporal fact validation passed")

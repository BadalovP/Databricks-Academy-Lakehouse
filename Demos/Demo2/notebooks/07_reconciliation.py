# Databricks notebook source
"""Reconcile physical V2 rows and trusted business rows across medallion layers."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
prefix = f"{catalog}.{schema}"
validation_table = f"{prefix}.demo2_validation_results"
batch = "DEMO2_V2_SCHEMA_EVOLUTION"

bronze_count = (
    spark.table(f"{prefix}.demo2_orders_bronze").filter(f"_source_batch_id = '{batch}'").count()
)
classified = spark.table(f"{prefix}.demo2_orders_classified").filter(
    f"_source_batch_id = '{batch}'"
)
counts = {
    row["_dq_status"]: row["count"] for row in classified.groupBy("_dq_status").count().collect()
}
valid_count = counts.get("VALID", 0)
warn_count = counts.get("WARN", 0)
quarantine_count = counts.get("QUARANTINE", 0)
trusted_count = valid_count + warn_count
fact = spark.table(f"{prefix}.fact_order_lines").filter(f"_source_batch_id = '{batch}'")
fact_count = fact.count()
trusted_duplicates = (
    classified.filter("_dq_status IN ('VALID', 'WARN')")
    .groupBy("order_line_id")
    .count()
    .filter("count > 1")
    .count()
)
fact_duplicates = fact.groupBy("order_line_id").count().filter("count > 1").count()
orphan_counts = {
    "customer": fact.filter("customer_key IS NULL").count(),
    "product": fact.filter("product_key IS NULL").count(),
    "date": fact.filter("date_key IS NULL").count(),
}

passed = (
    bronze_count == 100
    and valid_count == 92
    and warn_count == 2
    and quarantine_count == 6
    and bronze_count == trusted_count + quarantine_count
    and trusted_count == fact_count
    and trusted_duplicates == 0
    and fact_duplicates == 0
    and all(value == 0 for value in orphan_counts.values())
)
details = {
    "bronze": bronze_count,
    "valid": valid_count,
    "warn": warn_count,
    "trusted": trusted_count,
    "quarantine": quarantine_count,
    "fact": fact_count,
    "trusted_duplicates": trusted_duplicates,
    "fact_duplicates": fact_duplicates,
    "orphans": orphan_counts,
}
spark.createDataFrame(
    [
        (
            run_id,
            "reconciliation",
            passed,
            json.dumps(details, sort_keys=True),
            datetime.now(timezone.utc),
        )
    ],
    "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
).write.mode("append").saveAsTable(validation_table)
if not passed:
    raise AssertionError(f"Demo 2 reconciliation failed: {details}")
print(details)

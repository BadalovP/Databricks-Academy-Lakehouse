# Databricks notebook source
"""Validate the deterministic latest-batch quarantine-rate alert condition."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
prefix = f"{catalog}.{schema}"

latest = spark.sql(
    f"""
    SELECT *
    FROM {prefix}.demo2_dq_summary_gold
    ORDER BY _batch_loaded_at DESC, _source_batch_id DESC
    LIMIT 1
    """
).first()
details = latest.asDict()
passed = (
    details["_source_batch_id"] == "DEMO2_V2_SCHEMA_EVOLUTION"
    and details["total_rows"] == 100
    and details["quarantined_rows"] == 6
    and float(details["quarantine_rate_pct"]) == 6.0
    and float(details["quarantine_rate_pct"]) > 5.0
)
details["alert_condition"] = "quarantine_rate_pct > 5"
details["alert_triggered"] = passed
spark.createDataFrame(
    [
        (
            run_id,
            "alert",
            passed,
            json.dumps(details, sort_keys=True, default=str),
            datetime.now(timezone.utc),
        )
    ],
    "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
).write.mode("append").saveAsTable(f"{prefix}.demo2_validation_results")
if not passed:
    raise AssertionError(f"Demo 2 alert validation failed: {details}")
print(details)

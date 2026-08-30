# Databricks notebook source
"""Fail unless every persisted Demo 2 validation result passed."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
validation_table = f"{catalog}.{schema}.demo2_validation_results"
required = {
    "v1_schema",
    "schema_evolution",
    "scd2",
    "temporal_fact",
    "governance",
    "governance_cleanup",
    "reconciliation",
    "alert",
}
latest = spark.sql(
    f"""
    SELECT check_name, passed, details
    FROM (
      SELECT *, row_number() OVER (PARTITION BY check_name ORDER BY checked_at DESC) AS rn
      FROM {validation_table}
      WHERE run_id = '{run_id}'
    )
    WHERE rn = 1
    """
).collect()
by_name = {row["check_name"]: row.asDict() for row in latest}
missing = sorted(required - set(by_name))
failed = sorted(name for name in required if name in by_name and not by_name[name]["passed"])
passed = not missing and not failed
details = {"required": sorted(required), "missing": missing, "failed": failed}
spark.createDataFrame(
    [
        (
            run_id,
            "final_gate",
            passed,
            json.dumps(details, sort_keys=True),
            datetime.now(timezone.utc),
        )
    ],
    "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
).write.mode("append").saveAsTable(validation_table)
if not passed:
    raise AssertionError(f"Demo 2 final gate failed: {details}")
print("Demo 2 final gate passed")

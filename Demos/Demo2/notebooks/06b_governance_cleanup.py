# Databricks notebook source
"""Idempotently remove only the Demo 2 governance validation probe."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
access_table = f"{catalog}.{schema}.demo2_user_country_access"
validation_table = f"{catalog}.{schema}.demo2_validation_results"

exists = spark.catalog.tableExists(access_table)
if exists:
    spark.sql(f"DELETE FROM {access_table} WHERE is_cleanup_probe = true")
    remaining = spark.table(access_table).filter("is_cleanup_probe = true").count()
else:
    remaining = 0

passed = remaining == 0
spark.createDataFrame(
    [
        (
            run_id,
            "governance_cleanup",
            passed,
            json.dumps({"remaining_probes": remaining}),
            datetime.now(timezone.utc),
        )
    ],
    "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
).write.mode("append").saveAsTable(validation_table)
if not passed:
    raise AssertionError("Demo 2 governance cleanup failed")
print("Governance cleanup passed")

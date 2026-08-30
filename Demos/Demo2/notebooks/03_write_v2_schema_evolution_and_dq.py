# Databricks notebook source
"""Prove the V1 Bronze schema, then land the controlled V2 batch."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from demo2.data_generation import generate_v2_orders, write_json_lines

for name, default in (
    ("catalog", "dbr_dev"),
    ("schema", "parvinbadalov"),
    ("volume_name", "demo2_ecommerce"),
    ("run_id", "manual"),
):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")
run_id = dbutils.widgets.get("run_id")

validation_table = f"{catalog}.{schema}.demo2_validation_results"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {validation_table} (
      run_id STRING,
      check_name STRING,
      passed BOOLEAN,
      details STRING,
      checked_at TIMESTAMP
    ) USING DELTA
    """
)


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


bronze = f"{catalog}.{schema}.demo2_orders_bronze"
v1_columns = spark.table(bronze).columns
v1_passed = "sales_channel" not in v1_columns and "coupon_code" not in v1_columns
record("v1_schema", v1_passed, {"columns": v1_columns})
if not v1_passed:
    raise AssertionError("V1 Bronze schema unexpectedly contains evolved columns")

v2_rows = generate_v2_orders()
if len(v2_rows) != 100:
    raise AssertionError("V2 must contain exactly 100 physical rows")
if not all(row.get("sales_channel") and row.get("coupon_code") for row in v2_rows):
    raise AssertionError("V2 evolved fields must be present and non-null")

v2_path = Path(
    f"/Volumes/{catalog}/{schema}/{volume_name}/runtime/landing/orders/orders_v2_20260901_100000.json"
)
write_json_lines(v2_path, v2_rows)
print(f"V1 schema verified: {v1_columns}")
print(f"V2 rows landed: {len(v2_rows)}")

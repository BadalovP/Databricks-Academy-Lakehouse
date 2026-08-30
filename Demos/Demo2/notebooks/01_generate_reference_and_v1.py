# Databricks notebook source
"""Generate deterministic reference snapshots, products, and V1 orders only."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))

from demo2.config import validate_runtime_configuration
from demo2.data_generation import generate_v1_orders, write_json_lines, write_reference_data

for name in (
    "bundle_target",
    "catalog",
    "schema",
    "volume_name",
    "expected_catalog",
    "expected_schema",
    "expected_volume_name",
):
    dbutils.widgets.text(name, "")

bundle_target = dbutils.widgets.get("bundle_target")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")
expected_catalog = dbutils.widgets.get("expected_catalog")
expected_schema = dbutils.widgets.get("expected_schema")
expected_volume_name = dbutils.widgets.get("expected_volume_name")

validate_runtime_configuration(
    bundle_target=bundle_target,
    catalog=catalog,
    schema=schema,
    volume_name=volume_name,
    expected_catalog=expected_catalog,
    expected_schema=expected_schema,
    expected_volume_name=expected_volume_name,
)

root = Path(f"/Volumes/{catalog}/{schema}/{volume_name}/runtime")
reference_paths = write_reference_data(root)
v1_path = root / "landing/orders/orders_v1_20260901_090000.json"
v1_rows = generate_v1_orders()
if any("sales_channel" in row or "coupon_code" in row for row in v1_rows):
    raise AssertionError("V1 evolved keys must be absent")
write_json_lines(v1_path, v1_rows)

print(f"Reference files: {len(reference_paths)}")
print(f"V1 rows: {len(v1_rows)}")
print(f"V1 landing file: {v1_path}")

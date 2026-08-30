# Databricks notebook source
"""Safely reset only the fixed Demo 2 runtime subtree."""

import sys
from pathlib import Path, PurePosixPath

from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path.cwd().parent / "src"))

dbutils.widgets.text("catalog", "dbr_dev")
dbutils.widgets.text("schema", "parvinbadalov")
dbutils.widgets.text("volume_name", "demo2_ecommerce")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("run_id", "manual")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")
pipeline_id = dbutils.widgets.get("pipeline_id")

if (catalog, schema, volume_name) != ("dbr_dev", "parvinbadalov", "demo2_ecommerce"):
    raise RuntimeError("Demo 2 setup refuses any catalog, schema, or volume override")

runtime_root = f"/Volumes/{catalog}/{schema}/{volume_name}/runtime"
expected_parts = (
    "/",
    "Volumes",
    "dbr_dev",
    "parvinbadalov",
    "demo2_ecommerce",
    "runtime",
)
if PurePosixPath(runtime_root).parts != expected_parts:
    raise RuntimeError(f"Unsafe Demo 2 reset path: {runtime_root}")

volume_rows = spark.sql(
    f"""
    SELECT volume_type, storage_location
    FROM {catalog}.information_schema.volumes
    WHERE volume_catalog = '{catalog}'
      AND volume_schema = '{schema}'
      AND volume_name = '{volume_name}'
    """
).collect()
if len(volume_rows) != 1 or volume_rows[0]["volume_type"] != "EXTERNAL":
    raise RuntimeError("The required Demo 2 external volume is missing or not external")
storage_location = str(volume_rows[0]["storage_location"]).rstrip("/")
if not storage_location.endswith("/demo2_ecommerce"):
    raise RuntimeError(f"Unexpected Demo 2 storage location: {storage_location}")

if pipeline_id:
    details = WorkspaceClient().pipelines.get(pipeline_id=pipeline_id)
    active_states = {
        "CREATED",
        "WAITING_FOR_RESOURCES",
        "INITIALIZING",
        "RESETTING",
        "SETTING_UP_TABLES",
        "RUNNING",
        "STOPPING",
    }
    active_updates = [
        update
        for update in (details.latest_updates or [])
        if str(update.state).split(".")[-1] in active_states
    ]
    if active_updates:
        raise RuntimeError("Demo 2 setup refuses to reset while its pipeline has an active update")

dbutils.fs.rm(runtime_root, recurse=True)
for child in (
    "source/customers",
    "source/products",
    "landing/orders",
    "system/schemas/orders",
    "system/checkpoints/orders",
):
    dbutils.fs.mkdirs(f"{runtime_root}/{child}")

for object_name, object_type in (
    ("demo2_sales_governed", "VIEW"),
    ("demo2_user_country_access", "TABLE"),
    ("demo2_validation_results", "TABLE"),
):
    spark.sql(f"DROP {object_type} IF EXISTS {catalog}.{schema}.{object_name}")

print(f"Verified external volume: {storage_location}")
print(f"Reset only: {runtime_root}")

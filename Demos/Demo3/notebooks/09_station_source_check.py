# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Station Bronze Source Check
# MAGIC
# MAGIC **Business context:** The unified Job reuses the validated 2,520-row Bronze snapshot because the original Event Hubs retention window has passed.
# MAGIC
# MAGIC **Prerequisites:** Preflight passed on GP1 and `bronze_station_status` already contains the selected execution.
# MAGIC
# MAGIC **Learning objectives:** Verify source lineage, required business IDs and uniqueness before the Silver write begins.
# MAGIC
# MAGIC **Academy labs:** Labs 4, 7, 8 and 9.
# MAGIC
# MAGIC **Safety:** This task runs only `tableExists`, filtered `SELECT` and aggregate actions. It never invokes the producer or writes a table.
# MAGIC
# MAGIC **Actual validation:** This unified source-check notebook has not been executed on Azure; the same snapshot counts were independently validated read-only on 2026-10-03.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Resolve the selected Bronze scope
# MAGIC
# MAGIC **What:** Read the station branch switch and construct the fully qualified Bronze table name from target parameters.
# MAGIC
# MAGIC **Why:** The task must select one execution explicitly and must not fall back to a hard-coded development schema.
# MAGIC
# MAGIC **Input:** `run_station_pipeline`, `source_execution_id`, catalog and schema.
# MAGIC
# MAGIC **Output:** One filtered Bronze DataFrame, or a clean skip when the station branch is disabled.
# MAGIC
# MAGIC **Key concepts:** Execution-scoped reads, target isolation, optional branch behavior.
# MAGIC
# MAGIC **Expected result:** Sample release selects `urbanflow-20260929T195132Z-r3` from the Azure target.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Before transforming anything, we prove the exact Bronze snapshot still exists."
# MAGIC
# MAGIC **Rerun and cost considerations:** The query is bounded to one existing execution and performs no write.

# COMMAND ----------

import json

from pyspark.sql import functions as F

for name in ("run_station_pipeline", "source_execution_id", "catalog", "schema"):
    dbutils.widgets.text(name, "")
run_station = dbutils.widgets.get("run_station_pipeline").strip().lower() == "true"
source_execution_id = dbutils.widgets.get("source_execution_id").strip()
target_catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
if not run_station:
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "station branch disabled"}))
for label, value in (("catalog", target_catalog), ("schema", target_schema)):
    if not value.isidentifier():
        raise ValueError(f"{label} {value!r} is not a plain identifier.")
if not source_execution_id:
    raise ValueError("source_execution_id is required.")
table_name = f"{target_catalog}.{target_schema}.bronze_station_status"
if not spark.catalog.tableExists(table_name):
    raise RuntimeError(f"Required Bronze table {table_name} does not exist.")
source = spark.table(table_name).where(F.col("execution_id") == F.lit(source_execution_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Prove the snapshot contract
# MAGIC
# MAGIC **What:** Count rows, distinct event IDs and null business identifiers for the selected execution.
# MAGIC
# MAGIC **Why:** A partial or duplicated Bronze scope would make every downstream reconciliation look plausible while being wrong.
# MAGIC
# MAGIC **Input:** The filtered Bronze DataFrame.
# MAGIC
# MAGIC **Output:** A `PASS` JSON result, or a raised error that blocks Silver and Gold.
# MAGIC
# MAGIC **Key concepts:** Business-key uniqueness, null checks, lineage reconciliation.
# MAGIC
# MAGIC **Expected result:** 2,520 rows, 2,520 distinct event IDs and zero null event, station or execution IDs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The source gate catches a missing or duplicated snapshot before any durable transformation runs."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are read-only aggregates over one bounded execution.

# COMMAND ----------

metrics = (
    source.agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("null_event_ids"),
        F.sum(F.col("station_id").isNull().cast("int")).alias("null_station_ids"),
        F.sum(F.col("execution_id").isNull().cast("int")).alias("null_execution_ids"),
    )
    .first()
    .asDict()
)
expected = {
    "rows": 2520,
    "distinct_event_ids": 2520,
    "null_event_ids": 0,
    "null_station_ids": 0,
    "null_execution_ids": 0,
}
failures = {
    name: {"expected": value, "actual": metrics[name]}
    for name, value in expected.items()
    if metrics[name] != value
}
if failures:
    raise RuntimeError(f"Bronze source contract failed for {source_execution_id}: {failures}")
report = {"status": "PASS", "execution_id": source_execution_id, "table": table_name, **metrics}
print(report)
dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Reusing an existing Bronze snapshot is safe only when its lineage and business keys are rechecked at the start of every orchestration run. This task turns an old execution ID into a current, measured precondition.
# MAGIC
# MAGIC **Common errors:** Reading the whole Bronze table, trusting an execution ID without checking row count, or republishing Event Hubs data to recreate evidence that already exists.
# MAGIC
# MAGIC **Troubleshooting:** If the count differs, stop and inspect the named execution read-only. Do not substitute another execution or start the producer during the release.
# MAGIC
# MAGIC **Review questions:** Why is distinct `event_id` compared with row count? Why is the execution filter expressed with Spark columns rather than SQL interpolation?
# MAGIC
# MAGIC **Presentation summary:** "The unified Job starts from the preserved 2,520-row Bronze lineage and proves it is complete before Silver runs."

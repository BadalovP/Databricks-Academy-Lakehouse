# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Station Bronze source check
# MAGIC
# MAGIC **Business context:** Silver and Gold must start from a complete, uniquely keyed Bronze execution. Otherwise every downstream reconciliation would look plausible while being wrong.
# MAGIC
# MAGIC **Prerequisites:** Preflight passed on serverless compute, and `bronze_station_status` contains the selected execution. That execution was landed by `land_sources` (`station_source=sample_autoloader`) or already ingested from Event Hubs (`existing_bronze`).
# MAGIC
# MAGIC **Learning objectives:** Verify source lineage, required business IDs and uniqueness before the Silver write begins, with an exact size where the source has one.
# MAGIC
# MAGIC **Academy labs:** Labs 3, 4, 7 and 9.
# MAGIC
# MAGIC **Safety:** This task runs only `tableExists`, a filtered `SELECT` and aggregate actions. It never invokes the producer or writes a table.
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-12 (Final Project run 896549012431425, task `station_source_check`): 40 rows = 40 distinct event IDs, zero null keys. The first DEV run (304828008837365) failed here on a fixed Demo 3 count; the contract now derives the size from the sample.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Resolve the selected Bronze scope
# MAGIC
# MAGIC **What:** Read the station branch switch, the station source and the target, and filter Bronze to the one selected execution.
# MAGIC
# MAGIC **Why:** The task must select one execution explicitly and must not fall back to a hard-coded schema or a remembered size.
# MAGIC
# MAGIC **Input:** `run_station_pipeline`, `station_source`, `source_execution_id`, catalog and schema.
# MAGIC
# MAGIC **Output:** One filtered Bronze DataFrame, or a clean skip when the station branch is disabled.
# MAGIC
# MAGIC **Key concepts:** Execution-scoped reads, target isolation, optional branch behavior.
# MAGIC
# MAGIC **Expected result:** The selected execution has Bronze rows - one per sample station for a sample run.
# MAGIC
# MAGIC **How to explain it:** "Before transforming anything, we prove the exact Bronze execution exists."
# MAGIC
# MAGIC **Rerun and cost considerations:** The query is bounded to one existing execution and performs no write.

# COMMAND ----------

import json
import sys
from pathlib import Path

from pyspark.sql import functions as F

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
from urbanflow.landing import expected_sample_profile, station_bronze_failures

names = ("run_station_pipeline", "station_source", "source_execution_id", "catalog", "schema")
for name in names:
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name).strip() for name in names}
if values["run_station_pipeline"].lower() != "true":
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "station branch disabled"}))
for label in ("catalog", "schema"):
    if not values[label].isidentifier():
        raise ValueError(f"{label} {values[label]!r} is not a plain identifier.")
if not values["source_execution_id"]:
    raise ValueError("source_execution_id is required.")
table_name = f"{values['catalog']}.{values['schema']}.bronze_station_status"
if not spark.catalog.tableExists(table_name):
    raise RuntimeError(f"Required Bronze table {table_name} does not exist.")
source = spark.table(table_name).where(
    F.col("execution_id") == F.lit(values["source_execution_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Prove the Bronze contract
# MAGIC
# MAGIC **What:** Count rows, distinct event IDs and null business identifiers for the selected execution, and compare them with the contract for its source.
# MAGIC
# MAGIC **Why:** A partial or duplicated Bronze scope must stop the run before Silver.
# MAGIC
# MAGIC **Input:** The filtered Bronze DataFrame and, for the sample path, the committed station-status sample.
# MAGIC
# MAGIC **Output:** A `PASS` report, or a raised error that blocks Silver and Gold.
# MAGIC
# MAGIC **Key concepts:** Business-key uniqueness, null checks, an expected size derived from the source rather than remembered.
# MAGIC
# MAGIC **Expected result:** Sample path: rows = distinct event IDs = one per sample station (40), with zero null keys. Event Hubs path: rows > 0 and all event IDs distinct.
# MAGIC
# MAGIC **How to explain it:** "The source gate catches a missing or duplicated execution before any durable transformation runs."
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
profile = None
if values["station_source"] == "sample_autoloader":
    sample = PROJECT_ROOT / "fixtures" / "samples" / "station_status.sample.json"
    profile = expected_sample_profile(json.loads(sample.read_text(encoding="utf-8")))
failures = station_bronze_failures(values["station_source"], metrics, profile)
if failures:
    raise RuntimeError(
        f"Bronze source contract failed for {values['source_execution_id']}: {failures}"
    )
report = {"status": "PASS", "execution_id": values["source_execution_id"], "table": table_name,
          "station_source": values["station_source"], **metrics}  # fmt: skip
print(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Return the source report
# MAGIC
# MAGIC **What:** Return the contract report to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The report from step 2.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `PASS`.
# MAGIC
# MAGIC **How to explain it:** "The run records the exact Bronze scope it started from."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A Bronze execution is trusted only after its lineage and business keys are checked at the start of every run, against a size derived from its source. A remembered count from another project is a latent failure: here it stopped the first live run.
# MAGIC
# MAGIC **Common errors:** Reading the whole Bronze table, hard-coding a row count from an earlier environment, or trusting an execution ID without checking uniqueness.
# MAGIC
# MAGIC **Troubleshooting:** If the count differs, inspect the named execution read-only and the `land_sources` report. Do not substitute another execution.
# MAGIC
# MAGIC **Review questions:** Why is distinct `event_id` compared with row count? Why does the Event Hubs path have no fixed size?
# MAGIC
# MAGIC **Presentation summary:** "Silver starts only from a complete, uniquely keyed Bronze execution whose size comes from its source."

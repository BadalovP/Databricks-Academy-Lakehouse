# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Optional: Zerobus station-health telemetry
# MAGIC
# MAGIC **Business context:** Dock controllers could push health readings (battery, vibration, temperature) straight into the Lakehouse without a message broker. Zerobus Ingest writes such records directly to a Unity Catalog Delta table. This path is optional; the platform does not depend on it.
# MAGIC
# MAGIC **Prerequisites:** Serverless compute; the target schema exists. Sending records requires the Final Project's own service principal and `scripts/run_zerobus_health.py --send`. Lab 11's service principal, schemas and tables are never used and are refused in code. This notebook is not part of the platform Job.
# MAGIC
# MAGIC **Learning objectives:** Prepare a Zerobus target table whose columns match the producer's records, then turn at-least-once deliveries into an exactly-once Silver table and prove it per run.
# MAGIC
# MAGIC **Academy labs:** Lab 11 (Zerobus Ingest, at-least-once delivery, idempotent Silver), Lab 4 (MERGE), Lab 7 (validation as a gate).
# MAGIC
# MAGIC **Actual validation:** Offline tests cover the event contract, replay batches, the Lab 11 isolation guard and every SQL statement; in the Final Project this notebook has not been run in a workspace yet. Lab 11's own live runs are Lab 11 evidence, not Final Project evidence.
# MAGIC
# MAGIC All health readings are **synthetic** and labelled `synthetic = true`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the project code and read parameters
# MAGIC
# MAGIC **What:** Import the Zerobus helpers and read the step to run, the target and the producer run ID.
# MAGIC
# MAGIC **Why:** Nothing happens unless a step is chosen explicitly.
# MAGIC
# MAGIC **Input:** Widgets `expected_host`, `catalog`, `schema`, `zerobus_step` (`prepare` or `materialize`), `run_id`, `expected_events`.
# MAGIC
# MAGIC **Output:** Validated settings.
# MAGIC
# MAGIC **Key concepts:** Opt-in steps, run-scoped validation.
# MAGIC
# MAGIC **Expected result:** Settings are printed; an empty step leads to `SKIPPED` in step 2.
# MAGIC
# MAGIC **How to explain it:** "The optional path runs only when someone asks for a specific step."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow import zerobus_sql
from urbanflow.automation import assert_serverless_compute
from urbanflow.rag.runtime import assert_workspace

names = ("expected_host", "catalog", "schema", "zerobus_step", "run_id", "expected_events")
for name in names:
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name).strip() for name in names}
step = values["zerobus_step"]
if step not in ("", "prepare", "materialize"):
    raise ValueError("zerobus_step must be empty, 'prepare' or 'materialize'.")
print({key: value for key, value in values.items() if key != "expected_host"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Skip when no step is requested
# MAGIC
# MAGIC **What:** End the notebook with `SKIPPED` when `zerobus_step` is empty.
# MAGIC
# MAGIC **Why:** `dbutils.notebook.exit` discards the output of the cell that calls it, so it has its own cell.
# MAGIC
# MAGIC **Input:** The chosen step.
# MAGIC
# MAGIC **Output:** A `SKIPPED` result, or nothing.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults.
# MAGIC
# MAGIC **Expected result:** `SKIPPED` by default.
# MAGIC
# MAGIC **How to explain it:** "By default the optional path does nothing at all."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

if not step:
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "zerobus_step is empty"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Confirm the workspace, compute and Final Project ownership
# MAGIC
# MAGIC **What:** Check the workspace host and serverless compute, and build the table names through the isolation guard.
# MAGIC
# MAGIC **Why:** A Lab 11 schema or table, or a schema that is not a Final Project schema, is refused before any statement runs.
# MAGIC
# MAGIC **Input:** `expected_host`, `catalog`, `schema`.
# MAGIC
# MAGIC **Output:** The Bronze and Silver table names.
# MAGIC
# MAGIC **Key concepts:** Isolation between labs and the Final Project, serverless-only compute.
# MAGIC
# MAGIC **Expected result:** `zerobus_station_health_bronze` and `silver_station_health` in the Final Project schema.
# MAGIC
# MAGIC **How to explain it:** "The Final Project cannot write to Lab 11's tables even by mistake."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

assert_workspace(values["expected_host"])
assert_serverless_compute(spark.conf.get)
tables = zerobus_sql.HealthTables(values["catalog"], values["schema"])
print({"bronze": tables.bronze, "silver": tables.silver})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Prepare the tables, or materialize and validate Silver
# MAGIC
# MAGIC **What:** `prepare` creates both tables if missing. `materialize` MERGEs Bronze into Silver twice and validates the run.
# MAGIC
# MAGIC **Why:** Zerobus needs the target table to exist before a producer sends. The second MERGE proves idempotency: on unchanged Bronze it must affect no rows.
# MAGIC
# MAGIC **Input:** The tables and, for `materialize`, the producer's `run_id` and optional `expected_events`.
# MAGIC
# MAGIC **Output:** A report with MERGE metrics and per-run counts.
# MAGIC
# MAGIC **Key concepts:** Earliest-delivery-wins deduplication, delivery counts, idempotent MERGE.
# MAGIC
# MAGIC **Expected result:** Silver rows equal distinct events, no duplicate keys, delivery counts equal Bronze rows, and the second MERGE affects 0 rows.
# MAGIC
# MAGIC **How to explain it:** "Zerobus may deliver a record twice; Silver still holds it once and remembers how often it arrived."
# MAGIC
# MAGIC **Rerun and cost considerations:** Small serverless SQL; safe to rerun.

# COMMAND ----------

if step == "prepare":
    for statement in zerobus_sql.create_tables(tables):
        spark.sql(statement)
    report = {"status": "PREPARED", "bronze": tables.bronze, "silver": tables.silver}
else:
    first = spark.sql(zerobus_sql.merge_silver(tables)).first().asDict()
    second = spark.sql(zerobus_sql.merge_silver(tables)).first().asDict()
    run_id = values["run_id"]
    counts = spark.sql(zerobus_sql.validation(tables, run_id=run_id)).first().asDict()
    expected = int(values["expected_events"]) if values["expected_events"] else None
    problems = zerobus_sql.validation_problems(counts, expected_events=expected)
    if second.get("num_affected_rows", 0):
        problems.append(f"the repeated MERGE changed rows: {second}")
    if problems:
        raise AssertionError(f"Zerobus Silver validation failed: {problems} {counts}")
    report = {"status": "PASS", "run_id": run_id, "first_merge": first,
              "repeat_merge": second, **counts}  # fmt: skip
print(json.dumps(report, default=str, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Return the report
# MAGIC
# MAGIC **What:** Return the report to the caller.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept.
# MAGIC
# MAGIC **Input:** The report from step 4.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `PREPARED` or `PASS`.
# MAGIC
# MAGIC **How to explain it:** "Each step leaves a checkable result."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, default=str, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Zerobus Ingest gives at-least-once delivery straight into Delta; a stable `event_id`, an earliest-delivery-wins MERGE and per-run validation turn that into exactly-once Silver data.
# MAGIC
# MAGIC **Common errors:** Bronze columns that do not match the record fields; treating a locally enqueued record as delivered before its acknowledgment; reusing another project's service principal or tables.
# MAGIC
# MAGIC **Troubleshooting:** A schema-mismatch error from the producer means the Bronze DDL and the record differ - compare them field by field; a delivery-total mismatch means Bronze has rows from an unexpected run.
# MAGIC
# MAGIC **Review questions:** Why does the replay keep the original `event_id`? Why is the MERGE run twice?
# MAGIC
# MAGIC **Presentation summary:** An optional, isolated push-ingestion path whose duplicates are proven harmless.

# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Bounded Bronze to Silver Persistence
# MAGIC
# MAGIC **Business context:** The verified 2,520-row Bronze snapshot must become durable, quality-controlled Silver data without republishing Event Hubs messages.
# MAGIC
# MAGIC **Prerequisites:** The existing Bronze table and managed Volume exist, the Phase 2 Job has explicit approval, and GP1 or GP2 is already `RUNNING`.
# MAGIC
# MAGIC **Learning objectives:** Apply an explicit contract, preserve quarantine reasons and duplicates, use deterministic arrival order, MERGE idempotently, and prove Bronze reconciliation.
# MAGIC
# MAGIC **Academy labs:** Labs 4, 5, 7, 8, and 9.
# MAGIC
# MAGIC **Safety:** `run_transform` defaults to `false`; the notebook exits before reading or writing tables unless approval supplies `true`, and it never changes cluster state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the tested transformation and persistence helpers
# MAGIC
# MAGIC **What:** Add the synchronized project package to Python and import Silver, reporting, and compute-safety helpers.
# MAGIC
# MAGIC **Why:** The notebook should orchestrate reviewed functions rather than hide quality rules in presentation code.
# MAGIC
# MAGIC **Input:** Bundle-synchronized `src/urbanflow` modules.
# MAGIC
# MAGIC **Output:** Imported functions only; no Spark action occurs.
# MAGIC
# MAGIC **Key concepts:** Modular design, tested transformations, shared safety constants.
# MAGIC
# MAGIC **Expected result:** Imports complete without reading Bronze.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The notebook is a visible orchestration layer over unit-tested business rules."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports are repeatable and cause no data or resource changes.

# COMMAND ----------

import sys
import time
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyspark.sql import functions as F

from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS
from urbanflow.reporting import write_json_report
from urbanflow.silver import (
    freshness,
    persist_silver_outputs,
    reconcile_silver,
    split_silver_and_quarantine,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and fail closed before any Spark action
# MAGIC
# MAGIC **What:** Declare the approval gate, source execution ID, and bundle target identifiers, then verify the current cluster ID.
# MAGIC
# MAGIC **Why:** A default Job run must not read or write live tables, and only the two academy clusters are approved attachment targets.
# MAGIC
# MAGIC **Input:** Job parameters supplied by the bundle.
# MAGIC
# MAGIC **Output:** Validated values, or an immediate dry-run exit.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, existing-cluster allowlist, target isolation.
# MAGIC
# MAGIC **Expected result:** The committed default exits with `DRY_RUN`; an approved run continues only on GP1 or GP2.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "No table action happens until one visible parameter and the cluster allowlist both pass."
# MAGIC
# MAGIC **Rerun and cost considerations:** Exiting before a Spark action minimizes accidental compute work; this notebook never starts or stops a cluster.

# COMMAND ----------

dbutils.widgets.dropdown("run_transform", "false", ["false", "true"])
dbutils.widgets.text("source_execution_id", "urbanflow-20260929T195132Z-r3")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
run_transform = dbutils.widgets.get("run_transform").lower() == "true"
source_execution_id = dbutils.widgets.get("source_execution_id").strip()
target_catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
target_volume = dbutils.widgets.get("volume").strip()
if not run_transform:
    dbutils.notebook.exit("DRY_RUN: no Bronze read and no Delta write occurred.")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
if cluster_id not in APPROVED_RUN_CLUSTER_IDS:
    raise RuntimeError(f"Cluster {cluster_id!r} is not approved for UrbanFlow.")
if not source_execution_id or source_execution_id == "NOT_APPROVED":
    raise ValueError("source_execution_id must identify the verified Bronze snapshot.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Resolve isolated tables and evidence paths
# MAGIC
# MAGIC **What:** Validate the three Unity Catalog identifiers and build the Bronze, Silver, Quarantine, duplicate, and report names.
# MAGIC
# MAGIC **Why:** Development and Azure targets must never share tables or evidence paths.
# MAGIC
# MAGIC **Input:** Catalog, schema, and Volume passed from the deployed bundle target.
# MAGIC
# MAGIC **Output:** Plain, fully qualified target names under one authorized schema.
# MAGIC
# MAGIC **Key concepts:** Unity Catalog three-level names, managed tables, Volume isolation.
# MAGIC
# MAGIC **Expected result:** The Azure target resolves under `dbr_dev.parvinbadalov_urbanflow`.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every Phase 2 object is confined to the same personal UrbanFlow schema as Bronze."
# MAGIC
# MAGIC **Rerun and cost considerations:** Name construction is free; identifier validation prevents arbitrary SQL or path injection.

# COMMAND ----------

for label, value in (
    ("catalog", target_catalog),
    ("schema", target_schema),
    ("volume", target_volume),
):
    if not value.isidentifier():
        raise ValueError(f"{label} must be a plain identifier, received {value!r}.")
table_root = f"{target_catalog}.{target_schema}"
bronze_table = f"{table_root}.bronze_station_status"
silver_table = f"{table_root}.silver_station_status"
quarantine_table = f"{table_root}.quarantine_station_status"
duplicate_table = f"{table_root}.duplicate_station_status"
report_path = (
    f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/reports/silver_gold/"
    f"{source_execution_id}.silver.json"
)
print({"bronze": bronze_table, "silver": silver_table, "report": report_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read exactly the verified Bronze execution
# MAGIC
# MAGIC **What:** Filter the existing Bronze table by the previously reconciled execution ID.
# MAGIC
# MAGIC **Why:** Phase 2 must reuse the completed snapshot and must not mix future executions into this evidence.
# MAGIC
# MAGIC **Input:** Managed Bronze Delta table and `urbanflow-20260929T195132Z-r3` by default.
# MAGIC
# MAGIC **Output:** A bounded 2,520-row DataFrame when the live table still matches its committed evidence.
# MAGIC
# MAGIC **Key concepts:** Predicate pushdown, execution correlation, immutable Bronze evidence.
# MAGIC
# MAGIC **Expected result:** Exactly 2,520 input rows for the completed milestone.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We transform the proven Bronze slice instead of publishing or consuming another snapshot."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is a bounded Delta read; it does not contact Event Hubs or start a stream.

# COMMAND ----------

bronze = spark.table(bronze_table).where(F.col("execution_id") == source_execution_id)
bronze_rows = bronze.count()
if bronze_rows != 2520:
    raise RuntimeError(f"Expected the verified 2,520-row Bronze slice, found {bronze_rows}.")
print({"source_execution_id": source_execution_id, "bronze_rows": bronze_rows})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Apply the Silver contract and reconcile every outcome
# MAGIC
# MAGIC **What:** Annotate contract failures, route Quarantine, choose the first deterministic arrival, and separate duplicate deliveries.
# MAGIC
# MAGIC **Why:** Every Bronze row must have one accountable outcome before anything is persisted.
# MAGIC
# MAGIC **Input:** The execution-scoped Bronze DataFrame and shortage thresholds of two bikes or docks.
# MAGIC
# MAGIC **Output:** Silver, Quarantine, duplicate DataFrames plus reconciliation and freshness reports.
# MAGIC
# MAGIC **Key concepts:** Data contracts, deterministic deduplication, quarantine, freshness, reconciliation.
# MAGIC
# MAGIC **Expected result:** `Bronze = Silver + Quarantine + Duplicates` and unique Silver event IDs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "A row is accepted, quarantined, or counted as a duplicate; none disappear."
# MAGIC
# MAGIC **Rerun and cost considerations:** Transformations are bounded to one snapshot; the later MERGE makes approved reruns idempotent.

# COMMAND ----------

split = split_silver_and_quarantine(bronze, low_bike_threshold=2, low_dock_threshold=2)
silver_report = reconcile_silver(bronze, split)
freshness_report = freshness(split.silver, now_epoch_seconds=int(time.time()))
print({"reconciliation": silver_report, "freshness": freshness_report})
if silver_report["status"] != "PASS":
    raise RuntimeError("Bronze-to-Silver reconciliation failed before persistence.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Persist the three outcomes with idempotent MERGE
# MAGIC
# MAGIC **What:** MERGE Silver by event ID and MERGE Quarantine and duplicates by Kafka lineage.
# MAGIC
# MAGIC **Why:** Replaying the approved batch must update matching records rather than append copies.
# MAGIC
# MAGIC **Input:** Reconciled DataFrames and the three managed Delta table names.
# MAGIC
# MAGIC **Output:** Durable Silver, Quarantine, and duplicate tables with before/after counts.
# MAGIC
# MAGIC **Key concepts:** Delta MERGE, stable keys, idempotency, managed tables.
# MAGIC
# MAGIC **Expected result:** A second identical run reports zero inserted rows for all three outputs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Business events use event IDs; rejected and duplicate broker records use topic, partition, and offset."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is the first Azure write in Phase 2 and requires consolidated approval; no table is dropped or replaced.

# COMMAND ----------

write_report = persist_silver_outputs(
    spark,
    split,
    silver_table=silver_table,
    quarantine_table=quarantine_table,
    duplicate_table=duplicate_table,
)
print({"delta_merges": write_report})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Store secret-free Silver evidence
# MAGIC
# MAGIC **What:** Write one JSON report containing counts, freshness, and Delta MERGE results.
# MAGIC
# MAGIC **Why:** A green task alone does not prove completeness or rerun safety.
# MAGIC
# MAGIC **Input:** Reconciliation dictionaries and the execution-specific Volume path.
# MAGIC
# MAGIC **Output:** `<execution_id>.silver.json` without row payloads or credentials.
# MAGIC
# MAGIC **Key concepts:** Durable evidence, quality gates, minimal disclosure.
# MAGIC
# MAGIC **Expected result:** The JSON status is `PASS` and all table counts are visible.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The report records proof, while sensitive data and raw messages stay out of logs."
# MAGIC
# MAGIC **Rerun and cost considerations:** One small Volume file is overwritten for the same execution ID, making evidence deterministic.

# COMMAND ----------

evidence = {
    "phase": "bronze_to_silver",
    "source_execution_id": source_execution_id,
    "status": silver_report["status"],
    "reconciliation": silver_report,
    "freshness": freshness_report,
    "delta_merges": write_report,
}
write_json_report(evidence, report_path)
print({"status": evidence["status"], "report_path": report_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Silver has a physical Delta workflow: a bounded Bronze slice is validated, deterministically deduplicated, routed into three accountable outcomes, reconciled, and merged by stable keys.
# MAGIC
# MAGIC **Actual validation:** This Phase 2 notebook has not been run in Azure; its functions and contracts were tested locally against real Spark.
# MAGIC
# MAGIC **Common errors:** Wrong execution ID, a non-approved cluster, missing schema privileges, an altered Bronze row count, duplicate MERGE keys, or a target identifier from the wrong bundle environment.
# MAGIC
# MAGIC **Troubleshooting:** Stop before writes when the 2,520-row assertion or reconciliation fails; inspect the committed Bronze evidence and never republish Event Hubs data as a repair.
# MAGIC
# MAGIC **Review questions:** Why are Kafka coordinates the Quarantine key? Why is broker timestamp considered before partition offset? Why does a MERGE still reject duplicate source keys?
# MAGIC
# MAGIC **Presentation summary:** "UrbanFlow turns the already verified Bronze snapshot into durable, reconciled Silver, Quarantine, and duplicate tables with an idempotent MERGE and a secret-free report."

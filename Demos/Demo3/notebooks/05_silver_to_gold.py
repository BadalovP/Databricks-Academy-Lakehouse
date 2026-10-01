# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Silver to Gold Operational Tables
# MAGIC
# MAGIC **Business context:** The reconciled Silver snapshot must produce stable operational facts, shortage indicators, and transparent rebalancing priorities without inventing historical trends.
# MAGIC
# MAGIC **Prerequisites:** The approved Bronze-to-Silver task succeeded in the same Job run, the managed Volume exists, and the saved 40-row station-information sample is available.
# MAGIC
# MAGIC **Learning objectives:** Define fact and dimension grains, preserve unmatched stations, build daily aggregates, rank shortages, persist idempotently, and reconcile Gold to Silver.
# MAGIC
# MAGIC **Academy labs:** Labs 4, 5, 6, 7, 8, and 9.
# MAGIC
# MAGIC **Safety:** `run_transform` defaults to `false`; this notebook never starts compute, calls Event Hubs, or executes Lakeflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the tested Gold and reporting helpers
# MAGIC
# MAGIC **What:** Load JSON, Spark types, Gold transformations, Delta persistence, and the shared cluster allowlist.
# MAGIC
# MAGIC **Why:** Keeping table grains and scoring rules in tested modules makes the notebook concise and reviewable.
# MAGIC
# MAGIC **Input:** Bundle-synchronized project files.
# MAGIC
# MAGIC **Output:** Imports only, without a Spark action.
# MAGIC
# MAGIC **Key concepts:** Reusable transformations, orchestration, safety constants.
# MAGIC
# MAGIC **Expected result:** Imports succeed without reading Silver.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The presentation notebook calls the same tested functions that the Job uses."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports are free and idempotent.

# COMMAND ----------

import json
import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyspark.sql import functions as F
from pyspark.sql import types as T

from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS
from urbanflow.gold import (
    daily_station_summary,
    persist_gold_outputs,
    rebalancing_priority,
    reconcile_gold,
    shortage_indicators,
    station_availability_fact,
    station_dimension,
)
from urbanflow.reporting import (
    evidence_report_path,
    resolve_attempt_id,
    write_json_report,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Apply the dry-run gate and validate the target
# MAGIC
# MAGIC **What:** Read the same Job parameters as the Silver task and verify the current cluster is approved.
# MAGIC
# MAGIC **Why:** The second task must inherit the same source execution and target, and must exit before a table action by default.
# MAGIC
# MAGIC **Input:** `run_transform`, source execution ID, catalog, schema, and Volume.
# MAGIC
# MAGIC **Output:** Validated parameters or an immediate `DRY_RUN` exit.
# MAGIC
# MAGIC **Key concepts:** Multi-task Job parameters, fail-closed execution, existing compute.
# MAGIC
# MAGIC **Expected result:** Only an explicitly approved run continues.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Silver and Gold share one visible approval gate and one target."
# MAGIC
# MAGIC **Rerun and cost considerations:** The dry-run path performs no Spark read or write and never changes cluster state.

# COMMAND ----------

dbutils.widgets.dropdown("run_transform", "false", ["false", "true"])
dbutils.widgets.text("source_execution_id", "urbanflow-20260929T195132Z-r3")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
dbutils.widgets.text("run_attempt_id", "")
run_transform = dbutils.widgets.get("run_transform").lower() == "true"
source_execution_id = dbutils.widgets.get("source_execution_id").strip()
target_catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
target_volume = dbutils.widgets.get("volume").strip()
if not run_transform:
    dbutils.notebook.exit("DRY_RUN: no Silver read and no Gold write occurred.")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
if cluster_id not in APPROVED_RUN_CLUSTER_IDS:
    raise RuntimeError(f"Cluster {cluster_id!r} is not approved for UrbanFlow.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Build the isolated table map
# MAGIC
# MAGIC **What:** Validate target identifiers and name the Silver source, five Gold outputs, and execution report.
# MAGIC
# MAGIC **Why:** Explicit names make the physical data model and target boundary easy to review.
# MAGIC
# MAGIC **Input:** Bundle target variables.
# MAGIC
# MAGIC **Output:** Fully qualified managed-table names under the personal UrbanFlow schema.
# MAGIC
# MAGIC **Key concepts:** Fact grain, dimension grain, aggregates, managed Delta tables.
# MAGIC
# MAGIC **Expected result:** No root or unrelated lab table appears in the map.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Each Gold product has its own declared grain and stable MERGE key."
# MAGIC
# MAGIC **Rerun and cost considerations:** Constructing and validating names has no cloud side effect.

# COMMAND ----------

for label, value in (
    ("catalog", target_catalog),
    ("schema", target_schema),
    ("volume", target_volume),
):
    if not value.isidentifier():
        raise ValueError(f"{label} must be a plain identifier, received {value!r}.")
table_root = f"{target_catalog}.{target_schema}"
silver_table = f"{table_root}.silver_station_status"
gold_tables = {
    "dimension": f"{table_root}.dim_station_development_sample",
    "fact": f"{table_root}.fact_station_availability",
    "daily_summary": f"{table_root}.gold_daily_station_summary",
    "shortages": f"{table_root}.gold_station_shortage",
    "priorities": f"{table_root}.gold_rebalancing_priority",
}
attempt_id = resolve_attempt_id(job_run_id=dbutils.widgets.get("run_attempt_id"))
report_path = evidence_report_path(
    f"/Volumes/{target_catalog}/{target_schema}/{target_volume}",
    phase="silver_gold",
    execution_id=source_execution_id,
    attempt_id=attempt_id,
    suffix="gold",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read the saved station-information development sample
# MAGIC
# MAGIC **What:** Load the attributed 40-row GBFS sample and create an explicitly typed station-reference DataFrame.
# MAGIC
# MAGIC **Why:** Historical trip keys and current station UUIDs need a bridge, but this small sample must never be described as the complete station dimension.
# MAGIC
# MAGIC **Input:** `station_information.sample.json` committed under `data/samples`.
# MAGIC
# MAGIC **Output:** A 40-row development dimension carrying UUID and `short_name`.
# MAGIC
# MAGIC **Key concepts:** Dimension keys, UUID versus short name, explicit schema, sample limitations.
# MAGIC
# MAGIC **Expected result:** Exactly 40 reference rows; most of the 2,520 availability facts remain unenriched.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The sample proves the join design while the left join preserves every real observation."
# MAGIC
# MAGIC **Rerun and cost considerations:** This reads a small synchronized file and creates no network traffic.

# COMMAND ----------

sample_path = PROJECT_ROOT / "data" / "samples" / "station_information.sample.json"
payload = json.loads(sample_path.read_text(encoding="utf-8"))
information_rows = [
    {**row, "region_id": str(row.get("region_id") or "UNKNOWN")}
    for row in payload["data"]["stations"]
]
information_schema = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("short_name", T.StringType(), True),
        T.StructField("name", T.StringType(), False),
        T.StructField("lat", T.DoubleType(), False),
        T.StructField("lon", T.DoubleType(), False),
        T.StructField("capacity", T.IntegerType(), True),
        T.StructField("region_id", T.StringType(), True),
    ]
)
station_information = spark.createDataFrame(information_rows, information_schema)
dimension = station_dimension(station_information)
if dimension.count() != 40:
    raise RuntimeError("Expected the committed 40-row station-information sample.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Build stable Gold facts and operational outputs
# MAGIC
# MAGIC **What:** Verify the source execution exists in Silver, then build facts and aggregates from all persisted Silver history.
# MAGIC
# MAGIC **Why:** Gold should answer operational questions while retaining the one-observation grain and acknowledging incomplete reference enrichment.
# MAGIC
# MAGIC **Input:** Persisted Silver and the 40-row development dimension.
# MAGIC
# MAGIC **Output:** Five DataFrames with stable schemas and declared grains.
# MAGIC
# MAGIC **Key concepts:** Left enrichment, fact tables, aggregates, shortage indicators, explainable scoring.
# MAGIC
# MAGIC **Expected result:** Fact rows equal Silver rows; daily rows report one observation and `is_trend_capable=false`.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The values are real snapshot analytics, while the trend flag prevents one reading from becoming a fake trend."
# MAGIC
# MAGIC **Rerun and cost considerations:** All calculations are bounded to the completed execution and use no streaming source.

# COMMAND ----------

silver = spark.table(silver_table)
source_silver_rows = silver.where(F.col("execution_id") == source_execution_id).count()
if source_silver_rows != 2520:
    raise RuntimeError(f"Expected 2,520 source-execution Silver rows, found {source_silver_rows}.")
fact = station_availability_fact(silver, dimension)
daily = daily_station_summary(fact)
shortages = shortage_indicators(fact)
priorities = rebalancing_priority(fact)
matched_reference_rows = fact.where(F.col("station_name").isNotNull()).count()
print(
    {
        "silver_rows": silver.count(),
        "source_execution_silver_rows": source_silver_rows,
        "reference_matches": matched_reference_rows,
        "reference_limit": "40-row development sample",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Reconcile Gold back to Silver before writing
# MAGIC
# MAGIC **What:** Compare Silver and fact counts, event-ID uniqueness, and daily observation totals.
# MAGIC
# MAGIC **Why:** A fact table or aggregate can look plausible after a lossy join, so explicit reconciliation is mandatory.
# MAGIC
# MAGIC **Input:** Silver, availability fact, and daily summary DataFrames.
# MAGIC
# MAGIC **Output:** A `PASS` or `FAIL` report that gates all Gold writes.
# MAGIC
# MAGIC **Key concepts:** Grain preservation, aggregate reconciliation, quality gates.
# MAGIC
# MAGIC **Expected result:** Fact count and daily observation total both equal the Silver count.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Gold cannot pass by appearance; its lowest and aggregated grains both add back to Silver."
# MAGIC
# MAGIC **Rerun and cost considerations:** Count actions scan only the bounded Phase 2 data and stop the task before writes on failure.

# COMMAND ----------

gold_report = reconcile_gold(silver, fact, daily)
print({"gold_reconciliation": gold_report})
if gold_report["status"] != "PASS":
    raise RuntimeError("Silver-to-Gold reconciliation failed before persistence.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Persist all Gold outputs idempotently
# MAGIC
# MAGIC **What:** MERGE dimension, fact, daily summary, shortages, and priorities by their stable business grains.
# MAGIC
# MAGIC **Why:** An approved rerun should update the same records and never double counts.
# MAGIC
# MAGIC **Input:** Reconciled Gold DataFrames and the explicit table map.
# MAGIC
# MAGIC **Output:** Five managed Delta tables and per-table before/after counts.
# MAGIC
# MAGIC **Key concepts:** Idempotent Delta MERGE, composite keys, persisted Gold model.
# MAGIC
# MAGIC **Expected result:** Repeating the same source execution inserts zero additional rows.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every output has a key that matches its grain, from event ID to station and observation date."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are bounded writes requiring approval; the notebook never uses serverless or Lakeflow compute.

# COMMAND ----------

merge_report = persist_gold_outputs(
    spark,
    dimension=dimension,
    fact=fact,
    daily_summary=daily,
    shortages=shortages,
    priorities=priorities,
    table_names=gold_tables,
    execution_id=source_execution_id,
)
print({"delta_merges": merge_report})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Verify the corrected derived tables across their whole contents
# MAGIC
# MAGIC **What:** Assert that each derived table now contains exactly the rows its corrected source produces, that no row is left without an `execution_id`, and that any other execution's rows still have the same count as before the write.
# MAGIC
# MAGIC **Why:** The scoped comparison alone can report success while wrong rows remain. `gold_rebalancing_priority` is the proof: it was created before `execution_id` existed, so adding the column left all 746 legacy rows NULL, and `NULL = '<execution>'` is NULL rather than true. A scoped delete can therefore never match those rows, while the scoped count still compares 657 against 657 and looks correct. The 89 stations that are actually out of service would have stayed on an operator's action list.
# MAGIC
# MAGIC **Input:** The per-table reports returned by `persist_gold_outputs`, each already carrying a whole-table `verification` block and the legacy attribution report.
# MAGIC
# MAGIC **Output:** A raised error on any mismatch, otherwise a printed summary of attributed and deleted rows.
# MAGIC
# MAGIC **Key concepts:** NULL comparison semantics, execution-scoped deletion, verified-lineage backfill, two-way membership checks, failing closed.
# MAGIC
# MAGIC **Expected result:** `legacy_backfill` reports 746 rows attributed on the first corrected run and 0 on any later run; shortages and priorities each report 657 rows with 89 stale rows removed, 0 unassigned rows and status `PASS`.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We checked the whole table, not just the part we wrote. A row that belonged to no run would have slipped past a narrower check, and that is exactly the row that would have kept a broken station on the repair list."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are small counting queries over tables already in memory for this run, and they add no write, no new table and no extra compute.

# COMMAND ----------

backfill_report = merge_report["priorities"].get("legacy_backfill", {})
print({"legacy_backfill": backfill_report})

verification_failures = {}
for derived in ("shortages", "priorities"):
    verification = merge_report[derived].get("verification", {})
    if verification.get("status") != "PASS" or not merge_report[derived].get(
        "other_executions_preserved", True
    ):
        verification_failures[derived] = verification
    print(
        {
            "table": gold_tables[derived],
            "scope_rows": verification.get("scope_rows"),
            "stale_rows_removed": merge_report[derived].get("stale_rows_removed"),
            "unassigned_rows": verification.get("unassigned_rows"),
            "other_execution_rows": verification.get("other_execution_rows"),
            "status": verification.get("status"),
        }
    )

if verification_failures:
    raise RuntimeError(f"Derived Gold tables did not verify: {verification_failures}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Store Gold evidence and limitations
# MAGIC
# MAGIC **What:** Write reconciliation, MERGE counts, reference match coverage, and the one-snapshot limitation to JSON.
# MAGIC
# MAGIC **Why:** The evidence must distinguish real availability rows from the partial development dimension and from genuine historical trip trends.
# MAGIC
# MAGIC **Input:** Gold reports and execution-specific Volume path.
# MAGIC
# MAGIC **Output:** `<execution_id>.gold.json` with no raw station events.
# MAGIC
# MAGIC **Key concepts:** Evidence, limitations, data provenance, secret minimization.
# MAGIC
# MAGIC **Expected result:** Status `PASS`, 2,520 fact observations, and an explicit 40-row reference-sample label.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The report records what is complete, what is sampled, and why one snapshot cannot prove a trend."
# MAGIC
# MAGIC **Rerun and cost considerations:** The small report is overwritten deterministically for the same execution ID.

# COMMAND ----------

evidence = {
    "phase": "silver_to_gold",
    "source_execution_id": source_execution_id,
    "status": gold_report["status"],
    "reconciliation": gold_report,
    "delta_merges": merge_report,
    "legacy_priority_backfill": backfill_report,
    "derived_table_verification": {
        derived: merge_report[derived].get("verification", {})
        for derived in ("shortages", "priorities")
    },
    "station_reference_rows": 40,
    "matched_reference_rows": matched_reference_rows,
    "availability_history": "one real snapshot; not trend evidence",
}
write_json_report(evidence, report_path)
print({"status": evidence["status"], "report_path": report_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Gold now has durable designs for a station development dimension, an availability fact, daily station summaries, shortage indicators, and explainable rebalancing priorities. Left enrichment keeps all real observations even when the 40-row sample cannot name every station.
# MAGIC
# MAGIC **Actual validation:** This Phase 2 notebook has not been run in Azure; its transformations and reconciliation were tested locally with real Spark.
# MAGIC
# MAGIC **Common errors:** Treating `short_name` as the UUID, dropping unmatched facts with an inner join, describing one snapshot as a trend, or using an aggregate key that does not match its grain.
# MAGIC
# MAGIC **Troubleshooting:** Compare Silver and fact counts first, then inspect reference match coverage; do not fabricate station history or download a large archive to make charts look fuller.
# MAGIC
# MAGIC **Review questions:** Why is the station join left-sided? What makes a daily row trend-capable? Why does the development dimension have an explicit sample name?
# MAGIC
# MAGIC **Presentation summary:** "UrbanFlow preserves every Silver observation, adds available station context, produces honest one-snapshot operational metrics, and reconciles every Gold level before idempotent persistence."

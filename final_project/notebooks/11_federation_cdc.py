# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Station maintenance CDC with Delta Change Data Feed
# MAGIC
# MAGIC **Business context:** Rebalancing crews must not be sent to stations that are closed for maintenance. Maintenance tickets live in an operational system; this notebook keeps a governed Silver copy current by processing only what changed since the last run.
# MAGIC
# MAGIC **Prerequisites:** The target schema exists; the task runs on serverless compute. For `cdc_source=federation`, an approved read-only foreign catalog exposes the maintenance table. For `cdc_source=fixture_snapshot`, the committed fictional snapshots are used.
# MAGIC
# MAGIC **Learning objectives:** Turn successive snapshots into Change Data Feed events, process an exact version range incrementally with MERGE, keep an audit history, reconcile, and advance a checkpoint only after the checks pass.
# MAGIC
# MAGIC **Academy labs:** Lab 10 (Lakehouse Federation, Delta CDF, incremental MERGE, checkpointing), Lab 4 (MERGE, SCD Type 1), Lab 7 (reconciliation).
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-12 (Final Project run 896549012431425, task `federation_cdc`) in fixture mode for `batch_001`: 5 inserts, checkpoint -1 to 1, reconciliation zero mismatches in both directions. `batch_002` ran live on 2026-10-12 (run 343513312512955): 1 insert, 2 updates, 1 delete (6 CDF events), checkpoint 2 to 3, reconciliation zero. Five seconds later Databricks committed an automatic `OPTIMIZE` (source version 4, no data change); step 4 therefore decides from Change Data Feed rows, not from the latest version number. The Federation source has not been run live yet.
# MAGIC
# MAGIC CDC here is **Delta Change Data Feed on a Final-Project-owned table**, not database-native CDC: no logical replication or WAL decoding of the source system is involved. The external system is only ever read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read parameters and refuse unsafe runs
# MAGIC
# MAGIC **What:** Read the run switch, source mode and target, confirm the workspace and serverless compute, and exit when CDC is not requested.
# MAGIC
# MAGIC **Why:** The task is part of the platform Job; by default it must do nothing.
# MAGIC
# MAGIC **Input:** Job parameters.
# MAGIC
# MAGIC **Output:** Validated settings and the four CDC table names.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, identifier validation, serverless-only compute.
# MAGIC
# MAGIC **Expected result:** `SKIPPED` when `run_federation_cdc=false`; otherwise the run continues.
# MAGIC
# MAGIC **How to explain it:** "CDC runs only when asked, only in its own schema, and only on serverless compute."
# MAGIC
# MAGIC **Rerun and cost considerations:** Widget reads are free.

# COMMAND ----------

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow import cdc_sql
from urbanflow.automation import assert_serverless_compute
from urbanflow.rag.runtime import assert_workspace, flag

names = ("expected_host", "catalog", "schema", "run_federation_cdc", "cdc_source",
         "cdc_snapshot", "federation_catalog", "federation_table")  # fmt: skip
for name in names:
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name).strip() for name in names}
if not flag(values["run_federation_cdc"] or "false"):
    dbutils.notebook.exit(json.dumps({"status": "SKIPPED", "reason": "run_federation_cdc=false"}))
assert_workspace(values["expected_host"])
assert_serverless_compute(spark.conf.get)
if values["cdc_source"] not in ("fixture_snapshot", "federation"):
    raise ValueError("cdc_source must be fixture_snapshot or federation.")
tables = cdc_sql.CdcTables(values["catalog"], values["schema"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read one full snapshot of the maintenance system
# MAGIC
# MAGIC **What:** Read the snapshot either with a SELECT-only Lakehouse Federation query or from one committed fictional snapshot file, and check its contract.
# MAGIC
# MAGIC **Why:** Both sources deliver the same five columns, so everything after this step is identical.
# MAGIC
# MAGIC **Input:** The foreign table, or `fixtures/maintenance/station_maintenance_<cdc_snapshot>.csv`.
# MAGIC
# MAGIC **Output:** The temporary view `urbanflow_final_maintenance_snapshot`.
# MAGIC
# MAGIC **Key concepts:** Read-only federation, data contract, duplicate-key refusal.
# MAGIC
# MAGIC **Expected result:** A snapshot with unique `maintenance_id` values and no null keys.
# MAGIC
# MAGIC **How to explain it:** "We only read the external system; every write goes to tables this project owns."
# MAGIC
# MAGIC **Rerun and cost considerations:** One small read.

# COMMAND ----------

from urbanflow.cdc import load_snapshot_csv
from urbanflow.federation import FederationSource, select_statement, validate_maintenance_columns

if values["cdc_source"] == "federation":
    source = FederationSource(values["federation_catalog"], values["federation_table"])
    snapshot = spark.sql(select_statement(source))
    validate_maintenance_columns(set(snapshot.columns))
else:
    if not re.fullmatch(r"batch_\d{3}", values["cdc_snapshot"]):
        raise ValueError("cdc_snapshot must look like batch_001.")
    path = (
        PROJECT_ROOT
        / "fixtures"
        / "maintenance"
        / f"station_maintenance_{values['cdc_snapshot']}.csv"
    )
    rows = [{"maintenance_id": key, **row} for key, row in load_snapshot_csv(path).items()]
    snapshot = spark.createDataFrame(rows)
snapshot.selectExpr(*cdc_sql.SNAPSHOT_CASTS).createOrReplaceTempView(cdc_sql.SNAPSHOT_VIEW)
contract = spark.sql(cdc_sql.snapshot_contract()).first().asDict()
if contract["duplicate_keys"] or contract["null_keys"]:
    raise ValueError(f"Snapshot contract failed: {contract}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Merge the snapshot into the CDF-enabled source table
# MAGIC
# MAGIC **What:** Create the four tables if needed, compare the snapshot with the current source in both directions, and MERGE it into `station_maintenance_source` (Change Data Feed enabled) only when they differ.
# MAGIC
# MAGIC **Why:** The MERGE turns "a new full snapshot" into precise insert, update and delete events; unchanged rows are not rewritten, so they produce no events.
# MAGIC
# MAGIC **Input:** The snapshot view.
# MAGIC
# MAGIC **Output:** A new Delta version of the source table when the snapshot changed; no new version when it is identical (a Delta MERGE that changes nothing would still record an empty commit).
# MAGIC
# MAGIC **Key concepts:** Change Data Feed, `WHEN NOT MATCHED BY SOURCE THEN DELETE`, idempotent DDL.
# MAGIC
# MAGIC **Expected result:** The source equals the snapshot; `source_merged` says whether a MERGE ran; the latest version is known.
# MAGIC
# MAGIC **How to explain it:** "Delta records exactly what changed between two snapshots, row by row."
# MAGIC
# MAGIC **Rerun and cost considerations:** An identical snapshot is detected by a two-way comparison and skipped, so a replay writes nothing to the source and the checkpoint does not move. Pending versions from an earlier interrupted run are still processed in step 4.

# COMMAND ----------

for statement in cdc_sql.create_tables(tables):
    spark.sql(statement)
differences = spark.sql(cdc_sql.snapshot_differences(tables)).first().asDict()
source_merged = cdc_sql.snapshot_requires_merge(differences)
if source_merged:  # an identical snapshot creates no source commit at all
    spark.sql(cdc_sql.merge_snapshot(tables))
latest_version = int(spark.sql(f"DESCRIBE HISTORY {tables.source} LIMIT 1").first()["version"])
checkpoint_row = spark.sql(cdc_sql.checkpoint_version(tables)).first()["processed_version"]
checkpoint = int(checkpoint_row) if checkpoint_row is not None else -1
print({"checkpoint": checkpoint, "latest_source_version": latest_version,
       "source_merged": source_merged, **differences})  # fmt: skip

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Process exactly the new version range
# MAGIC
# MAGIC **What:** Read `table_changes` for the versions after the checkpoint and, when they contain change events, MERGE every event into the audit history and the newest final state per key into the Silver target.
# MAGIC
# MAGIC **Why:** Reading only new versions keeps the work proportional to the change, not to the table size. The decision comes from the Change Data Feed rows, not from the version number: Databricks may add commits that change no data (an automatic `OPTIMIZE` after a MERGE, or predictive optimization), and those have no CDF rows.
# MAGIC
# MAGIC **Input:** Source versions `checkpoint + 1` to `latest_version`.
# MAGIC
# MAGIC **Output:** Updated `maintenance_cdc_history` and `silver_station_maintenance`.
# MAGIC
# MAGIC **Key concepts:** Incremental processing, SCD Type 1, idempotent MERGE keyed by a deterministic event ID.
# MAGIC
# MAGIC **Expected result:** First run: 5 inserts. After `batch_002`: 2 updates, 1 delete, 1 insert. After `batch_003`: nothing to process, even if an `OPTIMIZE` added a version.
# MAGIC
# MAGIC **How to explain it:** "Both writes are MERGEs, so replaying the same range after a failure changes nothing. A commit without data changes is not work."
# MAGIC
# MAGIC **Rerun and cost considerations:** With no new versions this step does nothing; with only non-data versions it runs one small CDF read. Pending versions from a run that merged but failed before its checkpoint are still processed, because they have CDF rows. The checkpoint stays behind non-data commits, so if the source changes nothing for longer than the Delta log retention (30 days by default), the next read fails loudly rather than skipping changes.

# COMMAND ----------

event_counts = {}
if latest_version > checkpoint:  # scanned even when the snapshot matched (recovery)
    spark.sql(
        cdc_sql.changes_view(tables, start_version=checkpoint + 1, end_version=latest_version)
    )
    events = spark.sql(cdc_sql.change_counts()).collect()
    event_counts = {row["_change_type"]: row["n"] for row in events}
# A range of commits without data changes (e.g. an automatic OPTIMIZE) has no CDF rows: nothing
# to apply, and the checkpoint does not move.
new_versions = cdc_sql.range_has_changes(event_counts)
if new_versions:
    spark.sql(cdc_sql.merge_history(tables))
    spark.sql(cdc_sql.latest_changes_view())
    spark.sql(cdc_sql.merge_target(tables))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Reconcile, then advance the checkpoint
# MAGIC
# MAGIC **What:** Compare the target with the source at the processed version - values included, in both directions - check key uniqueness and audit completeness, and only then move the checkpoint forward.
# MAGIC
# MAGIC **Why:** If the checkpoint moved before the checks, a failed run would silently skip its changes forever.
# MAGIC
# MAGIC **Input:** Source at `VERSION AS OF latest_version`, the target, the history and this run's changes.
# MAGIC
# MAGIC **Output:** A reconciliation result and, on success, the new checkpoint.
# MAGIC
# MAGIC **Key concepts:** Two-way reconciliation, time travel, forward-only checkpoint.
# MAGIC
# MAGIC **Expected result:** Every difference count is 0. After change events the checkpoint equals the latest scanned source version; without change events it is unchanged (data at the latest version equals data at the checkpoint, so reconciling at the latest version is equivalent).
# MAGIC
# MAGIC **How to explain it:** "The checkpoint is the last thing we write, and only after everything matches."
# MAGIC
# MAGIC **Rerun and cost considerations:** A few small aggregate queries.

# COMMAND ----------

result = (
    spark.sql(
        cdc_sql.reconciliation(tables, processed_version=latest_version, check_history=new_versions)
    )
    .first()
    .asDict()
)
problems = {name: value for name, value in result.items() if name.endswith(("target", "keys",
            "history")) and value}  # fmt: skip
if result["source_count"] != result["target_count"] or problems:
    raise AssertionError(f"CDC reconciliation failed; checkpoint not advanced: {result}")
if new_versions:
    spark.sql(cdc_sql.advance_checkpoint(tables, processed_version=latest_version))
report = {"status": "PASS", "cdc_source": values["cdc_source"], "snapshot": values["cdc_snapshot"],
          "source_merged": source_merged, "snapshot_differences": differences,
          "source_latest_version": latest_version, "checkpoint_before": checkpoint,
          "checkpoint_after": latest_version if new_versions else checkpoint,
          "events": event_counts, **result}  # fmt: skip
print(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Return the CDC report
# MAGIC
# MAGIC **What:** Return the report to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The report from step 5.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `PASS`.
# MAGIC
# MAGIC **How to explain it:** "Each run records the version range it processed and the reconciliation numbers."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Successive snapshots become exact change events through a MERGE into a Change Data Feed table; processing a fixed version range with MERGE, reconciling at that version and advancing the checkpoint last makes the pipeline incremental, auditable and safe to rerun.
# MAGIC
# MAGIC **Common errors:** Advancing the checkpoint before reconciling; treating a commit without data changes (`OPTIMIZE`) as new work; reading `table_changes` without an upper bound; updating unchanged rows, which floods CDF with empty events; writing to the external system.
# MAGIC
# MAGIC **Troubleshooting:** If reconciliation fails, the checkpoint did not move - fix the cause and rerun; the same range is processed again and the MERGEs absorb what was already written.
# MAGIC
# MAGIC **Review questions:** Why are update pre-images kept in the history but ignored for the target? Why is the checkpoint compared with `<` before updating?
# MAGIC
# MAGIC **Presentation summary:** Change Data Feed plus a forward-only checkpoint gives incremental, replay-safe maintenance data for the rebalancing decisions.

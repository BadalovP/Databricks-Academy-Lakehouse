# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - One Bounded Event Hubs Snapshot to Bronze
# MAGIC
# MAGIC **Business context:** A single real Citi Bike station-status snapshot proves the path from a public REST API through the existing Event Hub into a traceable Bronze Delta table.
# MAGIC
# MAGIC **Architecture:** `Citi Bike GBFS REST feed` -> `urbanflow producer` -> `parvinbadalov_evh` Event Hub -> `Spark Structured Streaming (Kafka protocol)` -> `bronze_station_status` Delta table -> `Silver / Quarantine / shortage preview`.
# MAGIC
# MAGIC **Learning objectives:** Distinguish REST collection from Kafka transport, understand Event Hubs partitions and offsets, parse with an explicit schema, run bounded Structured Streaming micro-batches with a bounded wait, use an isolated checkpoint, keep deployment targets isolated, reconcile exact counts, and prepare idempotent downstream processing.
# MAGIC
# MAGIC **Academy labs:** Lab 3 streaming, Lab 4 data quality, Lab 5 Lakeflow preparation, Lab 7 testing, and Lab 9 automation safety.
# MAGIC
# MAGIC **Prerequisites:** The operator has approved one live run; GP1 or GP2 is already `RUNNING`; the deployed target's UrbanFlow schema and Volume already exist; the notebook identity can read the named secret; and the producer report supplies the execution ID, source timestamp, and published count.
# MAGIC
# MAGIC **Inputs:** JSON station-status events from `urbanflow producer`, tagged with deterministic event IDs and one execution ID, plus job parameters for the run gate, the producer manifest values, the storage target, and the streaming wait bound.
# MAGIC
# MAGIC **Outputs:** Bronze Delta rows in the deployed target's schema, an isolated checkpoint under that target's Volume, and one secret-free JSON execution report.
# MAGIC
# MAGIC **Safety summary:** The default run is a dry run; a live run may attach only to an approved cluster ID; the streaming wait is bounded by a visible parameter; and this notebook never starts, resizes, restarts, or terminates shared academy compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load the tested project helpers
# MAGIC
# MAGIC **What:** Import configuration loading, Kafka parsing, the bounded-wait helper, reconciliation, and local medallion functions.
# MAGIC
# MAGIC **Why:** Keeping protocol, safety, and validation logic in tested modules makes the notebook short enough to review before a live run.
# MAGIC
# MAGIC **Input:** The `src/urbanflow` package synchronized by the bundle.
# MAGIC
# MAGIC **Output:** Python functions and the approved-cluster allowlist; no cloud data is read or written.
# MAGIC
# MAGIC **Key concepts:** Modular testing, import path setup, lazy Spark plans, safety constants shared with the automation module.
# MAGIC
# MAGIC **Expected result:** The imports succeed silently.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The notebook orchestrates small tested functions, so the risky live path stays visible and reviewable."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports do not contact Event Hubs, do not touch shared compute, and never start a query.

# COMMAND ----------

import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyspark.sql import functions as F

from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS
from urbanflow.config import load_config
from urbanflow.medallion import prepare_station_batch
from urbanflow.reporting import build_bronze_execution_report, write_json_report
from urbanflow.streaming import (
    REQUIRED_EVENT_FIELDS,
    await_bounded_completion,
    event_hubs_kafka_options,
    isolated_stream_paths,
    parse_station_events,
    read_event_hubs_stream,
    redacted_kafka_options,
    start_bronze_available_now,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read the committed configuration defaults
# MAGIC
# MAGIC **What:** Load `config/azure.yml`, which holds the non-secret Event Hubs names, streaming limits, and shortage thresholds.
# MAGIC
# MAGIC **Why:** Configuration as code keeps resource names reviewable in Git, while the storage target itself is resolved from job parameters in Step 4.
# MAGIC
# MAGIC **Input:** `config/azure.yml` synchronized by the bundle.
# MAGIC
# MAGIC **Output:** A validated `config` object whose Azure section is only the default target.
# MAGIC
# MAGIC **Key concepts:** Configuration as code, validation on load, non-secret resource identifiers, secret references by name.
# MAGIC
# MAGIC **Expected result:** Only non-secret project, namespace, hub, and consumer-group names are printed.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every resource name is declared in version control, and the file contains references to secrets rather than secret values."
# MAGIC
# MAGIC **Rerun and cost considerations:** A local YAML read is free and repeatable; it contacts no cloud service.

# COMMAND ----------

config = load_config(PROJECT_ROOT / "config" / "azure.yml")
print(
    {
        "project": config.project_name,
        "namespace": config.azure.event_hubs_namespace,
        "event_hub": config.azure.event_hub_name,
        "consumer_group": config.azure.consumer_group,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Declare every run parameter, including the bounded wait
# MAGIC
# MAGIC **What:** Declare widgets for the live gate, secret scope, producer manifest values, starting offsets, the storage target, and `stream_timeout_seconds`.
# MAGIC
# MAGIC **Why:** The consumer must identify only the events published by this one snapshot, must remain a dry run by default, and must never wait forever on shared billable compute.
# MAGIC
# MAGIC **Input:** Values copied from the secret-free producer JSON report plus `run_stream=true` only after approval.
# MAGIC
# MAGIC **Output:** Parsed parameters, including a positive streaming wait bound that is visible in the Databricks UI.
# MAGIC
# MAGIC **Key concepts:** Widgets as job parameters, bounded execution, producer-consumer correlation, fail-closed defaults, cost ceilings.
# MAGIC
# MAGIC **Expected result:** Defaults show `run_stream=False`, expected count zero, and a 900-second wait bound, so no query is eligible to start.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The producer and consumer share a run ID, and the operator can see exactly how long the job is allowed to wait before we give up."
# MAGIC
# MAGIC **Rerun and cost considerations:** Widgets have no cloud side effect. The wait bound is the notebook's own cost ceiling and stays below the job's `timeout_seconds: 1200`, so a stalled stream ends with evidence instead of an open-ended bill.

# COMMAND ----------

dbutils.widgets.dropdown("run_stream", "false", ["false", "true"])
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("execution_id", "NOT_APPROVED")
dbutils.widgets.text("expected_published_events", "0")
dbutils.widgets.text("expected_source_last_updated", "0")
dbutils.widgets.dropdown("starting_offsets", "earliest", ["earliest", "latest"])
dbutils.widgets.text("stream_timeout_seconds", "900")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")

run_stream = dbutils.widgets.get("run_stream").lower() == "true"
secret_scope = dbutils.widgets.get("secret_scope").strip()
execution_id = dbutils.widgets.get("execution_id").strip()
expected_published_events = int(dbutils.widgets.get("expected_published_events"))
expected_source_last_updated = int(dbutils.widgets.get("expected_source_last_updated"))
starting_offsets = dbutils.widgets.get("starting_offsets")
stream_timeout_seconds = float(dbutils.widgets.get("stream_timeout_seconds"))
# The job sets timeout_seconds: 1200, so our own wait must end before that ceiling.
if not 0 < stream_timeout_seconds <= 1200:
    raise ValueError("stream_timeout_seconds must be above 0 and at most the 1200s job timeout.")
print({"run_stream": run_stream, "stream_timeout_seconds": stream_timeout_seconds})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Resolve the storage target from the deployed bundle target
# MAGIC
# MAGIC **What:** Take catalog, schema, and volume from job parameters, fall back to `config/azure.yml`, validate them as plain identifiers, and build the Bronze table name, Volume root, and isolated checkpoint and report paths.
# MAGIC
# MAGIC **Why:** `databricks.yml` gives the `dev` target its own schema (`parvinbadalov_urbanflow_dev`). Until these values reached the notebook, a `dev` deployment still wrote the `azure` target's Bronze table and checkpoint, so the two targets were not isolated.
# MAGIC
# MAGIC **Input:** `catalog`, `schema`, and `volume` parameters supplied by `resources/jobs.yml` from the bundle variables; empty means "use the committed default".
# MAGIC
# MAGIC **Output:** A fully qualified Bronze table name, a Volume root, a dedicated checkpoint directory, and an execution-specific report path.
# MAGIC
# MAGIC **Key concepts:** Databricks Asset Bundle variables and targets, environment isolation, Unity Catalog three-level names, identifier validation, checkpoint isolation.
# MAGIC
# MAGIC **Expected result:** A `dev` deployment prints `dbr_dev.parvinbadalov_urbanflow_dev.bronze_station_status`; the committed default prints the `azure` schema.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The job passes the deployed target's own catalog, schema, and volume, so a development run can never write production evidence."
# MAGIC
# MAGIC **Rerun and cost considerations:** Building strings is free. Each execution ID gets its own report file, and the checkpoint stays inside the target's Volume, so a rerun cannot mix two targets' offsets.

# COMMAND ----------

# Empty means "use config/azure.yml"; the bundle job passes the deployed target's own values.
target_catalog = dbutils.widgets.get("catalog").strip() or config.azure.catalog
target_schema = dbutils.widgets.get("schema").strip() or config.azure.schema
target_volume = dbutils.widgets.get("volume").strip() or config.azure.volume
for label, value in (
    ("catalog", target_catalog),
    ("schema", target_schema),
    ("volume", target_volume),
):
    # These become a table name and a Volume path, so reject anything but a plain identifier.
    if not value.isidentifier():
        raise ValueError(f"{label} must be a plain Unity Catalog identifier, received {value!r}.")

target_table = f"{target_catalog}.{target_schema}.bronze_station_status"
volume_root = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}"
stream_paths = isolated_stream_paths(
    volume_root=volume_root,
    checkpoint_subpath=config.streaming.checkpoint_subpath,
    report_subpath=config.streaming.report_subpath,
    execution_id=execution_id,
)
print({"target_table": target_table, "checkpoint": stream_paths.checkpoint})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Verify approved compute, Unity Catalog storage, and secret access
# MAGIC
# MAGIC **What:** Refuse any cluster outside `APPROVED_RUN_CLUSTER_IDS`, confirm the target Volume is readable, and retrieve the connection string only inside an approved run.
# MAGIC
# MAGIC **Why:** GP1 and GP2 are shared academy resources, so a live UrbanFlow run must attach only where it is explicitly approved. `APPROVED_RUN_CLUSTER_IDS` is the attach allowlist; `PROTECTED_SHARED_CLUSTER_IDS` is a different rule that forbids terminating those clusters. Using the never-terminate list as an allowlist was a real defect, because the two lists answer two different questions.
# MAGIC
# MAGIC **Input:** Current cluster ID, resolved Volume path, secret scope, and Key Vault-backed secret name.
# MAGIC
# MAGIC **Output:** An in-memory connection string whose value is never printed, or a dry-run message.
# MAGIC
# MAGIC **Key concepts:** Allowlist versus denylist, existing compute, Unity Catalog Volumes, secret scopes, least exposure, fail-closed preflight.
# MAGIC
# MAGIC **Expected result:** An approved run prints the allowed cluster ID, storage readiness, and secret retrieval without displaying credentials; anywhere else the cell raises and stops.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We attach only to a pre-approved cluster and verify our storage boundary before any stream starts, and we never reuse a protection list as a permission list."
# MAGIC
# MAGIC **Rerun and cost considerations:** These checks never start, stop, restart, resize, install libraries on, or terminate a cluster. Secret lookup and path listing are read-only.

# COMMAND ----------

connection_string = None
if run_stream:
    if expected_published_events < 1 or expected_source_last_updated < 1:
        raise ValueError("Producer count and source timestamp must be positive for a live run.")
    if not secret_scope:
        raise ValueError("secret_scope is required when run_stream=true.")
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    if cluster_id not in APPROVED_RUN_CLUSTER_IDS:
        raise RuntimeError(f"Refusing unapproved cluster ID {cluster_id!r}.")
    dbutils.fs.ls(volume_root)
    connection_string = dbutils.secrets.get(
        scope=secret_scope,
        key=config.azure.event_hubs_secret_name,
    )
    print({"cluster_id": cluster_id, "volume_ready": True, "secret_retrieved": True})
else:
    print("Dry run: cluster, Volume, and secret were not accessed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Build the Event Hubs Kafka stream with an explicit schema
# MAGIC
# MAGIC **What:** Connect Spark to the Event Hubs Kafka endpoint and parse JSON into typed station columns plus broker lineage.
# MAGIC
# MAGIC **Why:** REST performs request-response collection, while Kafka provides an ordered event log that decouples the producer from Spark. Event Hubs exposes that log through Kafka-compatible endpoints.
# MAGIC
# MAGIC **Input:** Namespace, Event Hub, consumer group, in-memory credential, and explicit `StructType` event contract.
# MAGIC
# MAGIC **Output:** A lazy streaming DataFrame with event ID, execution ID, source and collection timestamps, partition, offset, broker timestamp, ingestion timestamp, raw JSON, and parse status.
# MAGIC
# MAGIC **Key concepts:** REST versus Kafka, Event Hubs versus Kafka, topics, partitions, offsets, Structured Streaming, explicit schemas.
# MAGIC
# MAGIC **Expected result:** Approved runs report `isStreaming=True`; diagnostic options show `[REDACTED]` in place of the credential.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Event Hubs is the managed broker, Kafka is the protocol Spark uses, and partition plus offset identifies each broker record."
# MAGIC
# MAGIC **Rerun and cost considerations:** Building a streaming DataFrame is lazy. `startingOffsets=earliest` is used only when this dedicated checkpoint has no prior offsets, so retained messages are preserved and safely correlated by execution ID.

# COMMAND ----------

bronze_stream_df = None
if run_stream:
    kafka_options = event_hubs_kafka_options(
        namespace=config.azure.event_hubs_namespace,
        connection_string=connection_string,
        event_hub_name=config.azure.event_hub_name,
        consumer_group=config.azure.consumer_group,
        starting_offsets=starting_offsets,
        max_offsets_per_trigger=config.streaming.max_events_per_trigger,
    )
    print(redacted_kafka_options(kafka_options))
    bronze_stream_df = parse_station_events(read_event_hubs_stream(spark, kafka_options))
    print(f"isStreaming={bronze_stream_df.isStreaming}")
    bronze_stream_df.printSchema()
else:
    print("Dry run: Kafka plan was not constructed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Run finite `availableNow` micro-batches with a bounded wait
# MAGIC
# MAGIC **What:** Append available broker records to Delta, save offsets in the isolated checkpoint, and wait for completion through `await_bounded_completion(query, timeout_seconds=...)`.
# MAGIC
# MAGIC **Why:** The previous version called `query.awaitTermination()` with no timeout. On shared cluster GP1 or GP2 a stalled query would then hold billable academy compute with no ceiling at all. The bounded helper returns after at most `stream_timeout_seconds`, and we record whether the query really finished before stopping it.
# MAGIC
# MAGIC **Input:** Parsed Kafka stream, the resolved Bronze table, the dedicated checkpoint directory, and the wait bound from Step 3.
# MAGIC
# MAGIC **Output:** Bronze Delta rows, a completion summary, and `query_terminated`, the honest record of whether the query ended on its own.
# MAGIC
# MAGIC **Key concepts:** Micro-batches, `availableNow`, Delta Lake, checkpoints, at-least-once delivery, timeouts as cost controls, practical idempotency.
# MAGIC
# MAGIC **Expected result:** The query processes all data available at its start and becomes inactive well inside the bound, without any cluster lifecycle action.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The checkpoint remembers offsets, `availableNow` bounds the work, and the timeout bounds the waiting, so a stuck stream costs minutes instead of hours."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is the billable step, and its cost is now capped twice: by `availableNow` and by the visible timeout. If the bound elapses we stop the query so nothing keeps streaming on shared compute, then let Step 10 fail the task. A rerun continues from committed offsets and never deletes Event Hub data or changes shared compute.

# COMMAND ----------

query = None
query_terminated = False
query_inactive_after_cleanup = True
completion = None
if run_stream:
    query = start_bronze_available_now(
        bronze_stream_df,
        table_name=target_table,
        checkpoint_path=stream_paths.checkpoint,
    )
    try:
        completion = await_bounded_completion(query, timeout_seconds=stream_timeout_seconds)
    finally:
        # Record the truth before cleanup, then stop only this query if it is still active.
        query_terminated = not query.isActive
        if query.isActive:
            query.stop()
        query_inactive_after_cleanup = not query.isActive
    print(
        {
            "completion": completion,
            "query_terminated": query_terminated,
            "query_inactive_after_cleanup": query_inactive_after_cleanup,
        }
    )
else:
    print("Dry run: no Event Hubs read or Delta write occurred.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Collect only this execution's Bronze rows
# MAGIC
# MAGIC **What:** Read the Bronze table filtered to this `execution_id` with a typed column comparison, and collect the small evidence columns.
# MAGIC
# MAGIC **Why:** The shared Event Hub retains other runs' messages, so reconciliation must look at this snapshot only. The filter used to be an f-string interpolated into SQL text; a column comparison cannot be turned into SQL by a crafted parameter value, no matter what other cells do.
# MAGIC
# MAGIC **Input:** The Bronze table, the required event contract, and the `execution_id` parameter.
# MAGIC
# MAGIC **Output:** A small list of dictionaries holding event fields, broker lineage, and the parse flag; raw payloads stay in the table.
# MAGIC
# MAGIC **Key concepts:** `F.col(...) == value` versus SQL string interpolation, injection safety at the point of use, predicate pushdown, driver-side collection of a bounded result.
# MAGIC
# MAGIC **Expected result:** The printed row count equals the producer's published count for this execution ID.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The execution filter is a typed column comparison, so the parameter is always treated as data and never as query text."
# MAGIC
# MAGIC **Rerun and cost considerations:** This reads only the one execution slice and never restarts the stream, so it is cheap and safe to repeat.

# COMMAND ----------

bronze_columns = [
    *REQUIRED_EVENT_FIELDS,
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "parse_error",
]
bronze_records = []
if run_stream:
    rows = (
        spark.table(target_table)
        .where(F.col("execution_id") == execution_id)
        .select(*bronze_columns)
        .collect()
    )
    bronze_records = [row.asDict(recursive=True) for row in rows]
    print({"bronze_rows_for_execution": len(bronze_records)})
else:
    print("Dry run: no Bronze rows were read.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Reconcile the published snapshot with Bronze lineage
# MAGIC
# MAGIC **What:** Compare counts, distinct IDs, duplicates, rejects, timestamps, partitions, and offset ranges with the producer manifest, and include safe query metrics.
# MAGIC
# MAGIC **Why:** A successful Spark query alone does not prove that every published event arrived once with complete lineage.
# MAGIC
# MAGIC **Input:** The collected execution slice, the expected published count, the expected source timestamp, the honest `query_terminated` flag, and safe progress metrics.
# MAGIC
# MAGIC **Output:** A secret-free report dictionary containing event identifiers, partition and offset ranges, timestamp completeness, reject count, duplicate count, and pass/fail status.
# MAGIC
# MAGIC **Key concepts:** End-to-end reconciliation, broker lineage, duplicate detection, reject accounting, query observability, secret-free logging.
# MAGIC
# MAGIC **Expected result:** The run passes only when the query terminated on its own and all expected records are unique, valid, timestamped, and from the expected snapshot.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We prove data movement by matching producer facts to Bronze records, not by relying on a green task icon."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is pure driver-side arithmetic over the small slice already collected. It starts no query and adds no cloud cost.

# COMMAND ----------

bronze_report = None
if run_stream:
    progress = query.lastProgress or {}
    safe_progress = {
        key: progress.get(key)
        for key in ("batchId", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond")
    }
    bronze_report = build_bronze_execution_report(
        bronze_records,
        execution_id=execution_id,
        expected_published_events=expected_published_events,
        expected_source_last_updated=expected_source_last_updated,
        query_terminated=query_terminated,
        query_progress=safe_progress,
    )
    bronze_report["query_inactive_after_cleanup"] = query_inactive_after_cleanup
    print({key: value for key, value in bronze_report.items() if key != "event_ids"})
else:
    print("Dry run: no Bronze reconciliation was performed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Prepare Silver deduplication, Quarantine, and shortages
# MAGIC
# MAGIC **What:** Route invalid records, separate duplicate event IDs, and calculate low-bike and low-dock conditions without writing another table.
# MAGIC
# MAGIC **Why:** Bronze preserves ingestion evidence, while Silver needs deterministic records and Quarantine needs every failed rule. Shortage flags provide the first operational result.
# MAGIC
# MAGIC **Input:** The records already collected for this execution and the configured shortage thresholds.
# MAGIC
# MAGIC **Output:** Silver, Quarantine, duplicate, and shortage counts added to the execution report.
# MAGIC
# MAGIC **Key concepts:** Medallion architecture, deterministic deduplication, Quarantine routing, business rules, Bronze-to-output reconciliation.
# MAGIC
# MAGIC **Expected result:** Bronze count equals Silver plus Quarantine plus duplicates; no additional cloud table is written in this first test.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every Bronze row has an accountable outcome, and the useful result is the set of stations that need bikes or docks."
# MAGIC
# MAGIC **Rerun and cost considerations:** This bounded driver-side calculation handles only the one-snapshot test data and creates no second workload or Lakeflow update.

# COMMAND ----------

if run_stream:
    prepared = prepare_station_batch(
        bronze_records,
        low_bike_threshold=config.business_rules.low_bike_threshold,
        low_dock_threshold=config.business_rules.low_dock_threshold,
    )
    bronze_report["medallion_preview"] = {
        "silver_rows": len(prepared.silver),
        "quarantine_rows": len(prepared.quarantine),
        "duplicate_rows": len(prepared.duplicates),
        "shortage_rows": len(prepared.shortages),
        "shortage_counts": prepared.shortage_counts,
        "reconciles": prepared.reconciles,
        "rule_counts": prepared.rule_counts,
    }
    print(bronze_report["medallion_preview"])
else:
    print("Dry run: Silver, Quarantine, and shortage previews were not calculated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Persist the JSON evidence and enforce the quality gate
# MAGIC
# MAGIC **What:** Write one execution-specific JSON report inside the target's UrbanFlow Volume and fail the task if reconciliation did not pass.
# MAGIC
# MAGIC **Why:** Durable evidence supports review, while a failing task prevents incomplete data, or a timed-out stream, from being described as successful.
# MAGIC
# MAGIC **Input:** Bronze reconciliation, medallion preview, and the isolated report path from Step 4.
# MAGIC
# MAGIC **Output:** `<execution_id>.bronze.json` with no connection string, token, or raw message payload.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence, quality gates, secret minimization, reproducibility, target isolation.
# MAGIC
# MAGIC **Expected result:** A passing run prints the report path and finishes; a failed or timed-out run still writes diagnostics and then raises an error.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The task produces auditable JSON and cannot report success when counts, IDs, timestamps, or the termination check disagree."
# MAGIC
# MAGIC **Rerun and cost considerations:** One small Volume write occurs only in the approved live run, inside the deployed target's own Volume. The shared cluster remains unchanged and is never terminated by this notebook.

# COMMAND ----------

if run_stream:
    report_path = write_json_report(bronze_report, stream_paths.report)
    print({"report_path": str(report_path), "status": bronze_report["status"]})
    if bronze_report["status"] != "PASS":
        raise RuntimeError("UrbanFlow Bronze reconciliation failed; inspect the JSON report.")
else:
    print("Dry run complete: no cloud data or resource was modified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC - The producer uses REST to collect one validated GBFS snapshot; Event Hubs then buffers those JSON events and exposes them through the Kafka protocol.
# MAGIC - A partition is an ordered shard, and an offset identifies one record in that shard. Both are preserved in Bronze.
# MAGIC - Structured Streaming applies the explicit schema in one or more micro-batches and stops after `availableNow` finishes.
# MAGIC - The checkpoint persists consumed offsets. Deterministic event IDs support downstream idempotency and reveal duplicate publication or replay.
# MAGIC - Bronze keeps traceability, while the prepared Silver, Quarantine, and shortage flow accounts for every row.
# MAGIC
# MAGIC **Lessons learned from the Milestone 2 audit:** Four defects were fixed in this notebook, and each one is worth explaining.
# MAGIC
# MAGIC 1. An unbounded `awaitTermination()` on shared compute has no cost ceiling; `await_bounded_completion(query, timeout_seconds=...)` with a visible parameter does.
# MAGIC 2. A never-terminate protection list is not an approved-to-attach permission list. The gate now uses `APPROVED_RUN_CLUSTER_IDS`, and still refuses every other cluster.
# MAGIC 3. Hardcoding the config file meant a `dev` deployment wrote the `azure` target's table and checkpoint. The job now passes the bundle's catalog, schema, and volume, so targets are isolated.
# MAGIC 4. Safety that depends on an earlier cell is fragile. The execution filter is now a typed column comparison instead of interpolated SQL text, so it is safe on its own.
# MAGIC
# MAGIC **Actual validation:** All producer, schema, checkpoint-path, reconciliation, deduplication, Quarantine, shortage, bounded-wait, target-isolation, and shared-cluster protection behavior is tested offline. This notebook has not been run against Azure in this milestone, so every "expected result" above is a prediction, not evidence.
# MAGIC
# MAGIC **Expected outputs of a first live run:** one Bronze row per published station event, one checkpoint directory, one JSON report with `status: PASS`, and a printed shortage preview. None of these exist yet.
# MAGIC
# MAGIC **Common errors:** Cluster not already running or not on the approved list, missing schema or Volume for the deployed target, denied secret read, incorrect policy in the connection string, checkpoint collision with the superseded `03_streaming.py`, unavailable Kafka connector, source count mismatch, malformed retained events, or a wait bound set too low for the published volume.
# MAGIC
# MAGIC **Troubleshooting:** Stop after a failed preflight, retain the JSON report, compare it with the producer report offline, check `query_terminated` before blaming the data, and never print the SASL options before redaction.
# MAGIC
# MAGIC **Review questions:**
# MAGIC
# MAGIC 1. Why does the producer use REST while Spark uses Kafka?
# MAGIC 2. What do partition and offset prove?
# MAGIC 3. Why is an explicit schema safer than inference for a stream?
# MAGIC 4. What state does the checkpoint preserve?
# MAGIC 5. How do deterministic event IDs make a rerun measurable?
# MAGIC 6. Why is a timeout on the wait a cost control rather than a convenience?
# MAGIC 7. Why is an allowlist of approved clusters different from a list of clusters we must never terminate?
# MAGIC 8. How does passing the bundle's catalog and schema keep the `dev` and `azure` targets isolated?
# MAGIC
# MAGIC **Presentation summary:** "One bounded producer snapshot is correlated by execution ID through Event Hubs into a checkpointed Bronze Delta table in the deployed target's own schema, waited on under a visible timeout, reconciled by exact IDs and broker lineage, then previewed as Silver, Quarantine, and operational shortages."

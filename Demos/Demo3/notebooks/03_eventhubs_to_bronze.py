# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - One Bounded Event Hubs Snapshot to Bronze
# MAGIC
# MAGIC **Business context:** A single real Citi Bike station-status snapshot proves the path from a public REST API through the existing Event Hub into a traceable Bronze Delta table.
# MAGIC
# MAGIC **Learning objectives:** Distinguish REST collection from Kafka transport, understand Event Hubs partitions and offsets, parse with an explicit schema, run bounded Structured Streaming micro-batches, use an isolated checkpoint, reconcile exact counts, and prepare idempotent downstream processing.
# MAGIC
# MAGIC **Academy labs:** Lab 3 streaming, Lab 4 data quality, Lab 5 Lakeflow preparation, Lab 7 testing, and Lab 9 automation safety.
# MAGIC
# MAGIC **Prerequisites:** The operator has approved one live run; GP1 or GP2 is already `RUNNING`; the UrbanFlow schema and Volume already exist; the notebook identity can read the named secret; and the producer report supplies the execution ID, source timestamp, and published count.
# MAGIC
# MAGIC **Inputs:** JSON station-status events from `urbanflow producer`, tagged with deterministic event IDs and one execution ID.
# MAGIC
# MAGIC **Outputs:** Bronze Delta rows, an isolated checkpoint, and one secret-free JSON execution report under the UrbanFlow Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load the tested project helpers
# MAGIC
# MAGIC **What:** Load configuration, Kafka parsing, reconciliation, and local medallion helpers.
# MAGIC
# MAGIC **Why:** Keeping protocol and validation logic in tested modules makes the notebook short enough to review before a live run.
# MAGIC
# MAGIC **Input:** `config/azure.yml` and the `src/urbanflow` package synchronized by the bundle.
# MAGIC
# MAGIC **Output:** Validated configuration and Python functions; no cloud data is read or written.
# MAGIC
# MAGIC **Key concepts:** Configuration as code, modular testing, lazy Spark plans, safety gates.
# MAGIC
# MAGIC **Expected result:** Only non-secret project, namespace, hub, and consumer-group names are printed.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The notebook orchestrates small tested functions, so the risky live path stays visible and reviewable."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports and local file reads do not contact Event Hubs or start a query.

# COMMAND ----------

import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.automation import PROTECTED_SHARED_CLUSTER_IDS
from urbanflow.config import load_config
from urbanflow.medallion import prepare_station_batch
from urbanflow.reporting import build_bronze_execution_report, write_json_report
from urbanflow.streaming import (
    event_hubs_kafka_options,
    isolated_stream_paths,
    parse_station_events,
    read_event_hubs_stream,
    redacted_kafka_options,
    start_bronze_available_now,
)

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
# MAGIC ## Step 2 - Require the producer manifest values and explicit live gate
# MAGIC
# MAGIC **What:** Read job parameters for approval, execution identity, expected count, source timestamp, and starting offsets.
# MAGIC
# MAGIC **Why:** The consumer must identify only the events published by this one snapshot and must remain a dry run by default.
# MAGIC
# MAGIC **Input:** Values copied from the secret-free producer JSON report plus `run_stream=true` only after approval.
# MAGIC
# MAGIC **Output:** A fully qualified Bronze table, checkpoint path, and execution-specific report path.
# MAGIC
# MAGIC **Key concepts:** Bounded execution, producer-consumer correlation, path isolation, fail-closed defaults.
# MAGIC
# MAGIC **Expected result:** Defaults show `run_stream=False`, expected count zero, and no query is eligible to start.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The producer and consumer share a run ID, so old retained Event Hub messages cannot be mistaken for this test."
# MAGIC
# MAGIC **Rerun and cost considerations:** Widgets have no cloud side effect. Reusing the checkpoint continues from committed Kafka offsets; it never clears Event Hubs messages.

# COMMAND ----------

dbutils.widgets.dropdown("run_stream", "false", ["false", "true"])
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("execution_id", "NOT_APPROVED")
dbutils.widgets.text("expected_published_events", "0")
dbutils.widgets.text("expected_source_last_updated", "0")
dbutils.widgets.dropdown("starting_offsets", "earliest", ["earliest", "latest"])

run_stream = dbutils.widgets.get("run_stream").lower() == "true"
secret_scope = dbutils.widgets.get("secret_scope").strip()
execution_id = dbutils.widgets.get("execution_id").strip()
expected_published_events = int(dbutils.widgets.get("expected_published_events"))
expected_source_last_updated = int(dbutils.widgets.get("expected_source_last_updated"))
starting_offsets = dbutils.widgets.get("starting_offsets")
target_table = f"{config.azure.catalog}.{config.azure.schema}.bronze_station_status"
stream_paths = isolated_stream_paths(
    volume_root=config.azure.volume_root,
    checkpoint_subpath=config.streaming.checkpoint_subpath,
    report_subpath=config.streaming.report_subpath,
    execution_id=execution_id,
)
print(
    {
        "run_stream": run_stream,
        "execution_id": execution_id,
        "expected_published_events": expected_published_events,
        "target_table": target_table,
        "checkpoint": stream_paths.checkpoint,
        "report": stream_paths.report,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Verify shared compute, Unity Catalog storage, and secret access
# MAGIC
# MAGIC **What:** Refuse unknown compute, confirm the isolated Volume is readable, and retrieve the connection string only inside an approved run.
# MAGIC
# MAGIC **Why:** GP1 and GP2 are shared academy resources, while the Bronze table and checkpoint must stay inside the user's UrbanFlow boundary.
# MAGIC
# MAGIC **Input:** Current cluster ID, configured Volume path, secret scope, and Key Vault-backed secret name.
# MAGIC
# MAGIC **Output:** An in-memory connection string whose value is never printed, or a dry-run message.
# MAGIC
# MAGIC **Key concepts:** Existing compute, Unity Catalog Volumes, secret scopes, least exposure, fail-closed preflight.
# MAGIC
# MAGIC **Expected result:** An approved run confirms the allowed cluster ID, storage path, and secret lookup without displaying credentials.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We attach only to a pre-approved shared cluster and verify our storage boundary before any stream starts."
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
    if cluster_id not in PROTECTED_SHARED_CLUSTER_IDS:
        raise RuntimeError(f"Refusing unapproved cluster ID {cluster_id!r}.")
    dbutils.fs.ls(config.azure.volume_root)
    connection_string = dbutils.secrets.get(
        scope=secret_scope,
        key=config.azure.event_hubs_secret_name,
    )
    print({"cluster_id": cluster_id, "volume_ready": True, "secret_retrieved": True})
else:
    print("Dry run: cluster, Volume, and secret were not accessed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Build the Event Hubs Kafka stream with an explicit schema
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
# MAGIC ## Step 5 - Run finite `availableNow` micro-batches into Bronze
# MAGIC
# MAGIC **What:** Append available broker records to Delta, save offsets in the isolated checkpoint, and wait until the bounded query terminates.
# MAGIC
# MAGIC **Why:** Structured Streaming processes finite micro-batches for this test, while the checkpoint makes an ordinary rerun continue from committed offsets.
# MAGIC
# MAGIC **Input:** Parsed Kafka stream, fully qualified Bronze table, and dedicated checkpoint directory.
# MAGIC
# MAGIC **Output:** Bronze Delta rows and a completed `StreamingQuery` object.
# MAGIC
# MAGIC **Key concepts:** Micro-batches, `availableNow`, Delta Lake, checkpoints, at-least-once delivery, practical idempotency.
# MAGIC
# MAGIC **Expected result:** The query processes all data available at its start and becomes inactive without an explicit cluster lifecycle action.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The checkpoint remembers offsets, `availableNow` bounds the work, and deterministic IDs let downstream Silver logic remove retries."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is the billable step. It runs once after approval and never deletes Event Hub data or changes shared compute.

# COMMAND ----------

query = None
if run_stream:
    query = start_bronze_available_now(
        bronze_stream_df,
        table_name=target_table,
        checkpoint_path=stream_paths.checkpoint,
    )
    query.awaitTermination()
    print({"query_id": str(query.id), "query_active": query.isActive})
else:
    print("Dry run: no Event Hubs read or Delta write occurred.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Reconcile the published snapshot with Bronze lineage
# MAGIC
# MAGIC **What:** Select only this execution ID and compare counts, distinct IDs, duplicates, rejects, timestamps, partitions, and offset ranges with the producer manifest.
# MAGIC
# MAGIC **Why:** A successful Spark query alone does not prove that every published event arrived once with complete lineage.
# MAGIC
# MAGIC **Input:** Completed Bronze table, expected published count, expected source timestamp, and safe query progress metrics.
# MAGIC
# MAGIC **Output:** A secret-free report containing event identifiers, partition and offset ranges, timestamp completeness, reject count, duplicate count, and pass/fail status.
# MAGIC
# MAGIC **Key concepts:** End-to-end reconciliation, broker lineage, duplicate detection, reject accounting, query observability.
# MAGIC
# MAGIC **Expected result:** The run passes only when the query is inactive and all expected records are unique, valid, timestamped, and from the expected snapshot.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We prove data movement by matching producer facts to Bronze records, not by relying on a green task icon."
# MAGIC
# MAGIC **Rerun and cost considerations:** This reads only the small execution slice from Bronze. It does not restart the stream.

# COMMAND ----------

bronze_report = None
bronze_records = []
if run_stream:
    bronze_columns = [
        "event_id",
        "execution_id",
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
        "source_last_updated",
        "collected_at",
        "source_url",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "parse_error",
    ]
    bronze_records = [
        row.asDict(recursive=True)
        for row in spark.table(target_table)
        .where(f"execution_id = '{execution_id}'")
        .select(*bronze_columns)
        .collect()
    ]
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
        query_terminated=not query.isActive,
        query_progress=safe_progress,
    )
    print({key: value for key, value in bronze_report.items() if key != "event_ids"})
else:
    print("Dry run: no Bronze reconciliation was performed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Prepare Silver deduplication, Quarantine, and shortages
# MAGIC
# MAGIC **What:** Route invalid records, separate duplicate event IDs, and calculate low-bike and low-dock conditions without writing another table.
# MAGIC
# MAGIC **Why:** Bronze preserves ingestion evidence, while Silver needs deterministic records and Quarantine needs every failed rule. Shortage flags provide the first operational result.
# MAGIC
# MAGIC **Input:** The small list of records already collected for this execution and configured shortage thresholds.
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
        "reconciles": prepared.reconciles,
        "rule_counts": prepared.rule_counts,
    }
    print(bronze_report["medallion_preview"])
else:
    print("Dry run: Silver, Quarantine, and shortage previews were not calculated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Persist the JSON evidence and enforce the quality gate
# MAGIC
# MAGIC **What:** Write one execution-specific JSON report inside the UrbanFlow Volume and fail the task if reconciliation did not pass.
# MAGIC
# MAGIC **Why:** Durable evidence supports review, while a failing task prevents incomplete data from being described as successful.
# MAGIC
# MAGIC **Input:** Bronze reconciliation, medallion preview, and the isolated report path.
# MAGIC
# MAGIC **Output:** `<execution_id>.bronze.json` with no connection string, token, or raw message payload.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence, quality gates, secret minimization, reproducibility.
# MAGIC
# MAGIC **Expected result:** A passing run prints the report path and finishes; a failed run still writes diagnostics and then raises an error.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The task produces auditable JSON and cannot report success when counts, IDs, timestamps, or termination checks disagree."
# MAGIC
# MAGIC **Rerun and cost considerations:** One small Volume write occurs only in the approved live run. The shared cluster remains unchanged and is never terminated by this notebook.

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
# MAGIC **Actual validation:** All producer, schema, checkpoint-path, reconciliation, deduplication, Quarantine, shortage, timeout, and shared-cluster protection behavior is tested offline. This notebook has not been run against Azure in this milestone.
# MAGIC
# MAGIC **Common errors:** Cluster not already running, missing schema or Volume, denied secret read, incorrect policy in the connection string, checkpoint collision, unavailable Kafka connector, source count mismatch, or malformed retained events.
# MAGIC
# MAGIC **Troubleshooting:** Stop after a failed preflight, retain the JSON report, compare it with the producer report offline, and never print the SASL options before redaction.
# MAGIC
# MAGIC **Review questions:**
# MAGIC
# MAGIC 1. Why does the producer use REST while Spark uses Kafka?
# MAGIC 2. What do partition and offset prove?
# MAGIC 3. Why is an explicit schema safer than inference for a stream?
# MAGIC 4. What state does the checkpoint preserve?
# MAGIC 5. How do deterministic event IDs make a rerun measurable?
# MAGIC
# MAGIC **Presentation summary:** "One bounded producer snapshot is correlated by execution ID through Event Hubs into checkpointed Bronze Delta, reconciled by exact IDs and broker lineage, then previewed as Silver, Quarantine, and operational shortages."

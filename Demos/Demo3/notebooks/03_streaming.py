# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow — Bounded Event Hubs Streaming with the Kafka Protocol
# MAGIC
# MAGIC **Business context:** Repeated station-status observations let operators distinguish a brief shortage from a recurring pattern.
# MAGIC
# MAGIC **Learning objectives:** Understand Event Hubs as a Kafka-compatible broker, build an explicit event schema, parse Kafka metadata, use checkpoints, and run a bounded `availableNow` micro-batch.
# MAGIC
# MAGIC **Academy labs:** Lab 3 streaming and incremental ingestion, Lab 7 testability, and the streaming input for Lab 5 Lakeflow.
# MAGIC
# MAGIC **Prerequisites:** Approved access to the existing `parvinbadalov_evh`, an authorized Databricks secret scope backed by Key Vault, and an approved target table/schema.
# MAGIC
# MAGIC **Inputs:** JSON station-status events published by `urbanflow.producer`.
# MAGIC
# MAGIC **Outputs:** Optional Bronze Delta rows with Kafka partition, offset, broker timestamp, raw JSON, and ingestion timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load configuration and streaming helpers
# MAGIC
# MAGIC **What:** Import the project configuration and the small Kafka parsing functions.
# MAGIC
# MAGIC **Why:** The notebook stays readable while protocol options and the event contract remain unit-testable Python.
# MAGIC
# MAGIC **Input:** `config/azure.yml` and `src/urbanflow/streaming.py`.
# MAGIC
# MAGIC **Output:** Validated resource names and helper functions; no stream is started.
# MAGIC
# MAGIC **Key concepts:** Modular code, explicit configuration, lazy Spark streaming plans.
# MAGIC
# MAGIC **Expected result:** The cell prints only non-secret namespace, hub, and consumer-group names.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “Configuration identifies the approved resource, while credentials stay in the secret manager.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Local file reads only. No Event Hubs messages are consumed.

# COMMAND ----------

import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.config import load_config
from urbanflow.streaming import (
    event_hubs_kafka_options,
    parse_station_events,
    read_event_hubs_stream,
    start_bronze_available_now,
)

config = load_config(PROJECT_ROOT / "config" / "azure.yml")
print(
    config.azure.event_hubs_namespace,
    config.azure.event_hub_name,
    config.azure.consumer_group,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Require explicit runtime approval inputs
# MAGIC
# MAGIC **What:** Define widgets for the Databricks secret scope and a `run_stream` switch.
# MAGIC
# MAGIC **Why:** Building and testing the code is free of cloud side effects. Reading Event Hubs and writing Delta must be a deliberate, reviewed action.
# MAGIC
# MAGIC **Input:** Operator-provided secret-scope name; the Key Vault secret key name comes from configuration.
# MAGIC
# MAGIC **Output:** A Boolean gate and target table/checkpoint strings.
# MAGIC
# MAGIC **Key concepts:** Secret scopes, approval gates, checkpoint isolation, bounded execution.
# MAGIC
# MAGIC **Expected result:** The default gate is false and the notebook remains a dry run.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “A normal notebook run cannot silently start the stream; the operator must name the approved secret scope and opt in.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Widget operations are safe. Do not enable the stream before the combined Azure approval.

# COMMAND ----------

dbutils.widgets.text("secret_scope", "")
dbutils.widgets.dropdown("run_stream", "false", ["false", "true"])

secret_scope = dbutils.widgets.get("secret_scope").strip()
run_stream = dbutils.widgets.get("run_stream").lower() == "true"
target_table = f"{config.azure.catalog}.{config.azure.schema}.bronze_station_status"
checkpoint_path = f"{config.azure.volume_root}/{config.streaming.checkpoint_subpath}"
print(f"run_stream={run_stream}; target_table={target_table}; checkpoint={checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Retrieve the connection string only inside the approved run
# MAGIC
# MAGIC **What:** Read the Event Hubs connection string from a Databricks secret scope when the gate is enabled.
# MAGIC
# MAGIC **Why:** Kafka's SASL configuration needs the connection string, but the value must never appear in source control, widgets, logs, or notebook output.
# MAGIC
# MAGIC **Input:** An approved scope and the existing Key Vault secret name `parvinbadalov-eventhub-cs`.
# MAGIC
# MAGIC **Output:** An in-memory string used only to build the Kafka reader.
# MAGIC
# MAGIC **Key concepts:** Key Vault-backed secret scopes, least exposure, SASL authentication.
# MAGIC
# MAGIC **Expected result:** Dry runs print a gate message. Approved runs confirm retrieval without printing the secret.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The code references a secret by name; the credential never enters Git or notebook output.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Secret lookup is read-only. Enabling the gate prepares a real Event Hubs connection and requires prior approval.

# COMMAND ----------

connection_string = None
if run_stream:
    if not secret_scope:
        raise ValueError("secret_scope is required when run_stream=true")
    connection_string = dbutils.secrets.get(
        scope=secret_scope,
        key=config.azure.event_hubs_secret_name,
    )
    print("Event Hubs credential retrieved securely; value withheld.")
else:
    print("Dry run: no secret was requested.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Build the Kafka streaming DataFrame with an explicit schema
# MAGIC
# MAGIC **What:** Configure the Kafka-compatible endpoint, subscribe to one event hub, and parse JSON events into trusted columns plus broker metadata.
# MAGIC
# MAGIC **Why:** Partitions distribute events, offsets identify each record within a partition, and explicit schemas prevent uncontrolled type drift.
# MAGIC
# MAGIC **Input:** Event Hubs namespace, hub, consumer group, connection string, and producer JSON contract.
# MAGIC
# MAGIC **Output:** `bronze_stream_df`, a lazy streaming plan. No records move until a write query starts.
# MAGIC
# MAGIC **Key concepts:** Kafka topic mapping, partitions, offsets, consumer groups, SASL/SSL, explicit `StructType`, ingestion metadata.
# MAGIC
# MAGIC **Expected result:** Approved runs show `isStreaming=True` and the Bronze schema; dry runs skip construction.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “An Event Hub behaves like a Kafka topic. Spark records partition and offset so each consumed message has traceable broker lineage.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Constructing the plan does not start the stream. The connection string is never displayed.

# COMMAND ----------

bronze_stream_df = None
if run_stream:
    kafka_options = event_hubs_kafka_options(
        namespace=config.azure.event_hubs_namespace,
        connection_string=connection_string,
        event_hub_name=config.azure.event_hub_name,
        consumer_group=config.azure.consumer_group,
        max_offsets_per_trigger=config.streaming.max_events_per_trigger,
    )
    raw_kafka_df = read_event_hubs_stream(spark, kafka_options)
    bronze_stream_df = parse_station_events(raw_kafka_df)
    print(f"isStreaming={bronze_stream_df.isStreaming}")
    bronze_stream_df.printSchema()
else:
    print("Dry run: Kafka streaming plan was not constructed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Consume only currently available events into Bronze
# MAGIC
# MAGIC **What:** Start a Delta streaming write with `availableNow=True` and wait for that bounded query to finish.
# MAGIC
# MAGIC **Why:** `availableNow` processes the backlog in one or more micro-batches and stops. The checkpoint stores consumed offsets so a safe restart continues rather than rereading everything.
# MAGIC
# MAGIC **Input:** Parsed Kafka stream, target Bronze table, and an isolated checkpoint path.
# MAGIC
# MAGIC **Output:** Optional appended Bronze rows and a completed query object.
# MAGIC
# MAGIC **Key concepts:** Micro-batches, checkpoints, replay, at-least-once source delivery, idempotent downstream processing, bounded triggers.
# MAGIC
# MAGIC **Expected result:** With approval, the query stops after available data is consumed. With the default gate, no query starts.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The checkpoint remembers Kafka offsets; `availableNow` gives us a finite demonstration instead of leaving costly compute running.”
# MAGIC
# MAGIC **Rerun and cost considerations:** This is the billable step. Run only after approval; monitor until completion and stop the compute afterward.

# COMMAND ----------

query = None
if run_stream:
    query = start_bronze_available_now(
        bronze_stream_df,
        table_name=target_table,
        checkpoint_path=checkpoint_path,
    )
    query.awaitTermination()
    print(f"Bounded streaming query completed: {query.id}")
else:
    print("Dry run complete: no Event Hubs read and no Delta write occurred.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Inspect micro-batch progress without exposing credentials
# MAGIC
# MAGIC **What:** Select a few safe progress metrics from Spark's last progress report.
# MAGIC
# MAGIC **Why:** Input counts, processing rates, and batch duration prove what the stream did without logging Kafka security options.
# MAGIC
# MAGIC **Input:** The completed `StreamingQuery` object.
# MAGIC
# MAGIC **Output:** A small dictionary of batch metrics, or a dry-run message.
# MAGIC
# MAGIC **Key concepts:** StreamingQuery progress, observability, secret-safe logging, performance evidence.
# MAGIC
# MAGIC **Expected result:** An approved run reports batch ID and row/rate metrics; credentials are absent.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “We validate both data and operational behavior, while deliberately excluding security configuration from logs.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Reading query metadata is free once the query exists. It never restarts the stream.

# COMMAND ----------

if query and query.lastProgress:
    progress = query.lastProgress
    safe_metrics = {
        "batchId": progress.get("batchId"),
        "numInputRows": progress.get("numInputRows"),
        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
        "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
        "durationMs": progress.get("durationMs"),
    }
    print(safe_metrics)
else:
    print("No streaming progress exists in dry-run mode.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC - The Event Hubs namespace supports Kafka, and one Event Hub maps to the subscribed Kafka topic.
# MAGIC - Partitions and offsets provide broker ordering and lineage; a consumer group tracks an independent reading application.
# MAGIC - Checkpoints preserve offsets and query state across safe restarts.
# MAGIC - Event Hubs delivery and retries can be at least once, so stable event IDs and idempotent downstream MERGE remain necessary.
# MAGIC - `availableNow` gives a finite demonstration and prevents an unattended continuous stream.
# MAGIC
# MAGIC **Actual validation:** Kafka option construction, schema code, duplicate suppression, and bounded-loop behavior pass offline tests. No live messages were published or consumed in this stage.
# MAGIC
# MAGIC **Common errors:** Missing Kafka connector, wrong secret scope, unauthorized consumer group, incorrect connection policy, reused checkpoint from another stream, or an unapproved target schema.
# MAGIC
# MAGIC **Troubleshooting:** Verify the exact workspace and non-secret resource names first. Never print the connection string. Use a new UrbanFlow checkpoint only after confirming the approved target.
# MAGIC
# MAGIC **Review questions:**
# MAGIC
# MAGIC 1. How does an Event Hub relate to a Kafka topic?
# MAGIC 2. What do partition and offset identify?
# MAGIC 3. Why is a checkpoint different from a Delta table?
# MAGIC 4. Why can at-least-once delivery create duplicates?
# MAGIC 5. Why is `availableNow` safer for this academy demonstration?
# MAGIC
# MAGIC **Presentation summary:** “The producer polls the real REST feed at its published interval and Event Hubs buffers those observations. Spark consumes them through Kafka in a bounded, checkpointed micro-batch and preserves broker lineage in Bronze.”

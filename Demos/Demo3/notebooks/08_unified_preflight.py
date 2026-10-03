# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Unified Job Preflight
# MAGIC
# MAGIC **Business context:** The primary UrbanFlow Job needs one visible gate that proves a bounded, approved mode was selected before any transformation branch starts.
# MAGIC
# MAGIC **Prerequisites:** The protected release workflow has verified that GP1 is already `RUNNING`; the bundle passes target parameters into this notebook.
# MAGIC
# MAGIC **Learning objectives:** Validate high-level orchestration parameters, enforce sample and monthly path isolation, and refuse any compute other than GP1.
# MAGIC
# MAGIC **Academy labs:** Labs 8 and 9.
# MAGIC
# MAGIC **Safety:** This notebook performs parameter and Spark-context reads only. It never starts compute, reads a data table, publishes Event Hubs messages, or writes data.
# MAGIC
# MAGIC **Actual validation:** On 2026-10-03 it returned `PASS` in sample mode for unified runs `4222809815373` and `284335864579341`, and in full-month mode for run `96337578882467`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the release contract
# MAGIC
# MAGIC **What:** Read the branch switches, execution identifiers, landing path selector and target names supplied by the Job.
# MAGIC
# MAGIC **Why:** One high-level contract keeps operators from editing notebook source or mixing the development sample with a future monthly run.
# MAGIC
# MAGIC **Input:** Job parameters from `urbanflow_end_to_end`.
# MAGIC
# MAGIC **Output:** Parsed booleans and strings only.
# MAGIC
# MAGIC **Key concepts:** Parameter-driven orchestration, bounded modes, immutable source code.
# MAGIC
# MAGIC **Expected result:** Every boolean is exactly `true` or `false` and every identifier is visible in the run inputs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The release declares what it will process before any branch can touch a table."
# MAGIC
# MAGIC **Rerun and cost considerations:** Widget reads are repeatable and do not trigger a Spark action.

# COMMAND ----------

import json

GP1_CLUSTER_ID = "0702-132442-toro5spu"
SAMPLE_HISTORICAL_ID = "urbanflow-hist-devsample40-20261002T0010Z"
SAMPLE_WEATHER_ID = "urbanflow-weather-devsample48h-20261002T0015Z"

boolean_names = ("run_station_pipeline", "run_historical", "run_weather", "run_full_month")
text_names = (
    "source_execution_id",
    "historical_execution_id",
    "historical_landing_subdir",
    "weather_execution_id",
    "weather_source",
    "catalog",
    "schema",
    "volume",
)
for name in boolean_names + text_names:
    dbutils.widgets.text(name, "")
raw_flags = {name: dbutils.widgets.get(name).strip().lower() for name in boolean_names}
if any(value not in {"true", "false"} for value in raw_flags.values()):
    raise ValueError(f"Every run switch must be true or false: {raw_flags}")
flags = {name: value == "true" for name, value in raw_flags.items()}
values = {name: dbutils.widgets.get(name).strip() for name in text_names}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Enforce compute and mode isolation
# MAGIC
# MAGIC **What:** Require GP1, validate target identifiers, and keep sample and full-month parameters in separate namespaces.
# MAGIC
# MAGIC **Why:** A Job attached to a different cluster or a monthly run pointed at the sample checkpoint could create cost or silently skip data.
# MAGIC
# MAGIC **Input:** Parsed parameters and the current Spark cluster tag.
# MAGIC
# MAGIC **Output:** A validated release mode, or an immediate failure before any table read.
# MAGIC
# MAGIC **Key concepts:** Existing compute, fail-closed validation, checkpoint isolation.
# MAGIC
# MAGIC **Expected result:** Sample mode selects the committed 40-row and 48-hour assets; monthly mode selects `202401-full` and new execution IDs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The same Job supports two modes, but the preflight prevents their checkpoints and lineage IDs from ever being mixed."
# MAGIC
# MAGIC **Rerun and cost considerations:** Reading the cluster tag is metadata-only; this notebook never changes GP1 lifecycle.

# COMMAND ----------

cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
if cluster_id != GP1_CLUSTER_ID:
    raise RuntimeError(f"Unified UrbanFlow must run on GP1 {GP1_CLUSTER_ID}, found {cluster_id!r}.")
if not any(flags[name] for name in ("run_station_pipeline", "run_historical", "run_weather")):
    raise ValueError("At least one UrbanFlow processing branch must be enabled.")
if flags["run_weather"] and not flags["run_historical"]:
    raise ValueError("Weather enrichment requires the selected historical execution.")
for name in ("catalog", "schema", "volume"):
    if not values[name].isidentifier():
        raise ValueError(f"{name} {values[name]!r} is not a plain identifier.")
if flags["run_station_pipeline"] and not values["source_execution_id"]:
    raise ValueError("The station branch requires a Bronze source_execution_id.")
if flags["run_full_month"]:
    if not flags["run_historical"] or values["historical_landing_subdir"] != "202401-full":
        raise ValueError("Full-month mode requires historical ingestion from 202401-full.")
    if values["historical_execution_id"] == SAMPLE_HISTORICAL_ID:
        raise ValueError("Full-month mode requires a new historical execution ID.")
    if values["weather_execution_id"] == SAMPLE_WEATHER_ID:
        raise ValueError("Full-month mode requires a new weather execution ID.")
    if flags["run_weather"] and values["weather_source"] != "archive_api":
        raise ValueError("Full-month weather must use the bounded Open-Meteo archive source.")
elif (
    values["historical_landing_subdir"] or values["historical_execution_id"] != SAMPLE_HISTORICAL_ID
):
    raise ValueError("Sample mode must use the committed historical sample and empty subdirectory.")
elif flags["run_weather"] and (
    values["weather_execution_id"] != SAMPLE_WEATHER_ID or values["weather_source"] != "sample_json"
):
    raise ValueError("Sample mode must use the committed 48-hour weather sample.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Emit the approved plan
# MAGIC
# MAGIC **What:** Print and return a compact, non-secret description of the branches and lineage identifiers that passed preflight.
# MAGIC
# MAGIC **Why:** The Job run should show exactly which bounded mode was accepted without exposing credentials or connection strings.
# MAGIC
# MAGIC **Input:** Validated flags, IDs and target names.
# MAGIC
# MAGIC **Output:** A JSON `PASS` result for the downstream task graph and release evidence.
# MAGIC
# MAGIC **Key concepts:** Auditable orchestration, lineage, secret-free evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `PASS`, cluster is GP1 and `event_hubs_producer` is `excluded`.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The first task leaves a small receipt for the exact safe mode the workflow approved."
# MAGIC
# MAGIC **Rerun and cost considerations:** Returning JSON is deterministic and does not persist a table or file.

# COMMAND ----------

report = {
    "status": "PASS",
    "cluster_id": cluster_id,
    "mode": "full_month" if flags["run_full_month"] else "development_sample",
    "branches": flags,
    "source_execution_id": values["source_execution_id"],
    "historical_execution_id": values["historical_execution_id"],
    "weather_execution_id": values["weather_execution_id"],
    "target": f"{values['catalog']}.{values['schema']}",
    "event_hubs_producer": "excluded",
}
print(report)
dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A multi-task Job is safer when its first task validates the operator's intent as data. The checks make sample and monthly lineage mutually exclusive, require GP1 explicitly, and record that Event Hubs publishing is outside this DAG.
# MAGIC
# MAGIC **Common errors:** Reusing the sample checkpoint for a monthly archive, selecting weather without its historical source, or treating a protected cluster ID as permission to start that cluster.
# MAGIC
# MAGIC **Troubleshooting:** If preflight fails, compare the Job parameters with the selected mode. If GP1 is not already running, stop the release and wait for its owner; never start it from UrbanFlow.
# MAGIC
# MAGIC **Review questions:** Why is a new execution ID required for the monthly archive? Why does weather depend on historical ingestion? Where is the Event Hubs producer excluded?
# MAGIC
# MAGIC **Presentation summary:** "The Job starts by proving it is on the approved running cluster and that one isolated, bounded data mode was selected."

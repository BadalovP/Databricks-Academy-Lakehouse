# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Unified Job Preflight
# MAGIC
# MAGIC **Business context:** The primary UrbanFlow Job needs one visible gate that proves a bounded, approved mode was selected before any transformation branch starts.
# MAGIC
# MAGIC **Prerequisites:** The Job runs on serverless compute; the bundle passes target parameters into this notebook.
# MAGIC
# MAGIC **Learning objectives:** Validate high-level orchestration parameters, enforce sample and monthly path isolation, and refuse any compute other than serverless.
# MAGIC
# MAGIC **Academy labs:** Labs 8 and 9.
# MAGIC
# MAGIC **Safety:** This notebook performs parameter and Spark-context reads only. It never starts compute, reads a data table, publishes Event Hubs messages, or writes data.
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-12 (Final Project run 896549012431425, task `preflight`): development-sample mode accepted on serverless compute with the Event Hubs producer excluded.

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
# MAGIC **How to explain it:** "The release declares what it will process before any branch can touch a table."
# MAGIC
# MAGIC **Rerun and cost considerations:** Widget reads are repeatable and do not trigger a Spark action.

# COMMAND ----------

import json
import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
sys.path.insert(0, str(NOTEBOOK_DIR.parent / "src"))

from urbanflow import landing
from urbanflow.automation import assert_serverless_compute
from urbanflow.landing import SAMPLE_HISTORICAL_ID, SAMPLE_WEATHER_ID, validate_station_source

boolean_names = ("run_station_pipeline", "run_historical", "run_weather", "run_full_month")
text_names = (
    "station_source",
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
# MAGIC **What:** Require serverless compute, validate target identifiers, and keep sample and full-month parameters in separate namespaces.
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
# MAGIC **How to explain it:** "The same Job supports two modes, but the preflight prevents their checkpoints and lineage IDs from ever being mixed."
# MAGIC
# MAGIC **Rerun and cost considerations:** Reading the cluster tag is metadata-only; this notebook never starts or changes any cluster.

# COMMAND ----------

assert_serverless_compute(spark.conf.get)
if not any(flags[name] for name in ("run_station_pipeline", "run_historical", "run_weather")):
    raise ValueError("At least one UrbanFlow processing branch must be enabled.")
if flags["run_weather"] and not flags["run_historical"]:
    raise ValueError("Weather enrichment requires the selected historical execution.")
for name in ("catalog", "schema", "volume"):
    if not values[name].isidentifier():
        raise ValueError(f"{name} {values[name]!r} is not a plain identifier.")
if flags["run_station_pipeline"]:
    validate_station_source(values["station_source"], values["source_execution_id"])
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
# MAGIC ## Step 2b - Check the station dimension the historical path needs
# MAGIC
# MAGIC **What:** When historical ingestion is requested without the station branch, require `dim_station_development_sample` to exist already.
# MAGIC
# MAGIC **Why:** Trips are joined to the station dimension that the station Gold task writes. With the station branch on, the Job runs Gold before the historical task; with it off, a fresh schema has no dimension and the historical task would fail late.
# MAGIC
# MAGIC **Input:** The validated switches and target.
# MAGIC
# MAGIC **Output:** Nothing, or a clear error before any task writes.
# MAGIC
# MAGIC **Key concepts:** Task dependencies, fail-early preconditions.
# MAGIC
# MAGIC **Expected result:** No error for the default sample run, which enables both branches.
# MAGIC
# MAGIC **How to explain it:** "A missing prerequisite is reported by the first task, not discovered by the fifth."
# MAGIC
# MAGIC **Rerun and cost considerations:** One metadata lookup.

# COMMAND ----------

dimension = f"{values['catalog']}.{values['schema']}.{landing.STATION_DIMENSION_TABLE}"
problem = landing.historical_dimension_problem(
    run_historical=flags["run_historical"],
    run_station_pipeline=flags["run_station_pipeline"],
    dimension_exists=spark.catalog.tableExists(dimension),
)
if problem:
    raise ValueError(problem)

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
# MAGIC **Expected result:** `status` is `PASS`, compute is `serverless` and `event_hubs_producer` is `excluded`.
# MAGIC
# MAGIC **How to explain it:** "The first task leaves a small receipt for the exact safe mode the workflow approved."
# MAGIC
# MAGIC **Rerun and cost considerations:** Returning JSON is deterministic and does not persist a table or file.

# COMMAND ----------

report = {
    "status": "PASS",
    "compute": "serverless",
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
# MAGIC A multi-task Job is safer when its first task validates the operator's intent as data. The checks make sample and monthly lineage mutually exclusive, require serverless compute explicitly, and record that Event Hubs publishing is outside this DAG.
# MAGIC
# MAGIC **Common errors:** Reusing the sample checkpoint for a monthly archive, selecting weather without its historical source, or treating a protected cluster ID as permission to start that cluster.
# MAGIC
# MAGIC **Troubleshooting:** If preflight fails, compare the Job parameters with the selected mode. If it reports a classic cluster, the task was attached to the wrong compute: redeploy the bundle so the task uses its serverless environment.
# MAGIC
# MAGIC **Review questions:** Why is a new execution ID required for the monthly archive? Why does weather depend on historical ingestion? Where is the Event Hubs producer excluded?
# MAGIC
# MAGIC **Presentation summary:** "The Job starts by proving it is on the approved running cluster and that one isolated, bounded data mode was selected."

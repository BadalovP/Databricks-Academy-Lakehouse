# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow — Identifying Low-Availability Citi Bike Stations
# MAGIC
# MAGIC **Business context:** Bike-sharing customers cannot rent from an empty station or return to a full station. This notebook turns a small, dated sample of real Citi Bike GBFS data into an understandable station-availability view.
# MAGIC
# MAGIC **Learning objectives:** Read JSON, create explicit Spark schemas, use `select`, `filter`, `join`, and `groupBy`, apply reusable business rules, prepare an idempotent Delta write, and query the result with SQL.
# MAGIC
# MAGIC **Academy labs:** Lab 1 fundamentals, Lab 2 Unity Catalog, Lab 4 Delta patterns, and the business foundation for Lab 6 analytics.
# MAGIC
# MAGIC **Prerequisites:** A Databricks runtime with this project deployed. The default walkthrough uses committed samples and does not call a live API.
# MAGIC
# MAGIC **Inputs:** `data/samples/station_information.sample.json` and `station_status.sample.json`, captured from the official Citi Bike GBFS feeds.
# MAGIC
# MAGIC **Outputs:** Spark DataFrames for current station availability, low-bike and low-dock views, an aggregate summary, and an optional Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load project configuration and reusable functions
# MAGIC
# MAGIC **What:** Add the project source directory to Python's import path, load the safe development YAML, and import the UrbanFlow Spark transformation.
# MAGIC
# MAGIC **Why:** Central configuration keeps thresholds and resource names consistent. Importable functions let pytest validate the same business rules used in the notebook.
# MAGIC
# MAGIC **Input:** `config/dev.yml` and `src/urbanflow/`.
# MAGIC
# MAGIC **Output:** A validated `config` object and the `with_shortage_flags` function.
# MAGIC
# MAGIC **Key concepts:** Python modules, YAML configuration, separation of business logic from notebook orchestration.
# MAGIC
# MAGIC **Expected result:** The cell prints the environment and the two demonstration thresholds.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The notebook contains the teaching flow, while reusable and tested logic lives in a normal Python package.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Local file reads only. Safe to repeat and does not start additional compute beyond the already attached notebook compute.

# COMMAND ----------

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.config import load_config
from urbanflow.transformations import with_shortage_flags

config = load_config(PROJECT_ROOT / "config" / "dev.yml")
print(
    f"Environment={config.environment}; "
    f"low bikes <= {config.business_rules.low_bike_threshold}; "
    f"low docks <= {config.business_rules.low_dock_threshold}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Make writes an explicit choice
# MAGIC
# MAGIC **What:** Create widgets for the target catalog, schema, and a `write_enabled` safety switch.
# MAGIC
# MAGIC **Why:** Reading and transforming the sample is safe. Creating a schema or table changes the workspace, so the notebook requires an intentional switch.
# MAGIC
# MAGIC **Input:** Safe defaults from `config/dev.yml`.
# MAGIC
# MAGIC **Output:** Three widget values used later by the optional Delta section.
# MAGIC
# MAGIC **Key concepts:** Databricks widgets, parameterized notebooks, safe defaults, idempotency.
# MAGIC
# MAGIC **Expected result:** `write_enabled` is `false` unless the operator deliberately changes it.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The educational analysis runs read-only by default; persistent writes are visible and opt-in.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Widget creation is safe to repeat. Keep writes disabled until the UrbanFlow schema is approved.

# COMMAND ----------

dbutils.widgets.text("catalog", config.azure.catalog)
dbutils.widgets.text("schema", config.azure.schema)
dbutils.widgets.dropdown("write_enabled", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
write_enabled = dbutils.widgets.get("write_enabled").lower() == "true"
print(f"Target={catalog}.{schema}; write_enabled={write_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Read the saved real GBFS JSON samples
# MAGIC
# MAGIC **What:** Load the station-information and station-status JSON envelopes with ordinary Python.
# MAGIC
# MAGIC **Why:** Seeing the real nested `data.stations` shape makes the REST response easier to understand before Spark is introduced.
# MAGIC
# MAGIC **Input:** Two attributed, dated samples containing 40 stations each.
# MAGIC
# MAGIC **Output:** Python lists named `information_rows` and `status_rows`.
# MAGIC
# MAGIC **Key concepts:** REST JSON envelopes, source timestamps, collection timestamps, offline reproducibility.
# MAGIC
# MAGIC **Expected result:** Both feeds contain the same 40 selected station UUIDs and report a 60-second TTL.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The live producer discovers these feeds dynamically, while this notebook uses a saved sample so the lesson is repeatable.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Local file reads only. The data is historical evidence, not a claim about current bike availability.

# COMMAND ----------

sample_dir = PROJECT_ROOT / "data" / "samples"
information_payload = json.loads(
    (sample_dir / "station_information.sample.json").read_text(encoding="utf-8")
)
status_payload = json.loads((sample_dir / "station_status.sample.json").read_text(encoding="utf-8"))

information_rows = information_payload["data"]["stations"]
collection_time = datetime.fromtimestamp(status_payload["last_updated"], UTC).isoformat()
status_rows = [
    {**row, "source_last_updated": status_payload["last_updated"], "collected_at": collection_time}
    for row in status_payload["data"]["stations"]
]
print(
    f"information={len(information_rows)}, status={len(status_rows)}, "
    f"ttl={status_payload['ttl']} seconds"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Convert Python records into explicitly typed Spark DataFrames
# MAGIC
# MAGIC **What:** Define the trusted columns and types, then create station-reference and station-status DataFrames.
# MAGIC
# MAGIC **Why:** Explicit schemas prevent a malformed value from silently changing a column type and form the first data contract.
# MAGIC
# MAGIC **Input:** The two Python record lists from Step 3.
# MAGIC
# MAGIC **Output:** `station_information_df` and `station_status_df`.
# MAGIC
# MAGIC **Key concepts:** `StructType`, nullable fields, schema enforcement, DataFrame creation.
# MAGIC
# MAGIC **Expected result:** Spark shows string IDs, numeric availability counts, and timestamp metadata with stable types.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The REST API is flexible JSON; the lakehouse starts by converting it into a strict contract.”
# MAGIC
# MAGIC **Rerun and cost considerations:** In-memory transformation over 40 rows. Safe and inexpensive to repeat.

# COMMAND ----------

from pyspark.sql import types as T

information_schema = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("short_name", T.StringType(), True),
        T.StructField("name", T.StringType(), False),
        T.StructField("lat", T.DoubleType(), False),
        T.StructField("lon", T.DoubleType(), False),
        T.StructField("capacity", T.IntegerType(), True),
    ]
)
status_schema = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("num_bikes_available", T.IntegerType(), False),
        T.StructField("num_docks_available", T.IntegerType(), False),
        T.StructField("is_installed", T.IntegerType(), False),
        T.StructField("is_renting", T.IntegerType(), False),
        T.StructField("is_returning", T.IntegerType(), False),
        T.StructField("last_reported", T.LongType(), False),
        T.StructField("source_last_updated", T.LongType(), False),
        T.StructField("collected_at", T.StringType(), False),
    ]
)

station_information_df = spark.createDataFrame(information_rows, information_schema)
station_status_df = spark.createDataFrame(status_rows, status_schema)
station_information_df.printSchema()
station_status_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Select useful columns and filter operational stations
# MAGIC
# MAGIC **What:** Keep the fields needed by operators and remove stations that are not installed, renting, and returning.
# MAGIC
# MAGIC **Why:** A maintenance closure is different from a supply shortage and should not be sent to the redistribution queue.
# MAGIC
# MAGIC **Input:** `station_status_df`.
# MAGIC
# MAGIC **Output:** `operational_status_df`.
# MAGIC
# MAGIC **Key concepts:** Spark `select`, boolean expressions, `filter`, lazy evaluation.
# MAGIC
# MAGIC **Expected result:** Only operational stations remain, with bike and dock counts ready for analysis.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “We separate service availability from inventory availability before applying business thresholds.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Read-only lazy transformations. `display` executes a small Spark job over 40 rows.

# COMMAND ----------

operational_status_df = station_status_df.select(
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
    "collected_at",
).filter("is_installed = 1 AND is_renting = 1 AND is_returning = 1")
display(operational_status_df.orderBy("num_bikes_available"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Join live status to station names, coordinates, and capacity
# MAGIC
# MAGIC **What:** Join the operational observations to station information using the current GBFS UUID `station_id`.
# MAGIC
# MAGIC **Why:** Counts alone are not actionable; operators need station names and locations. Capacity also provides context for future ratios.
# MAGIC
# MAGIC **Input:** `operational_status_df` and `station_information_df`.
# MAGIC
# MAGIC **Output:** `station_snapshot_df`, one enriched row per selected station observation.
# MAGIC
# MAGIC **Key concepts:** Equi-join, aliases, reference data, fact-to-dimension enrichment.
# MAGIC
# MAGIC **Expected result:** Every sample status row has its station name. Historical trip IDs use GBFS `short_name`, documented separately.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “Current feeds join on UUID; historical trip station IDs map to `short_name`, so we preserve both identifiers.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Read-only join over a tiny sample. Safe to repeat.

# COMMAND ----------

station_snapshot_df = (
    operational_status_df.alias("status")
    .join(
        station_information_df.alias("station"),
        on="station_id",
        how="left",
    )
    .select(
        "station_id",
        "station.short_name",
        "station.name",
        "station.lat",
        "station.lon",
        "station.capacity",
        "status.num_bikes_available",
        "status.num_docks_available",
        "status.is_installed",
        "status.is_renting",
        "status.is_returning",
        "status.last_reported",
        "status.collected_at",
    )
)
display(station_snapshot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Apply the low-bike and low-dock business rules
# MAGIC
# MAGIC **What:** Add flags and a readable status using the reusable transformation function.
# MAGIC
# MAGIC **Why:** The first business rule highlights stations that may deserve investigation. The threshold is configurable and is not presented as an industry standard.
# MAGIC
# MAGIC **Input:** `station_snapshot_df` and the YAML thresholds, both currently set to two.
# MAGIC
# MAGIC **Output:** `classified_snapshot_df`, `low_bikes_df`, and `low_docks_df`.
# MAGIC
# MAGIC **Key concepts:** Derived columns, conditional expressions, reusable transformations, observed facts versus operational heuristics.
# MAGIC
# MAGIC **Expected result:** Stations at or below the threshold appear in the appropriate view.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “These flags are simple demonstration indicators; repeated observations and capacity ratios will support stronger prioritization later.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Read-only and deterministic for the same sample and configuration.

# COMMAND ----------

classified_snapshot_df = with_shortage_flags(
    station_snapshot_df,
    low_bike_threshold=config.business_rules.low_bike_threshold,
    low_dock_threshold=config.business_rules.low_dock_threshold,
)
low_bikes_df = classified_snapshot_df.filter("is_low_bikes")
low_docks_df = classified_snapshot_df.filter("is_low_docks")
display(classified_snapshot_df.orderBy("availability_status", "num_bikes_available"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Group stations into a dashboard-ready summary
# MAGIC
# MAGIC **What:** Count stations and average their availability by classification.
# MAGIC
# MAGIC **Why:** A dashboard needs both station-level detail and a small KPI summary for quick operational interpretation.
# MAGIC
# MAGIC **Input:** `classified_snapshot_df`.
# MAGIC
# MAGIC **Output:** `availability_summary_df`.
# MAGIC
# MAGIC **Key concepts:** `groupBy`, aggregation, averages, analytical grain.
# MAGIC
# MAGIC **Expected result:** One row per availability status with station counts and mean bike/dock availability.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The detailed fact supports maps and tables; this aggregate supports headline KPIs.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Small read-only aggregation. Safe to repeat.

# COMMAND ----------

from pyspark.sql import functions as F

availability_summary_df = (
    classified_snapshot_df.groupBy("availability_status")
    .agg(
        F.count("*").alias("station_count"),
        F.round(F.avg("num_bikes_available"), 2).alias("average_bikes"),
        F.round(F.avg("num_docks_available"), 2).alias("average_docks"),
    )
    .orderBy("availability_status")
)
display(availability_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Prepare an idempotent Delta MERGE
# MAGIC
# MAGIC **What:** When explicitly enabled, create the approved schema/table if needed and MERGE observations by `station_id` plus `last_reported`.
# MAGIC
# MAGIC **Why:** The same observation may be processed again after a retry. MERGE prevents that retry from creating duplicate facts.
# MAGIC
# MAGIC **Input:** `classified_snapshot_df` and the target widgets.
# MAGIC
# MAGIC **Output:** Optional managed Delta table `fact_station_snapshot`; otherwise a clear dry-run message.
# MAGIC
# MAGIC **Key concepts:** Delta Lake, natural event key, idempotency, managed table, Unity Catalog identifiers.
# MAGIC
# MAGIC **Expected result:** With writes disabled, no persistent object changes. With approved writes enabled, only unseen observations are inserted.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “Retries are safe because the event grain is station plus source report time, and MERGE inserts only missing keys.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Default is read-only. Enabling writes changes Unity Catalog and consumes the attached compute; use only after approval.

# COMMAND ----------

identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
if not identifier_pattern.fullmatch(catalog) or not identifier_pattern.fullmatch(schema):
    raise ValueError("Catalog and schema widgets must be simple SQL identifiers.")

target_table = f"{catalog}.{schema}.fact_station_snapshot"
classified_snapshot_df.createOrReplaceTempView("urbanflow_station_snapshot_source")
if write_enabled:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA "
        "AS SELECT * FROM urbanflow_station_snapshot_source WHERE 1 = 0"
    )
    spark.sql(
        f"MERGE INTO {target_table} target "
        "USING urbanflow_station_snapshot_source source "
        "ON target.station_id = source.station_id "
        "AND target.last_reported = source.last_reported "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    print(f"Idempotent MERGE completed: {target_table}")
else:
    print(f"Dry run only. No table was created or modified: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Query the result with SQL for a basic dashboard
# MAGIC
# MAGIC **What:** Run a Spark SQL query against the temporary view, ordered by the stations with the fewest bikes.
# MAGIC
# MAGIC **Why:** AI/BI dashboards and SQL users can consume the same trusted columns without reproducing the Python logic.
# MAGIC
# MAGIC **Input:** Temporary view `urbanflow_station_snapshot_source`.
# MAGIC
# MAGIC **Output:** A dashboard-ready table of stations requiring attention.
# MAGIC
# MAGIC **Key concepts:** Spark SQL, temporary views, semantic reuse, dashboard source query.
# MAGIC
# MAGIC **Expected result:** Low-bike and low-dock stations appear first with their counts and coordinates.
# MAGIC
# MAGIC **How to explain it to my supervisor:** “The transformation is defined once, then exposed to SQL for operational reporting.”
# MAGIC
# MAGIC **Rerun and cost considerations:** Read-only query over the temporary sample view. Safe to repeat.

# COMMAND ----------

attention_df = spark.sql(
    """
    SELECT
      station_id,
      name,
      short_name,
      num_bikes_available,
      num_docks_available,
      availability_status,
      lat,
      lon
    FROM urbanflow_station_snapshot_source
    WHERE is_low_bikes OR is_low_docks
    ORDER BY num_bikes_available, num_docks_available
    """
)
display(attention_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC - Real GBFS JSON becomes more reliable after explicit schema enforcement.
# MAGIC - Operational-state filtering prevents closures from being confused with redistribution shortages.
# MAGIC - The first thresholds are configurable educational heuristics, not validated service standards.
# MAGIC - Current status joins to current station information by UUID; January 2024 trip IDs map to `short_name`.
# MAGIC - Delta MERGE makes retries idempotent, while the notebook remains read-only by default.
# MAGIC
# MAGIC **Actual validation:** The committed samples and transformation package are validated by offline pytest. Live Azure table creation has not been executed in this stage.
# MAGIC
# MAGIC **Common errors:** Wrong project path, unavailable `dbutils` outside Databricks, an unapproved schema name, or enabling writes without Unity Catalog permission.
# MAGIC
# MAGIC **Troubleshooting:** Confirm the bundle deployed the `data`, `config`, and `src` folders; keep `write_enabled=false`; verify the catalog/schema widgets before any write.
# MAGIC
# MAGIC **Review questions:**
# MAGIC
# MAGIC 1. Why do we preserve both source and collection timestamps?
# MAGIC 2. Why is an out-of-service station different from a low-bike station?
# MAGIC 3. What makes the Delta MERGE safe to rerun?
# MAGIC 4. Why can historical `start_station_id` not be joined directly to the current UUID?
# MAGIC 5. Which additional observations would make the priority indicator more trustworthy?
# MAGIC
# MAGIC **Presentation summary:** “UrbanFlow converts real Citi Bike station observations into a governed, repeatable shortage view. This first milestone proves the business rule and data contract locally; cloud persistence remains an explicit approved step.”

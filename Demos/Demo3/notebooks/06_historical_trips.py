# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Historical Citi Bike Trips with Auto Loader
# MAGIC
# MAGIC **Business context:** The availability tables hold one real snapshot, which can never show a trend. Official Citi Bike trip archives contain weeks of real rides, so this is where genuine historical demand comes from, and it is the evidence an operator would use to plan capacity rather than react to a single reading.
# MAGIC
# MAGIC **Prerequisites:** The existing UrbanFlow schema and managed Volume exist, at least one official trip CSV has been landed under the Volume's `landing/historical_trips` directory, GP1 or GP2 is already `RUNNING`, and this run has explicit approval.
# MAGIC
# MAGIC **Learning objectives:** Use Auto Loader with an explicit schema and a rescued-data column, keep station identifiers as strings, join historical trips to current stations on `short_name` rather than the UUID, route every row to valid, quarantine or duplicate, and prove the counts reconcile.
# MAGIC
# MAGIC **Academy labs:** Labs 3, 4, 5, 6, 7, 8, and 9.
# MAGIC
# MAGIC **Safety:** `run_ingest` defaults to `false`, so the committed default exits before any read or write. The notebook never starts, stops or resizes a cluster, never downloads an archive, and never writes outside the UrbanFlow schema.
# MAGIC
# MAGIC **Actual validation:** This notebook has not been run in Azure. Every function it calls is covered by the offline and local-Spark test suite, and the figures below are expectations rather than observations until an approved run produces evidence.
# MAGIC
# MAGIC **The one mistake this notebook exists to prevent:** historical trips carry values like `7407.13` in `start_station_id`. That is the GBFS **short name**, not the UUID `station_id`. Joining on `station_id` matches nothing and produces an empty result that looks like missing data rather than a join bug, so the match rate is reported and a rate of exactly zero fails the run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the tested Auto Loader and quality helpers
# MAGIC
# MAGIC **What:** Put the synchronized project package on the path and import the historical ingestion, join, demand and persistence functions.
# MAGIC
# MAGIC **Why:** The quality rules and the join key belong in reviewed, unit-tested modules rather than in presentation code where they cannot be tested.
# MAGIC
# MAGIC **Input:** The bundle-synchronized `src/urbanflow` modules.
# MAGIC
# MAGIC **Output:** Imported functions only; no Spark action occurs.
# MAGIC
# MAGIC **Key concepts:** Modular design, tested transformations, one implementation of each rule.
# MAGIC
# MAGIC **Expected result:** Imports complete without touching storage.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The notebook orchestrates functions that already have tests; it does not define the business rules itself."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports are free and repeatable, and change no data.

# COMMAND ----------

import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
PROJECT_ROOT = NOTEBOOK_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS
from urbanflow.gold import station_dimension
from urbanflow.historical import (
    daily_trip_demand,
    historical_landing_paths,
    join_trips_to_stations,
    persist_historical_outputs,
    read_trips_autoloader,
    reconcile_historical,
    rider_mix_summary,
    split_trips_and_quarantine,
    start_historical_available_now,
    trip_duration_profile,
    trip_join_match_rate,
    with_trip_lineage,
)
from urbanflow.reporting import write_json_report
from urbanflow.streaming import await_bounded_completion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and fail closed before any Spark action
# MAGIC
# MAGIC **What:** Declare the approval gate, the execution ID, the bundle target identifiers and the wait bound, then verify the attached cluster.
# MAGIC
# MAGIC **Why:** A default Job run must not read or write anything, and only the two academy clusters are approved attachment targets. Exiting before the first Spark action is what makes an accidental run cost nothing.
# MAGIC
# MAGIC **Input:** Job parameters supplied by the deployed bundle target.
# MAGIC
# MAGIC **Output:** Validated values, or an immediate `DRY_RUN` exit.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, existing-cluster allowlist, bounded waits, target isolation.
# MAGIC
# MAGIC **Expected result:** The committed default exits with `DRY_RUN`; an approved run continues only on GP1 or GP2.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Nothing happens until one visible parameter is set to true and the cluster is on the approved list."
# MAGIC
# MAGIC **Rerun and cost considerations:** The default path performs no Spark action at all, so a scheduled or accidental run is free.

# COMMAND ----------

dbutils.widgets.dropdown("run_ingest", "false", ["false", "true"])
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
dbutils.widgets.text("stream_timeout_seconds", "600")
run_ingest = dbutils.widgets.get("run_ingest").lower() == "true"
execution_id = dbutils.widgets.get("execution_id").strip()
target_catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
target_volume = dbutils.widgets.get("volume").strip()
stream_timeout_seconds = float(dbutils.widgets.get("stream_timeout_seconds"))
if not run_ingest:
    dbutils.notebook.exit("DRY_RUN: no archive was read and no Delta table was written.")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
if cluster_id not in APPROVED_RUN_CLUSTER_IDS:
    raise RuntimeError(f"Cluster {cluster_id!r} is not approved for UrbanFlow.")
if not execution_id:
    raise ValueError("execution_id must identify this historical ingestion run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Resolve isolated tables and Volume paths
# MAGIC
# MAGIC **What:** Validate the three Unity Catalog identifiers, then build the table names and the landing, schema and checkpoint paths.
# MAGIC
# MAGIC **Why:** The development and Azure targets must never share a table or a checkpoint. Validating each identifier also prevents a path or SQL injection through a Job parameter.
# MAGIC
# MAGIC **Input:** Catalog, schema and Volume names passed from the deployed bundle target.
# MAGIC
# MAGIC **Output:** Fully qualified table names and three distinct Volume directories.
# MAGIC
# MAGIC **Key concepts:** Unity Catalog three-level names, managed Volumes, separate schema and checkpoint locations.
# MAGIC
# MAGIC **Expected result:** Paths resolve under the existing `urbanflow_landing` Volume; the Auto Loader schema location and the streaming checkpoint are different directories.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Everything this run touches lives inside the one Volume and schema the project already owns."
# MAGIC
# MAGIC **Rerun and cost considerations:** Name construction is free. The checkpoint is per execution, so a rerun is a clean bounded read rather than a resume of partial progress.

# COMMAND ----------

for label, value in (
    ("catalog", target_catalog),
    ("schema", target_schema),
    ("volume", target_volume),
):
    if not value.isidentifier():
        raise ValueError(f"{label} {value!r} is not a plain identifier.")
table_root = f"{target_catalog}.{target_schema}"
volume_root = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}"
paths = historical_landing_paths(volume_root, execution_id=execution_id)
bronze_trips_table = f"{table_root}.bronze_historical_trips"
historical_tables = {
    "trips": f"{table_root}.silver_historical_trips",
    "quarantine": f"{table_root}.quarantine_historical_trips",
    "duplicates": f"{table_root}.duplicate_historical_trips",
    "daily_demand": f"{table_root}.gold_daily_trip_demand",
}
report_path = f"{volume_root}/reports/historical/{execution_id}.historical.json"
print({"paths": paths, "tables": historical_tables})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Confirm the archive has actually been landed
# MAGIC
# MAGIC **What:** List the landing directory and refuse to continue if it holds no CSV file.
# MAGIC
# MAGIC **Why:** Auto Loader on an empty directory succeeds and processes nothing, which looks identical to a successful run that found no new data. Checking first turns a silent no-op into a clear error, and it also confirms this notebook never downloads the archive itself.
# MAGIC
# MAGIC **Input:** The landing directory inside the existing Volume.
# MAGIC
# MAGIC **Output:** The list of files found, or a raised error.
# MAGIC
# MAGIC **Key concepts:** Explicit preconditions, operator-supplied data, distinguishing "nothing new" from "nothing there".
# MAGIC
# MAGIC **Expected result:** At least one `.csv` file is listed. A zipped archive must be expanded before this step, because Auto Loader reads CSV and not ZIP.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We check the data is really there before claiming a successful load of nothing."
# MAGIC
# MAGIC **Rerun and cost considerations:** A directory listing is a metadata operation and costs effectively nothing.

# COMMAND ----------

landed = [entry for entry in dbutils.fs.ls(paths["landing"]) if entry.name.endswith(".csv")]
if not landed:
    raise RuntimeError(
        f"No CSV file found under {paths['landing']}. Land an official Citi Bike trip "
        "archive there and expand it before rerunning; this notebook never downloads it."
    )
print(
    {
        "files": [entry.name for entry in landed],
        "total_bytes": sum(entry.size for entry in landed),
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Land the archive into Bronze with a bounded Auto Loader read
# MAGIC
# MAGIC **What:** Start one `availableNow` Auto Loader stream into the Bronze trips table and wait for it inside an explicit timeout.
# MAGIC
# MAGIC **Why:** `availableNow` processes exactly the files present and then stops, which gives a bounded cost rather than an open-ended stream on shared academy compute. The explicit schema means Auto Loader never infers `7407.13` as a number, which would silently destroy the join key. Unexpected columns land in the rescued-data column instead of failing the read.
# MAGIC
# MAGIC **Input:** The landed CSV files, the explicit trip schema and the per-execution schema and checkpoint locations.
# MAGIC
# MAGIC **Output:** The Bronze trips table and the query's final progress, with the stream always stopped in a `finally` block.
# MAGIC
# MAGIC **Key concepts:** Auto Loader `cloudFiles`, explicit schema, rescued data column, `availableNow` trigger, bounded waits, separate schema and checkpoint locations.
# MAGIC
# MAGIC **Expected result:** The stream terminates by itself well inside the timeout, and the Bronze row count equals the number of data rows in the landed files.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "It reads the files that are there, writes them once, and stops. It cannot run indefinitely."
# MAGIC
# MAGIC **Rerun and cost considerations:** A rerun with the same execution ID uses the same checkpoint and reprocesses nothing. A new execution ID reprocesses every file, so the ID is the deliberate control over cost here.

# COMMAND ----------

query = None
try:
    trips_stream = read_trips_autoloader(spark, paths["landing"], schema_location=paths["schema"])
    query = start_historical_available_now(
        trips_stream,
        table_name=bronze_trips_table,
        checkpoint_location=paths["checkpoint"],
    )
    progress = await_bounded_completion(query, timeout_seconds=stream_timeout_seconds)
finally:
    if query is not None and query.isActive:
        query.stop()
query_inactive_after_cleanup = query is None or not query.isActive
print({"progress": progress, "query_stopped": query_inactive_after_cleanup})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Stamp lineage and route every trip to exactly one outcome
# MAGIC
# MAGIC **What:** Read the Bronze trips table, stamp the execution ID and source file, then split the rows into valid, quarantine and duplicate sets.
# MAGIC
# MAGIC **Why:** A row that fails a rule must be recorded rather than dropped, or the counts stop reconciling and a quality problem becomes indistinguishable from a lost row. Duplicates are the archive redelivering the same `ride_id`; the first copy by start time wins so the choice is deterministic rather than decided by whichever partition arrived first.
# MAGIC
# MAGIC **Input:** The Bronze trips table and the configured duration bounds.
# MAGIC
# MAGIC **Output:** Three DataFrames whose row counts must sum to the Bronze count.
# MAGIC
# MAGIC **Key concepts:** Quarantine routing, deterministic deduplication, lineage columns, trip-duration quality rules.
# MAGIC
# MAGIC **Expected result:** Sub-minute trips are flagged `TRIP_TOO_SHORT` as false starts and trips over a day `TRIP_TOO_LONG` as unreturned bikes. Both are quarantined, not deleted.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every landed ride ends up in exactly one place, and we can say which rule sent it there."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are lazy transformations; the counts in the next steps are what trigger the actual work.

# COMMAND ----------

bronze_trips = spark.table(bronze_trips_table)
stamped = with_trip_lineage(bronze_trips, execution_id=execution_id)
split = split_trips_and_quarantine(stamped, min_trip_seconds=60, max_trip_hours=24)
landed_rows = bronze_trips.count()
valid_rows = split["valid"].count()
quarantine_rows = split["quarantine"].count()
duplicate_rows = split["duplicates"].count()
print(
    {
        "landed_rows": landed_rows,
        "valid_rows": valid_rows,
        "quarantine_rows": quarantine_rows,
        "duplicate_rows": duplicate_rows,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Join trips to current stations on short_name, and measure the match rate
# MAGIC
# MAGIC **What:** Build the station dimension from the reference feed, join trips to it on `short_name`, and report how many trips found a current station.
# MAGIC
# MAGIC **Why:** This is the join the whole notebook is organised around. `start_station_id` holds a value like `7407.13`, which is the GBFS `short_name` and not the UUID `station_id`. The join is a LEFT join so a renamed or retired station keeps its trips instead of losing them, and the match rate is reported so a wrong key cannot pass unnoticed.
# MAGIC
# MAGIC **Input:** The valid trips and the station reference feed.
# MAGIC
# MAGIC **Output:** Joined trips with `station_matched`, plus the match-rate report.
# MAGIC
# MAGIC **Key concepts:** Business keys versus surrogate keys, left joins preserving facts, measuring a join instead of trusting it.
# MAGIC
# MAGIC **Expected result:** A match rate below 1.0 is normal and expected, because stations really are renamed and retired. A match rate of exactly 0.0 means the key is wrong, and the reconciliation step fails the run.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Old trips name stations the way the archive did. We translate that to today's station IDs and report how many we could translate."
# MAGIC
# MAGIC **Rerun and cost considerations:** The reference feed is a small committed sample, so the join is broadcast-sized and cheap.

# COMMAND ----------

station_reference = (
    spark.read.format("json")
    .option("multiLine", "true")
    .load(f"{volume_root}/landing/station_information")
    .selectExpr("explode(data.stations) AS station")
    .select("station.*")
)
dimension = station_dimension(station_reference)
joined = join_trips_to_stations(split["valid"], dimension)
match_rate = trip_join_match_rate(joined)
print({"match_rate": match_rate})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Build real historical demand, the rider mix and a duration profile
# MAGIC
# MAGIC **What:** Aggregate trips per station per day, summarise member versus casual riders, and profile trip durations.
# MAGIC
# MAGIC **Why:** This is the one part of UrbanFlow that genuinely supports a trend, because a monthly archive contains weeks of real rides. The duration profile is also the evidence for the quality rules: it shows the sub-minute and over-a-day tails that the quarantine removed from the headline figures.
# MAGIC
# MAGIC **Input:** The joined valid trips and this run's execution ID.
# MAGIC
# MAGIC **Output:** The daily demand frame plus two printed summaries.
# MAGIC
# MAGIC **Key concepts:** Aggregation grain, member versus casual segmentation, distribution tails as quality evidence, trend versus snapshot.
# MAGIC
# MAGIC **Expected result:** One demand row per station per day. The duration profile's minimum and maximum should sit inside the configured bounds, because the rows outside them were quarantined.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "This is the real history. The availability tables are one photograph; this is weeks of rides."
# MAGIC
# MAGIC **Rerun and cost considerations:** Aggregations over one monthly archive are bounded and run on the already-attached cluster.

# COMMAND ----------

demand = daily_trip_demand(joined, execution_id=execution_id)
demand_rows = demand.count()
rider_mix = rider_mix_summary(joined)
duration = trip_duration_profile(split["valid"])
print({"demand_rows": demand_rows, "duration_profile": duration})
display(rider_mix.orderBy("trips", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Reconcile before writing anything
# MAGIC
# MAGIC **What:** Check that valid plus quarantine plus duplicates equals the landed row count, and that the join did not collapse to zero matches.
# MAGIC
# MAGIC **Why:** A successful Job is not evidence of correct data. The reconciliation identity is what distinguishes a rejected row from a lost one, and the zero-match check catches the wrong-join-key failure before it reaches a table. Running this before the write means a failure costs nothing.
# MAGIC
# MAGIC **Input:** The four row counts and the match-rate report.
# MAGIC
# MAGIC **Output:** A reconciliation report, and a raised error on `FAIL`.
# MAGIC
# MAGIC **Key concepts:** Reconciliation identities, quality gates before persistence, distinguishing a finding from a bug.
# MAGIC
# MAGIC **Expected result:** `PASS`, with `accounted_rows` equal to `landed_rows` and `join_produced_no_matches` false.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We prove every ride is accounted for before we store anything."
# MAGIC
# MAGIC **Rerun and cost considerations:** The counts are already computed, so this step adds no new scan.

# COMMAND ----------

reconciliation = reconcile_historical(
    landed_rows=landed_rows,
    valid_rows=valid_rows,
    quarantine_rows=quarantine_rows,
    duplicate_rows=duplicate_rows,
    match_rate=match_rate,
    demand_rows=demand_rows,
)
print({"reconciliation": reconciliation})
if reconciliation["status"] != "PASS":
    raise RuntimeError(f"Historical reconciliation failed: {reconciliation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Persist the historical tables idempotently
# MAGIC
# MAGIC **What:** MERGE the trip, quarantine and duplicate tables by their keys, and replace this execution's scope in the daily demand aggregate.
# MAGIC
# MAGIC **Why:** `ride_id` is the archive's own unique key, so the trip tables MERGE on it and a rerun updates rather than duplicates. Daily demand is different: it is a derived aggregate whose row set legitimately shrinks when a trip moves to quarantine, and UPDATE with INSERT alone can never remove a row. So it uses the same execution-scoped atomic replacement the Gold shortage list uses.
# MAGIC
# MAGIC **Input:** The three split frames, the demand frame and the explicit table map.
# MAGIC
# MAGIC **Output:** Four managed Delta tables and per-table before and after counts.
# MAGIC
# MAGIC **Key concepts:** Idempotent MERGE, execution-scoped replacement for derived aggregates, add-only schema migration.
# MAGIC
# MAGIC **Expected result:** A second identical run reports `inserted_rows = 0` everywhere and `stale_rows_removed = 0` for demand.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Running it twice gives the same tables, and a ride that stops qualifying actually disappears from the summary."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are bounded MERGE statements on the attached cluster; no serverless or Lakeflow compute is used.

# COMMAND ----------

merge_report = persist_historical_outputs(
    spark,
    valid=split["valid"],
    quarantine=split["quarantine"],
    duplicates=split["duplicates"],
    daily_demand=demand,
    table_names=historical_tables,
    execution_id=execution_id,
)
print({"delta_merges": merge_report})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Store the evidence, including what this run does not prove
# MAGIC
# MAGIC **What:** Write a JSON report with the reconciliation, the match rate, the duration profile, the MERGE counts and the stated limitations.
# MAGIC
# MAGIC **Why:** A claim without stored evidence is not a validated requirement. The report also records the limitations explicitly, so a later reader does not have to infer that the station dimension is a 40-row sample or that an unmatched trip is expected rather than broken.
# MAGIC
# MAGIC **Input:** The reports gathered above and the execution-specific Volume path.
# MAGIC
# MAGIC **Output:** `<execution_id>.historical.json` containing counts only, never trip rows.
# MAGIC
# MAGIC **Key concepts:** Evidence, stated limitations, data minimisation in reports.
# MAGIC
# MAGIC **Expected result:** Status `PASS` and a report path under the Volume.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The report says what we proved and, just as importantly, what we did not."
# MAGIC
# MAGIC **Rerun and cost considerations:** The report is small and is overwritten deterministically for the same execution ID.

# COMMAND ----------

evidence = {
    "phase": "historical_trips",
    "execution_id": execution_id,
    "status": reconciliation["status"],
    "reconciliation": reconciliation,
    "match_rate": match_rate,
    "duration_profile": duration,
    "delta_merges": merge_report,
    "landed_files": [entry.name for entry in landed],
    "limitations": [
        "the station dimension is a committed 40-row sample, so unmatched trips are expected",
        "a match rate below 1.0 reflects renamed and retired stations, not a defect",
        "trip demand IS real history; the availability tables remain one snapshot",
    ],
}
write_json_report(evidence, report_path)
print({"status": evidence["status"], "report_path": report_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Auto Loader gives bounded, incremental file ingestion with an explicit contract: the schema location remembers what the files looked like, the checkpoint remembers which files were processed, and `availableNow` means the run ends. Typing station identifiers as strings is what preserves the join key, and joining on `short_name` rather than the UUID is what makes the join find anything at all. Every landed ride leaves with exactly one outcome, so a quality finding can never be confused with a lost row. This is also the only part of UrbanFlow that may be charted over time.
# MAGIC
# MAGIC **Common errors:** Inferring the schema and getting `start_station_id` as a double, which silently destroys values like `7407.13`. Joining trips to `station_id` and getting zero rows. Sharing one directory between the Auto Loader schema location and the streaming checkpoint. Pointing Auto Loader at a directory holding a ZIP rather than expanded CSV files. Treating a match rate below 1.0 as a bug.
# MAGIC
# MAGIC **Troubleshooting:** An empty result with no error usually means the landing directory held no CSV, which Step 4 now catches. A zero match rate means the join key is wrong, and Step 9 fails the run. A `_rescued_data` column with values means the archive added a field, which is information rather than an error, and the next run can widen the schema deliberately. If the stream does not finish inside the timeout, check the file count first; `availableNow` processes everything present in one pass.
# MAGIC
# MAGIC **Review questions:** Why must `start_station_id` be a string? What is the difference between the Auto Loader schema location and the streaming checkpoint? Why is the trips-to-stations join a LEFT join? Why is daily demand written with an execution-scoped replacement while the trips table uses a plain MERGE? Why is a match rate of 0.6 acceptable when 0.0 is a failure?
# MAGIC
# MAGIC **Presentation summary:** "The station snapshot tells us what is happening right now. The trip archive tells us what normally happens. We load the archive once, prove every ride is accounted for, translate old station names to current station IDs, and report how much of that translation succeeded instead of assuming it did."

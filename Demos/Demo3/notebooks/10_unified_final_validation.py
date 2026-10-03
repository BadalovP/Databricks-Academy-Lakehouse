# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Unified Final Validation
# MAGIC
# MAGIC **Business context:** A successful task graph is useful only when its persisted station, historical and weather outputs still satisfy their reconciliation contracts.
# MAGIC
# MAGIC **Prerequisites:** Enabled processing branches completed successfully in the same unified Job run.
# MAGIC
# MAGIC **Learning objectives:** Validate execution-scoped counts, uniqueness, null safety, status composition and join fanout without changing data.
# MAGIC
# MAGIC **Academy labs:** Labs 4, 5, 7, 8 and 9.
# MAGIC
# MAGIC **Safety:** Every operation in this notebook is a read-only `SELECT` or aggregate. It creates no tables, views, files, checkpoints or compute.
# MAGIC
# MAGIC **Actual validation:** This unified final-validation notebook has not been executed on Azure; its expected sample metrics come from the independent 2026-10-03 read-only validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 - Import the shared Bronze naming contract
# MAGIC
# MAGIC **What:** Put the project package on the path and import the function that names each namespace's Bronze trips table.
# MAGIC
# MAGIC **Why:** Bronze has no execution column, so each source namespace has its own table. Importing the writer's own naming function means the validator can never read a different table from the one notebook 06 wrote.
# MAGIC
# MAGIC **Input:** The bundle's synced `src` directory.
# MAGIC
# MAGIC **Output:** `historical_bronze_table` available to the historical checks.
# MAGIC
# MAGIC **Key concepts:** Single source of truth, physical namespace isolation.
# MAGIC
# MAGIC **Expected result:** The import succeeds; nothing is read.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The checker asks the loader where it put the data instead of guessing."
# MAGIC
# MAGIC **Rerun and cost considerations:** A path change and an import; free.

# COMMAND ----------

import sys
from pathlib import Path

try:
    NOTEBOOK_DIR = Path(__file__).resolve().parent
except NameError:
    NOTEBOOK_DIR = Path.cwd()
sys.path.insert(0, str(NOTEBOOK_DIR.parent / "src"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 - Read validation scope and define assertion helpers
# MAGIC
# MAGIC **What:** Parse the enabled branches, lineage IDs, weather window and Unity Catalog target, then define small helpers for measured assertions.
# MAGIC
# MAGIC **Why:** Every validation must be tied to one execution so development and monthly data cannot be summed together.
# MAGIC
# MAGIC **Input:** Unified Job parameters.
# MAGIC
# MAGIC **Output:** Validated scope values plus empty metrics and failure collections.
# MAGIC
# MAGIC **Key concepts:** Execution isolation, reusable assertions, read-only verification.
# MAGIC
# MAGIC **Expected result:** All identifiers are non-empty and target names are plain identifiers.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The validator uses the same lineage parameters as the writers, so it cannot accidentally certify a different run."
# MAGIC
# MAGIC **Rerun and cost considerations:** Parameter parsing is free and deterministic.
# COMMAND ----------
import json

from pyspark.sql import functions as F

from urbanflow.historical import historical_bronze_table  # noqa: E402

boolean_names = ("run_station_pipeline", "run_historical", "run_weather", "run_full_month")
text_names = (
    "source_execution_id",
    "historical_execution_id",
    "historical_landing_subdir",
    "weather_execution_id",
    "weather_start_date",
    "weather_end_date",
    "catalog",
    "schema",
)
for name in boolean_names + text_names:
    dbutils.widgets.text(name, "")
flags = {name: dbutils.widgets.get(name).strip().lower() == "true" for name in boolean_names}
values = {name: dbutils.widgets.get(name).strip() for name in text_names}
for name in ("catalog", "schema"):
    if not values[name].isidentifier():
        raise ValueError(f"{name} {values[name]!r} is not a plain identifier.")
root = f"{values['catalog']}.{values['schema']}"
failures = []
metrics = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b - Define execution-scoped validation helpers
# MAGIC
# MAGIC **What:** Define one assertion recorder and one table reader that always filters by execution ID.
# MAGIC
# MAGIC **Why:** Central helpers keep every later check consistent and collect all failures for one actionable error.
# MAGIC
# MAGIC **Input:** The validated target root and each branch's lineage ID.
# MAGIC
# MAGIC **Output:** Reusable `require` and `scoped` functions; no Spark action yet.
# MAGIC
# MAGIC **Key concepts:** Consistent assertions, execution-scoped reads, complete diagnostics.
# MAGIC
# MAGIC **Expected result:** Missing tables fail immediately, while measured contract failures accumulate for the terminal report.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Every table check goes through the same lineage filter and evidence recorder."
# MAGIC
# MAGIC **Rerun and cost considerations:** Defining functions is free and has no data side effect.

# COMMAND ----------


def require(label, condition, observed):
    metrics[label] = observed
    if not condition:
        failures.append({"check": label, "observed": observed})


def scoped(name, execution_id):
    table = f"{root}.{name}"
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Required table {table} does not exist.")
    return spark.table(table).where(F.col("execution_id") == F.lit(execution_id))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Reconcile the station medallion path
# MAGIC
# MAGIC **What:** Compare Bronze, Silver, fact, shortage and priority counts and test each execution-scoped business key.
# MAGIC
# MAGIC **Why:** MERGE can hide duplicates or stale derived rows unless counts and keys are checked together.
# MAGIC
# MAGIC **Input:** The selected station execution across five persisted tables.
# MAGIC
# MAGIC **Output:** Exact sample counts and uniqueness results in the shared validation report.
# MAGIC
# MAGIC **Key concepts:** Medallion reconciliation, idempotency, execution-scoped derived data.
# MAGIC
# MAGIC **Expected result:** 2,520 Bronze/Silver/fact rows and 657 shortage/priority rows, with unique business keys.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The same 2,520 events survive to the fact table, while exactly 657 actionable stations enter both operational lists."
# MAGIC
# MAGIC **Rerun and cost considerations:** Aggregates are bounded to one station execution and do not write evidence files.

# COMMAND ----------

if flags["run_station_pipeline"]:
    station_id = values["source_execution_id"]
    station_frames = {
        "bronze": scoped("bronze_station_status", station_id),
        "silver": scoped("silver_station_status", station_id),
        "fact": scoped("fact_station_availability", station_id),
        "shortage": scoped("gold_station_shortage", station_id),
        "priority": scoped("gold_rebalancing_priority", station_id),
    }
    expected_counts = {
        "bronze": 2520,
        "silver": 2520,
        "fact": 2520,
        "shortage": 657,
        "priority": 657,
    }
    station_counts = {name: frame.count() for name, frame in station_frames.items()}
    for name, expected in expected_counts.items():
        require(f"station_{name}_rows", station_counts[name] == expected, station_counts[name])
    for name in ("bronze", "silver", "fact", "shortage"):
        distinct = station_frames[name].select("event_id").distinct().count()
        require(f"station_{name}_business_key_unique", distinct == station_counts[name], distinct)
    priority_distinct = (
        station_frames["priority"].select("station_id", "observed_at").distinct().count()
    )
    require(
        "station_priority_business_key_unique",
        priority_distinct == station_counts["priority"],
        priority_distinct,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Validate station status composition and legacy cleanup
# MAGIC
# MAGIC **What:** Measure Silver status categories, prove actionable Gold excludes out-of-service rows, and find null business or lineage identifiers.
# MAGIC
# MAGIC **Why:** Correct total counts can still hide a wrong operational classification or legacy rows outside every execution scope.
# MAGIC
# MAGIC **Input:** The station frames and all execution-aware station tables.
# MAGIC
# MAGIC **Output:** Exact category counts, zero actionable out-of-service rows and zero null identifiers.
# MAGIC
# MAGIC **Key concepts:** Business-rule composition, null safety, legacy-lineage cleanup.
# MAGIC
# MAGIC **Expected result:** 1,774 available, 278 low bikes, 374 low docks, 5 low both and 89 out of service; Gold contains zero out-of-service actions.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We validate the meaning of the 657 rows, not only the number."
# MAGIC
# MAGIC **Rerun and cost considerations:** Grouped counts and null filters are read-only and execution bounded where possible.

# COMMAND ----------

if flags["run_station_pipeline"]:
    rows = station_frames["silver"].groupBy("availability_status").count().collect()
    composition = {row["availability_status"]: row["count"] for row in rows}
    expected = {
        "AVAILABLE": 1774,
        "LOW_BIKES": 278,
        "LOW_DOCKS": 374,
        "LOW_BIKES_AND_DOCKS": 5,
        "OUT_OF_SERVICE": 89,
    }
    require("station_status_composition", composition == expected, composition)
    for name in ("shortage", "priority"):
        out_of_service = (
            station_frames[name].where(F.col("availability_status") == "OUT_OF_SERVICE").count()
        )
        require(f"station_{name}_out_of_service", out_of_service == 0, out_of_service)
    for name, frame in station_frames.items():
        null_ids = frame.where(F.col("station_id").isNull()).count()
        require(f"station_{name}_null_station_ids", null_ids == 0, null_ids)
    for name in (
        "silver_station_status",
        "fact_station_availability",
        "gold_station_shortage",
        "gold_rebalancing_priority",
    ):
        unassigned = spark.table(f"{root}.{name}").where(F.col("execution_id").isNull()).count()
        require(f"{name}_unassigned_execution_ids", unassigned == 0, unassigned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Reconcile historical trips
# MAGIC
# MAGIC **What:** Reconcile landed Bronze rows with valid, quarantine and duplicate outputs, then measure ride uniqueness, rider mix, days and station-reference matches.
# MAGIC
# MAGIC **Why:** The full archive may have honest reference gaps, while the committed development sample has exact expected counts; both modes still need the same core identities.
# MAGIC
# MAGIC **Input:** One historical execution and the persisted station dimension.
# MAGIC
# MAGIC **Output:** Generic reconciliation for every mode plus strict 40-row sample assertions when `run_full_month=false`.
# MAGIC
# MAGIC **Key concepts:** Quarantine accounting, reference-data measurement, sample versus production assertions.
# MAGIC
# MAGIC **Expected result:** Sample mode is 40 valid, zero quarantine, zero duplicate, 40 unique rides, 35 members, 5 casual riders and 17 days.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The sample has exact expected answers; the monthly mode keeps the identities but measures station drift honestly."
# MAGIC
# MAGIC **Rerun and cost considerations:** All reads are restricted to one historical execution.

# COMMAND ----------

historical_valid_rows = None
if flags["run_historical"]:
    historical_id = values["historical_execution_id"]
    # Bronze has no execution column: one table and one owning execution per namespace.
    bronze_table = historical_bronze_table(root, landing_subdir=values["historical_landing_subdir"])
    historical = {
        name: scoped(table, historical_id)
        for name, table in {
            "valid": "silver_historical_trips",
            "quarantine": "quarantine_historical_trips",
            "duplicate": "duplicate_historical_trips",
        }.items()
    }
    historical["bronze"] = spark.table(bronze_table)
    counts = {name: frame.count() for name, frame in historical.items()}
    historical_valid_rows = counts["valid"]
    require(
        "historical_reconciliation",
        counts["bronze"] == counts["valid"] + counts["quarantine"] + counts["duplicate"],
        counts,
    )
    unique_rides = historical["valid"].select("ride_id").distinct().count()
    require("historical_ride_id_unique", unique_rides == counts["valid"], unique_rides)
    null_rides = historical["valid"].where(F.col("ride_id").isNull()).count()
    require("historical_null_ride_ids", null_rides == 0, null_rides)
    require(
        "historical_station_id_type",
        dict(historical["valid"].dtypes)["start_station_id"] == "string",
        dict(historical["valid"].dtypes)["start_station_id"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4b - Measure historical reference coverage and sample expectations
# MAGIC
# MAGIC **What:** Join valid trips to the persisted station short names, then measure rider mix and calendar coverage.
# MAGIC
# MAGIC **Why:** Reference matching is exact for the committed sample but may reveal honest station drift in a full historical month.
# MAGIC
# MAGIC **Input:** Valid trips, the station dimension and the selected run mode.
# MAGIC
# MAGIC **Output:** Strict sample assertions or measured full-month station coverage.
# MAGIC
# MAGIC **Key concepts:** Reference matching, categorical reconciliation, honest historical drift.
# MAGIC
# MAGIC **Expected result:** Sample mode matches all 40 rides, 35 members, 5 casual riders and 17 dates.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We require the known sample answer and report monthly station drift instead of hiding it."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is a read-only join bounded to one execution and a small dimension.

# COMMAND ----------

if flags["run_historical"]:
    dimension = spark.table(f"{root}.dim_station_development_sample").select("station_short_name")
    match_condition = historical["valid"].start_station_id == dimension.station_short_name
    matched = historical["valid"].join(dimension, match_condition, "inner").count()
    rider_rows = historical["valid"].groupBy("member_casual").count().collect()
    riders = {row["member_casual"]: row["count"] for row in rider_rows}
    days = historical["valid"].select(F.to_date("started_at").alias("day")).distinct().count()
    if not flags["run_full_month"]:
        require(
            "historical_sample_counts",
            counts == {"bronze": 40, "valid": 40, "quarantine": 0, "duplicate": 0},
            counts,
        )
        require("historical_sample_station_matches", matched == 40, matched)
        require("historical_sample_rider_mix", riders == {"member": 35, "casual": 5}, riders)
        require("historical_sample_days", days == 17, days)
    else:
        require(
            "historical_full_month_nonempty", counts["bronze"] > 0 and counts["valid"] > 0, counts
        )
        metrics["historical_full_month_station_matches"] = {
            "matched": matched,
            "valid": counts["valid"],
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Validate weather completeness and join cardinality
# MAGIC
# MAGIC **What:** Check the selected hourly weather window for completeness and duplicates, then compare summed enriched demand with the historical trip count.
# MAGIC
# MAGIC **Why:** Duplicate weather hours multiply rides during a join, while missing readings can make a chart misleading even when row counts still reconcile.
# MAGIC
# MAGIC **Input:** Weather date window, weather execution and the selected historical execution.
# MAGIC
# MAGIC **Output:** Hourly completeness metrics and a no-fanout assertion.
# MAGIC
# MAGIC **Key concepts:** Temporal uniqueness, completeness, join fanout prevention.
# MAGIC
# MAGIC **Expected result:** Sample mode has 48 unique complete hours and its demand summary accounts for all 40 trips without fanout.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Weather can add columns to a ride, but it may never add another copy of the ride."
# MAGIC
# MAGIC **Rerun and cost considerations:** The two-day sample or bounded monthly window is read-only.

# COMMAND ----------

if flags["run_weather"]:
    weather = spark.table(f"{root}.dim_weather_hourly").where(
        F.to_date("weather_hour").between(values["weather_start_date"], values["weather_end_date"])
    )
    weather_rows = weather.count()
    unique_hours = weather.select("weather_grid_label", "weather_hour").distinct().count()
    complete_hours = weather.where(F.col("temperature_celsius").isNotNull()).count()
    require(
        "weather_hour_business_key_unique",
        unique_hours == weather_rows,
        {"rows": weather_rows, "unique": unique_hours},
    )
    require(
        "weather_temperature_complete",
        complete_hours == weather_rows and weather_rows > 0,
        {"rows": weather_rows, "complete": complete_hours},
    )
    if not flags["run_full_month"]:
        require("weather_sample_hours", weather_rows == 48, weather_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5b - Prove weather enrichment did not multiply trips
# MAGIC
# MAGIC **What:** Sum the execution-scoped demand summary and compare it with the selected valid historical trip count.
# MAGIC
# MAGIC **Why:** Unique weather hours protect the dimension, while this second identity protects the actual enriched output.
# MAGIC
# MAGIC **Input:** One weather summary execution and the historical valid-row metric.
# MAGIC
# MAGIC **Output:** A no-fanout assertion and a whole-table null-lineage check.
# MAGIC
# MAGIC **Key concepts:** Join cardinality, aggregate reconciliation, lineage completeness.
# MAGIC
# MAGIC **Expected result:** The summary accounts for every selected trip exactly once and has no unassigned execution rows.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The weather result must add up to the trip input, so a duplicated hour cannot inflate demand."
# MAGIC
# MAGIC **Rerun and cost considerations:** These are read-only aggregates over the selected execution.

# COMMAND ----------

if flags["run_weather"]:
    summary = scoped("gold_weather_demand", values["weather_execution_id"])
    summary_trips = summary.agg(F.coalesce(F.sum("trips"), F.lit(0)).alias("trips")).first()[
        "trips"
    ]
    require(
        "weather_join_no_fanout",
        summary_trips == historical_valid_rows,
        {"summary_trips": summary_trips, "historical_trips": historical_valid_rows},
    )
    unassigned = (
        spark.table(f"{root}.gold_weather_demand").where(F.col("execution_id").isNull()).count()
    )
    require("gold_weather_demand_unassigned_execution_ids", unassigned == 0, unassigned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Return one terminal validation result
# MAGIC
# MAGIC **What:** Fail the task with every observed contract violation, or return a compact `PASS` report for the release workflow.
# MAGIC
# MAGIC **Why:** The workflow must not call the release successful merely because notebook processes terminated; the data invariants are the acceptance result.
# MAGIC
# MAGIC **Input:** Collected metrics and failures from all enabled branches.
# MAGIC
# MAGIC **Output:** A JSON notebook result with `PASS`, or a raised error that fails the Job.
# MAGIC
# MAGIC **Key concepts:** Data acceptance gate, actionable diagnostics, machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** No failures and a result naming the three selected execution IDs.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The last task is the release acceptance test for the data, not a cosmetic success message."
# MAGIC
# MAGIC **Rerun and cost considerations:** The report is returned through the Jobs API and is not persisted as a new object.

# COMMAND ----------

if failures:
    raise RuntimeError(f"UrbanFlow final validation failed: {json.dumps(failures, sort_keys=True)}")
report = {
    "status": "PASS",
    "mode": "full_month" if flags["run_full_month"] else "development_sample",
    "source_execution_id": values["source_execution_id"],
    "historical_execution_id": values["historical_execution_id"],
    "weather_execution_id": values["weather_execution_id"],
    "checks": metrics,
}
print({"status": report["status"], "checks": len(metrics), "mode": report["mode"]})
dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Orchestration success and data success are separate facts. A final read-only task makes the release prove both: every task finished, and the persisted business keys, counts, categories and joins still reconcile for the selected lineage.
# MAGIC
# MAGIC **Common errors:** Counting all executions together, checking only totals, treating a low station match rate as automatic corruption, or validating a weather join without checking fanout.
# MAGIC
# MAGIC **Troubleshooting:** Use the failed check name and observed value to inspect only that execution. Do not rerun Event Hubs, overwrite another execution, or weaken a sample assertion to make the Job green.
# MAGIC
# MAGIC **Review questions:** Why are sample counts exact while monthly station matching is measured? How does the no-fanout check detect duplicate weather hours? Why are null execution IDs checked across the whole table?
# MAGIC
# MAGIC **Presentation summary:** "The final task independently reads the outputs and refuses to approve the release unless every selected lineage reconciles."

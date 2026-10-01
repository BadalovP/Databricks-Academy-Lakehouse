# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow - Open-Meteo Weather Enrichment
# MAGIC
# MAGIC **Business context:** Bike-share demand is weather-driven in a way almost nothing else explains: the same station on a mild dry morning and a freezing wet one behaves completely differently. Joining real weather observations to real trip history turns "demand varies" into something an operator can plan around.
# MAGIC
# MAGIC **Prerequisites:** The historical trip tables exist from notebook 06, the UrbanFlow schema and Volume exist, GP1 or GP2 is already `RUNNING`, this run has explicit approval, and the cluster can reach the public Open-Meteo archive API.
# MAGIC
# MAGIC **Learning objectives:** Retrieve real observations from a public API within an explicit bound, normalise timestamps to an hourly grain, join without losing or multiplying facts, keep a missing reading missing, and state the resolution of the data honestly.
# MAGIC
# MAGIC **Academy labs:** Labs 1, 4, 5, 6, 7, and 9.
# MAGIC
# MAGIC **Safety:** `run_enrichment` defaults to `false`, so the committed default exits before any call or write. The request window is bounded and refused above 62 days. The notebook never starts, stops or resizes a cluster.
# MAGIC
# MAGIC **Actual validation:** This notebook has not been run in Azure. The parser and the join are covered by tests against a real committed Open-Meteo archive response, but no figure below has been observed on the cluster.
# MAGIC
# MAGIC **Two limitations that must be read before any chart from this is shown:** the weather series is ONE COORDINATE for New York City, not per-station weather, and every row carries `weather_grid_label` so that resolution stays visible. And these are COMPARISONS, not predictions: grouping trips by temperature bucket shows that cold wet days have fewer rides, but it models nothing and controls for nothing, not day of week, not holidays, not closures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the tested weather helpers
# MAGIC
# MAGIC **What:** Put the synchronized project package on the path and import the request, parsing, enrichment and persistence functions.
# MAGIC
# MAGIC **Why:** The parsing is the fragile part of any API integration, because the response is column-oriented parallel arrays rather than a list of objects. That logic belongs in a tested module, not in a notebook cell.
# MAGIC
# MAGIC **Input:** The bundle-synchronized `src/urbanflow` modules.
# MAGIC
# MAGIC **Output:** Imported functions only; no Spark action and no network call occurs.
# MAGIC
# MAGIC **Key concepts:** Modular design, tested API parsing, separation of retrieval from transformation.
# MAGIC
# MAGIC **Expected result:** Imports complete without contacting the API.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The risky parsing has unit tests against a real saved response; the notebook just calls it."
# MAGIC
# MAGIC **Rerun and cost considerations:** Imports are free and change nothing.

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

from urbanflow.automation import APPROVED_RUN_CLUSTER_IDS
from urbanflow.reporting import write_json_report
from urbanflow.weather import (
    WeatherRequest,
    build_archive_url,
    enrich_trips_with_weather,
    fetch_hourly_weather,
    parse_hourly_payload,
    persist_weather_outputs,
    reconcile_weather_join,
    weather_coverage,
    weather_demand_summary,
    weather_frame,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and fail closed before any call or Spark action
# MAGIC
# MAGIC **What:** Declare the approval gate, the execution ID, the coordinate, the date window and the bundle target identifiers, then verify the attached cluster.
# MAGIC
# MAGIC **Why:** A default Job run must neither call the public API nor write a table. Open-Meteo is free, but an unbounded repeated request against a free public service is still rude, and the gate makes an accidental run do nothing at all.
# MAGIC
# MAGIC **Input:** Job parameters supplied by the deployed bundle target.
# MAGIC
# MAGIC **Output:** Validated values, or an immediate `DRY_RUN` exit.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, existing-cluster allowlist, bounded external requests, courtesy toward public APIs.
# MAGIC
# MAGIC **Expected result:** The committed default exits with `DRY_RUN`; an approved run continues only on GP1 or GP2.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Nothing is requested and nothing is written until someone sets one parameter to true."
# MAGIC
# MAGIC **Rerun and cost considerations:** The default path makes no network call and no Spark action, so it is free.

# COMMAND ----------

dbutils.widgets.dropdown("run_enrichment", "false", ["false", "true"])
dbutils.widgets.dropdown("weather_source", "sample_json", ["sample_json", "archive_api"])
dbutils.widgets.text("weather_sample_file", "open_meteo_archive_202401.sample.json")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("end_date", "2024-01-31")
dbutils.widgets.text("latitude", "40.7128")
dbutils.widgets.text("longitude", "-74.0060")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
run_enrichment = dbutils.widgets.get("run_enrichment").lower() == "true"
execution_id = dbutils.widgets.get("execution_id").strip()
if not run_enrichment:
    dbutils.notebook.exit("DRY_RUN: no weather was requested and no Delta table was written.")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")
if cluster_id not in APPROVED_RUN_CLUSTER_IDS:
    raise RuntimeError(f"Cluster {cluster_id!r} is not approved for UrbanFlow.")
if not execution_id:
    raise ValueError("execution_id must identify this weather enrichment run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Build the bounded request and resolve isolated targets
# MAGIC
# MAGIC **What:** Construct the `WeatherRequest`, which validates the coordinate and the date window, then resolve the table names and the report path.
# MAGIC
# MAGIC **Why:** Validation happens in the request object's constructor, so an invalid coordinate or a reversed date range fails before any call is made rather than producing an empty response that looks like missing data. Printing the exact URL means a run log records precisely what was asked for.
# MAGIC
# MAGIC **Input:** The widget values and the bundle target identifiers.
# MAGIC
# MAGIC **Output:** A validated request, the rendered URL, and fully qualified table names.
# MAGIC
# MAGIC **Key concepts:** Validation at construction, auditable external requests, Unity Catalog three-level names.
# MAGIC
# MAGIC **Expected result:** A 31-day January window is accepted; anything above 62 days is refused in the next step.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The log shows the exact request we made, so the numbers can be checked against the source."
# MAGIC
# MAGIC **Rerun and cost considerations:** Building the request costs nothing and makes no call.

# COMMAND ----------

target_catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
target_volume = dbutils.widgets.get("volume").strip()
for label, value in (
    ("catalog", target_catalog),
    ("schema", target_schema),
    ("volume", target_volume),
):
    if not value.isidentifier():
        raise ValueError(f"{label} {value!r} is not a plain identifier.")
request = WeatherRequest(
    latitude=float(dbutils.widgets.get("latitude")),
    longitude=float(dbutils.widgets.get("longitude")),
    start_date=dbutils.widgets.get("start_date").strip(),
    end_date=dbutils.widgets.get("end_date").strip(),
)
table_root = f"{target_catalog}.{target_schema}"
volume_root = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}"
weather_tables = {
    "weather": f"{table_root}.dim_weather_hourly",
    "demand_summary": f"{table_root}.gold_weather_demand",
}
print({"url": build_archive_url(request), "grid": request.grid_label})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Retrieve real hourly observations, within an explicit bound
# MAGIC
# MAGIC **What:** Obtain hourly observations and parse them into one record per hour, either from the committed 48-hour archive sample landed in the Volume (`weather_source=sample_json`, the default) or from one live Open-Meteo archive call (`archive_api`).
# MAGIC
# MAGIC **Why:** The API returns parallel arrays, so a silently short array would shift every later reading onto the wrong hour and produce a plausible-looking but wrong chart. The parser checks every array's length against the `time` array and refuses to align them positionally if they disagree. Nothing here generates, interpolates or back-fills a reading.
# MAGIC
# MAGIC **Input:** The validated request, plus the landed sample file when the sample source is selected.
# MAGIC
# MAGIC **Output:** One record per hour, with nulls preserved as nulls.
# MAGIC
# MAGIC **Key concepts:** Column-oriented API responses, positional alignment as a failure mode, bounded external requests, never fabricating data.
# MAGIC
# MAGIC **Expected result:** 24 records per requested day, so 744 for a 31-day January window.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "These are real published observations. Where the source has no reading, we keep the gap rather than filling it in."
# MAGIC
# MAGIC **Rerun and cost considerations:** The default sample source makes NO external request at all, so a rerun costs only cluster time and cannot burden a public API. The `archive_api` source makes one HTTPS GET, with a 62-day ceiling so a typo in a year cannot become a very large request. Both paths run through the same parser, so choosing the sample does not bypass the validation.

# COMMAND ----------

weather_source = dbutils.widgets.get("weather_source").strip()
if weather_source == "sample_json":
    # The committed 48-hour archive response, landed in the Volume. Parsed by the SAME
    # parse_hourly_payload the API path uses, so the validation covers the real code path
    # while making no external request from the cluster.
    sample_path = (
        f"{volume_root}/landing/weather/" + dbutils.widgets.get("weather_sample_file").strip()
    )
    with open(sample_path, encoding="utf-8") as handle:
        records = parse_hourly_payload(json.load(handle), request)
else:
    records = fetch_hourly_weather(request, timeout_seconds=30.0, max_days=62)
coverage = weather_coverage(records)
print({"source": weather_source, "hours": coverage["hours"], "coverage": coverage})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Judge the coverage before trusting anything joined to it
# MAGIC
# MAGIC **What:** Report how many hours carry each measurement and refuse to continue if the series is empty.
# MAGIC
# MAGIC **Why:** A join against a half-empty weather series produces charts that look exactly as convincing as complete ones. Counting the gaps up front, and carrying that count into the evidence report, is what lets a reader discount the figures appropriately. An entirely empty series is a genuine failure and stops the run.
# MAGIC
# MAGIC **Input:** The coverage report from the retrieved records.
# MAGIC
# MAGIC **Output:** A printed summary, or a raised error on an empty series.
# MAGIC
# MAGIC **Key concepts:** Completeness as a reported dimension, distinguishing a gap from a failure, nulls as real answers.
# MAGIC
# MAGIC **Expected result:** `complete` is true for a historical January window, because the archive is settled. Recent dates can legitimately be incomplete.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We say how much weather we actually got before we show any chart that depends on it."
# MAGIC
# MAGIC **Rerun and cost considerations:** This is arithmetic over a list already in memory and costs nothing.

# COMMAND ----------

if coverage["hours"] == 0:
    raise RuntimeError(
        f"Open-Meteo returned no hours for {request.start_date}..{request.end_date}."
    )
if not coverage["complete"]:
    print(
        {
            "note": "weather series has gaps; they stay null and are reported, never zero-filled",
            "missing_temperature_hours": coverage["missing_temperature"],
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Type the series and join it to real trips on the start hour
# MAGIC
# MAGIC **What:** Build the typed weather frame, then left-join trips to it on the truncated trip start hour.
# MAGIC
# MAGIC **Why:** A trip starting at 09:47 belongs to the 09:00 observation. That is a documented rounding decision made in one place, not an interpolation. The join is LEFT so an uncovered hour never deletes a real trip: losing rides to a weather gap would corrupt the demand figures in order to flatter the weather figures.
# MAGIC
# MAGIC **Input:** The retrieved records and the Silver historical trips table.
# MAGIC
# MAGIC **Output:** The weather frame and the enriched trips, with `has_weather` marking coverage.
# MAGIC
# MAGIC **Key concepts:** Hourly grain, documented rounding, left joins preserving facts, measuring coverage rather than assuming it.
# MAGIC
# MAGIC **Expected result:** Every trip survives the join, and trips outside the requested window have null weather and `has_weather` false.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Each ride is matched to the weather for the hour it started. Rides we have no weather for are kept and labelled."
# MAGIC
# MAGIC **Rerun and cost considerations:** The weather series is at most a few hundred rows, so the join is broadcast-sized.

# COMMAND ----------

weather = weather_frame(spark, records)
trips = spark.table(f"{table_root}.silver_historical_trips")
enriched = enrich_trips_with_weather(trips, weather)
trip_rows = trips.count()
enriched_rows = enriched.count()
matched_rows = enriched.where("has_weather").count()
print({"trip_rows": trip_rows, "enriched_rows": enriched_rows, "matched_rows": matched_rows})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Prove the join neither dropped nor multiplied a trip
# MAGIC
# MAGIC **What:** Check that the enriched row count equals the trip row count, and fail the run if it does not.
# MAGIC
# MAGIC **Why:** This is the specific failure a left join can hide. If the weather series contained a duplicate hour, every trip in that hour would be MULTIPLIED, inflating demand while each individual number still looked reasonable. Comparing counts catches it. A coverage shortfall, by contrast, is reported but is not a failure, because an uncovered hour is a real fact about the archive window.
# MAGIC
# MAGIC **Input:** The three row counts from the previous step.
# MAGIC
# MAGIC **Output:** A reconciliation report, and a raised error on `FAIL`.
# MAGIC
# MAGIC **Key concepts:** Join fan-out as a silent corruption, reconciliation before persistence, separating a gap from a defect.
# MAGIC
# MAGIC **Expected result:** `PASS` with `rows_preserved` true, and `weather_coverage` at or near 1.0 for a window covering the archive month.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "We check we still have exactly the same number of rides after adding weather. More rides would mean we had double-counted."
# MAGIC
# MAGIC **Rerun and cost considerations:** The counts are already computed, so this adds no new scan.

# COMMAND ----------

reconciliation = reconcile_weather_join(enriched_rows, trip_rows, matched_rows)
print({"reconciliation": reconciliation})
if reconciliation["status"] != "PASS":
    raise RuntimeError(f"Weather join changed the trip row count: {reconciliation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Summarise demand against weather and persist both tables
# MAGIC
# MAGIC **What:** Build the daily demand-versus-weather comparison, then MERGE the hourly dimension and replace this execution's scope in the summary.
# MAGIC
# MAGIC **Why:** Temperature is averaged and precipitation summed, because a day has an average temperature but a total rainfall. The dimension is append-or-update, since an hour's observed temperature does not stop existing, so a MERGE is right. The summary is a derived aggregate whose rows legitimately disappear when a day's trips move to quarantine, and UPDATE with INSERT can never remove a row, so it uses the execution-scoped replacement.
# MAGIC
# MAGIC **Input:** The enriched trips, the weather frame and the explicit table map.
# MAGIC
# MAGIC **Output:** Two managed Delta tables with before and after counts.
# MAGIC
# MAGIC **Key concepts:** Aggregation choices that match how a measure is read, idempotent MERGE, execution-scoped replacement of derived aggregates.
# MAGIC
# MAGIC **Expected result:** A second identical run inserts zero rows and removes zero stale rows. Trips with no weather appear in the `UNKNOWN` temperature bucket rather than being dropped or counted as cold.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "Running it again gives the same two tables, and a day that stops having rides loses its summary row instead of lingering."
# MAGIC
# MAGIC **Rerun and cost considerations:** Two bounded MERGE statements on the attached cluster; no serverless or Lakeflow compute.

# COMMAND ----------

summary = weather_demand_summary(enriched, execution_id=execution_id)
merge_report = persist_weather_outputs(
    spark,
    weather=weather,
    demand_summary=summary,
    table_names=weather_tables,
    execution_id=execution_id,
)
print({"delta_merges": merge_report})
display(summary.orderBy("trip_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Store the evidence, including the resolution and the gaps
# MAGIC
# MAGIC **What:** Write a JSON report with the request, the coverage, the reconciliation, the MERGE counts and the stated limitations.
# MAGIC
# MAGIC **Why:** The two limitations of this dataset are easy to forget and damaging to forget: it is one coordinate, not per-station weather, and it supports comparisons rather than predictions. Writing them into the evidence file means a later reader does not have to rediscover them, and the exact request URL makes every figure traceable to its source.
# MAGIC
# MAGIC **Input:** The reports gathered above and the execution-specific Volume path.
# MAGIC
# MAGIC **Output:** `<execution_id>.weather.json` containing counts and metadata only.
# MAGIC
# MAGIC **Key concepts:** Provenance metadata, stated limitations, reproducible external requests.
# MAGIC
# MAGIC **Expected result:** Status `PASS`, with the grid label and the retrieval timestamp recorded.
# MAGIC
# MAGIC **How to explain it to my supervisor:** "The report records where the weather came from, how complete it was, and what it cannot be used to claim."
# MAGIC
# MAGIC **Rerun and cost considerations:** The report is small and is overwritten deterministically for the same execution ID.

# COMMAND ----------

evidence = {
    "phase": "weather_enrichment",
    "execution_id": execution_id,
    "status": reconciliation["status"],
    "request": {
        "source": weather_source,
        "url": build_archive_url(request),
        "grid_label": request.grid_label,
    },
    "coverage": coverage,
    "reconciliation": reconciliation,
    "delta_merges": merge_report,
    "limitations": [
        "one city coordinate, NOT per-station weather; see weather_grid_label",
        "comparisons only, not predictions; nothing is modelled or controlled for",
        "a missing reading stays null and is never replaced with zero",
    ],
}
write_json_report(evidence, f"{volume_root}/reports/weather/{execution_id}.weather.json")
print({"status": evidence["status"]})

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Integrating a public API is mostly about distrusting its shape. Open-Meteo returns parallel arrays, so the parser checks every array against the `time` array rather than zipping them hopefully, because a short array would shift readings onto the wrong hours and the resulting chart would look fine. Joining weather to trips is a left join measured by a row-count identity, because the dangerous outcome is not a missing row but a multiplied one. And a null reading stays null: zero degrees and "unknown" are different facts, and substituting one for the other invents a cold day that never happened.
# MAGIC
# MAGIC **Common errors:** Zero-filling a missing reading and creating a freezing hour from nothing. Joining on an unrounded timestamp so almost nothing matches. Letting a duplicated weather hour multiply trips and reading the inflated demand as growth. Presenting one city coordinate as station-level weather. Describing a grouped average as a prediction.
# MAGIC
# MAGIC **Troubleshooting:** If almost no trip matches, check that the date window actually covers the archive month. If the enriched count exceeds the trip count, the weather series has duplicate hours and Step 7 fails the run. If `complete` is false for a historical month, the request window probably extends past the settled archive into recent dates. If the request is refused outright, the window is above the 62-day ceiling, which is deliberate.
# MAGIC
# MAGIC **Review questions:** Why is a trip at 09:47 joined to the 09:00 observation, and where is that decision made? Why is a null temperature not a zero? What would a duplicated weather hour do to the demand figures, and which check catches it? Why is the weather dimension MERGEd while the demand summary is execution-scoped? Why can this data support a comparison but not a forecast?
# MAGIC
# MAGIC **Presentation summary:** "We pull the real published weather for the month we have trip data for, attach it to each ride by the hour the ride started, and prove we still have exactly the same number of rides afterwards. That lets us show how demand changes with temperature and rain. It is one weather station for the city, so it is a city-level comparison, not a per-station forecast."

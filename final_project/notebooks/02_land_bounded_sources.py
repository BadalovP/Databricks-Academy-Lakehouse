# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Land bounded sources for one platform run
# MAGIC
# MAGIC **Business context:** A brand-new environment has no Bronze data. Before Silver and Gold can run, this task lands the committed Citi Bike GBFS snapshot and January 2024 trip sample into the target's own Volume, then ingests the station events with Auto Loader. In Azure the station events can instead come from Event Hubs (`station_source=existing_bronze`).
# MAGIC
# MAGIC **Prerequisites:** The bundle created the target schema and Volume; the task runs on serverless compute; preflight accepted the run parameters.
# MAGIC
# MAGIC **Learning objectives:** Land files idempotently in a Unity Catalog Volume, ingest them with Auto Loader in `availableNow` mode, and keep a per-execution checkpoint.
# MAGIC
# MAGIC **Academy labs:** Lab 1 (files, DataFrames), Lab 2 (Unity Catalog Volumes), Lab 3 (Auto Loader, checkpoints, bounded streaming).
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-12 (Final Project run 896549012431425, task `land_sources`): 40 station events and the trip sample already landed and reused without duplication, and the 48-hour weather sample landed at the path notebook 07 reads. The first run (304828008837365) produced the 40 Bronze rows; offline tests cover event construction, path safety and Bronze-schema parity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read parameters and decide what to land
# MAGIC
# MAGIC **What:** Read the run switches, lineage IDs and target names, refuse classic compute, and exit early when nothing needs landing.
# MAGIC
# MAGIC **Why:** A task that does nothing by default cannot surprise anyone with writes or cost.
# MAGIC
# MAGIC **Input:** Job parameters passed by the platform Job.
# MAGIC
# MAGIC **Output:** Validated settings and the per-execution landing paths.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, execution-scoped paths, serverless-only compute.
# MAGIC
# MAGIC **Expected result:** A dry run exits with `NOTHING_TO_LAND`; an approved run continues.
# MAGIC
# MAGIC **How to explain it:** "Each run lands its own files under its own execution ID, so runs never overwrite each other."
# MAGIC
# MAGIC **Rerun and cost considerations:** Reading widgets triggers no Spark action.

# COMMAND ----------

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.automation import assert_serverless_compute
from urbanflow.landing import (
    LANDING_PARAMETERS,
    landing_paths,
    landing_plan,
    validate_station_source,
    weather_sample_landing,
)

for name in LANDING_PARAMETERS:
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name).strip() for name in LANDING_PARAMETERS}
land_station, land_trips = landing_plan(values)
volume_root = f"/Volumes/{values['catalog']}/{values['schema']}/{values['volume']}"
weather_target = weather_sample_landing(values, volume_root)
if not (land_station or land_trips or weather_target):
    dbutils.notebook.exit(json.dumps({"status": "NOTHING_TO_LAND"}))
assert_serverless_compute(spark.conf.get)
validate_station_source(values["station_source"], values["source_execution_id"])
paths = landing_paths(
    volume_root,
    source_execution_id=values["source_execution_id"],
    historical_execution_id=values["historical_execution_id"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Land the station snapshot as JSON lines
# MAGIC
# MAGIC **What:** Build one event per station from the committed snapshot with the producer's own event construction, and write them as one JSON-lines file.
# MAGIC
# MAGIC **Why:** Using the same `build_events` as the Event Hubs producer means Bronze cannot tell the two sources apart except by lineage.
# MAGIC
# MAGIC **Input:** `fixtures/samples/station_information.sample.json` and `station_status.sample.json`.
# MAGIC
# MAGIC **Output:** `landing/station_events/<execution_id>/events.jsonl` in the target Volume.
# MAGIC
# MAGIC **Key concepts:** Idempotent landing; the collection time comes from the snapshot itself, so a rerun writes identical bytes.
# MAGIC
# MAGIC **Expected result:** 40 events landed, or the identical file was already present.
# MAGIC
# MAGIC **How to explain it:** "Landing is idempotent: the same execution always produces the same file, and an existing different file stops the run."
# MAGIC
# MAGIC **Rerun and cost considerations:** One small file write; skipped when the identical file exists.

# COMMAND ----------

from urbanflow.landing import sample_station_events, to_json_lines


def land_once(target: Path, content: str) -> str:
    if target.exists():
        if target.read_text(encoding="utf-8") != content:
            raise RuntimeError(
                f"{target} already exists with different content; refusing to overwrite."
            )
        return "already_landed"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return "landed"


report = {"status": "PASS", "source_execution_id": values["source_execution_id"]}
if land_station:
    samples = PROJECT_ROOT / "fixtures" / "samples"
    status = json.loads((samples / "station_status.sample.json").read_text(encoding="utf-8"))
    information = json.loads(
        (samples / "station_information.sample.json").read_text(encoding="utf-8")
    )
    collected = datetime.fromtimestamp(int(status["last_updated"]), UTC)
    events = sample_station_events(
        information, status, execution_id=values["source_execution_id"], collected_at=collected
    )
    station_file = Path(paths.station_events_dir) / "events.jsonl"
    report["station_landing"] = land_once(station_file, to_json_lines(events))
    report["station_events_landed"] = len(events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Ingest the landed events with Auto Loader
# MAGIC
# MAGIC **What:** Read the landing folder with Auto Loader, parse it with the Event Hubs Bronze parser, keep only this execution, and append to `bronze_station_status` in `availableNow` mode.
# MAGIC
# MAGIC **Why:** Auto Loader tracks which files it has processed in its checkpoint, so a rerun ingests nothing twice.
# MAGIC
# MAGIC **Input:** The landed JSON-lines file.
# MAGIC
# MAGIC **Output:** Bronze rows for this execution, with lineage topic `volume:station_events`.
# MAGIC
# MAGIC **Key concepts:** Auto Loader, `availableNow`, per-execution checkpoint, bounded wait.
# MAGIC
# MAGIC **Expected result:** 40 Bronze rows for this execution on the first run, and no new rows on a rerun.
# MAGIC
# MAGIC **How to explain it:** "Auto Loader remembers processed files, so the stream is incremental and safe to rerun."
# MAGIC
# MAGIC **Rerun and cost considerations:** The query stops when the available files are consumed; the wait is bounded by `stream_timeout_seconds`.

# COMMAND ----------

from pyspark.sql import functions as F

from urbanflow.streaming import (
    await_bounded_completion,
    filter_to_execution,
    landed_text_as_kafka_records,
    parse_station_events,
    read_landed_station_events,
    start_bronze_available_now,
)

if land_station:
    bronze_table = f"{values['catalog']}.{values['schema']}.bronze_station_status"
    landed = read_landed_station_events(spark, paths.station_events_dir)
    bronze = filter_to_execution(
        parse_station_events(landed_text_as_kafka_records(landed)), values["source_execution_id"]
    )
    query = start_bronze_available_now(
        bronze, table_name=bronze_table, checkpoint_path=paths.station_checkpoint
    )
    completion = await_bounded_completion(
        query, timeout_seconds=float(values["stream_timeout_seconds"])
    )
    if completion["timed_out"]:
        query.stop()
        raise TimeoutError(f"Auto Loader did not finish within {completion['timeout_seconds']} s.")
    bronze_rows = (
        spark.table(bronze_table)
        .where(F.col("execution_id") == values["source_execution_id"])
        .count()
    )
    report["bronze_rows_for_execution"] = bronze_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Land the historical trip and weather samples
# MAGIC
# MAGIC **What:** Copy the committed 40-row January 2024 trip sample into the historical landing folder, and the committed 48-hour Open-Meteo archive sample into `landing/weather/` when the run uses `weather_source=sample_json`.
# MAGIC
# MAGIC **Why:** Notebook 06 ingests trips with Auto Loader from the sample namespace (the full month lives in a sibling namespace, so the two never mix), and notebook 07 reads the weather sample from the Volume. Nothing else lands that file; the first complete DEV run failed without it.
# MAGIC
# MAGIC **Input:** `fixtures/samples/historical_trips_202401_sample.csv` and `fixtures/samples/<weather_sample_file>`.
# MAGIC
# MAGIC **Output:** `landing/historical_trips/historical_trips_202401_sample.csv` (read by notebook 06) and `landing/weather/<weather_sample_file>` (read by notebook 07).
# MAGIC
# MAGIC **Key concepts:** Separate sample and full-month namespaces, idempotent copy.
# MAGIC
# MAGIC **Expected result:** Both sample files are present, `landed` the first time and `already_landed` on a rerun.
# MAGIC
# MAGIC **How to explain it:** "Development runs read the 40-row sample namespace; the full month uses a sibling, approved namespace."
# MAGIC
# MAGIC **Rerun and cost considerations:** Two small copies, skipped when the identical file exists. Full-month mode lands neither (it uses its own namespace and the archive API).

# COMMAND ----------

if land_trips:
    trip_sample = PROJECT_ROOT / "fixtures" / "samples" / "historical_trips_202401_sample.csv"
    trip_target = Path(paths.historical_dir) / trip_sample.name
    report["historical_landing"] = land_once(trip_target, trip_sample.read_text(encoding="utf-8"))
    report["historical_landing_dir"] = paths.historical_dir
if weather_target:
    weather_sample = PROJECT_ROOT / "fixtures" / "samples" / values["weather_sample_file"]
    report["weather_landing"] = land_once(
        Path(weather_target), weather_sample.read_text(encoding="utf-8")
    )
    report["weather_landing_path"] = weather_target
print(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Return the landing report
# MAGIC
# MAGIC **What:** Return the non-secret landing report to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept in the run output, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The report built in steps 2-4.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable task evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `PASS` with the landed counts.
# MAGIC
# MAGIC **How to explain it:** "Every task leaves a small JSON record of what it did."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A fresh environment becomes runnable without any manual file upload: the committed samples are landed per execution and ingested with Auto Loader, and the Bronze rows are indistinguishable from Event Hubs rows except for their lineage topic.
# MAGIC
# MAGIC **Common errors:** Reusing one checkpoint for many executions; landing different content under an existing execution ID; reading the landing folder without the execution filter.
# MAGIC
# MAGIC **Troubleshooting:** If landing refuses an existing file, a previous run used the same execution ID with different content - start a new run instead of overwriting.
# MAGIC
# MAGIC **Review questions:** Why does the collection time come from the snapshot? What does the Auto Loader checkpoint store?
# MAGIC
# MAGIC **Presentation summary:** Bounded, idempotent landing plus Auto Loader gives every environment its own reproducible Bronze data.

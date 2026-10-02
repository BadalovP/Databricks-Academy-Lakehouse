# Phase 3 - historical trips, weather, Lakeflow, dashboard and governance

Status: **the 40-row historical sample and the 48-hour weather sample are now VALIDATED LIVE on
GP1. The full monthly archive, Lakeflow and the published dashboard remain unexecuted.**

That distinction is the point of this document. The bounded sample runs provide real infrastructure
evidence for their stated scope only. They do not prove a full monthly archive, a Lakeflow update,
or a published dashboard. The exact status changes and evidence are recorded in
[LABS_1_TO_9_COVERAGE.md](LABS_1_TO_9_COVERAGE.md).

## What was built

| Area | Module | What it does | Tests |
|---|---|---|---|
| Historical trips | `historical.py`, `notebooks/06_historical_trips.py` | Auto Loader with an explicit schema, per-execution checkpoint, three-way valid/quarantine/duplicate split, trip-duration bounds, rider mix, daily demand | 26 + 14 existing |
| Weather | `weather.py`, `notebooks/07_weather_enrichment.py` | Bounded Open-Meteo archive retrieval, hourly normalisation, left join preserving every trip, demand comparison | 31 |
| Lakeflow | `pipeline/silver.py`, `pipeline/gold.py` | Eight declarative tables with 18 non-dropping expectations, reusing the notebook path's own functions | 22 |
| Dashboard | `sql/10`-`sql/13`, `DASHBOARD.md` | 22 read-only datasets, four pages, per-tile caveats | 32 (shared with governance) |
| Governance | `sql/20_governance_rls_cls.sql` | Row filter, two column masks, grants, ABAC tagging, inspection queries | shared |
| Maintenance | `sql/21_maintenance_optimize_vacuum.sql` | Z-ORDER vs liquid clustering, deletion vectors, CDF, column mapping, time travel | shared |

## Read-only Azure preflight, 2026-10-01 21:22Z - and the blocker it found

An authorized run attempt was preflighted and **stopped before any Azure write** because GP1 was
not running. The checks that need no compute were completed, and one of them found a real defect
in notebook 06.

| Check | Result |
|---|---|
| GP1 `0702-132442-toro5spu` | **TERMINATED.** Started manually 08:09:24Z, auto-terminated 09:11:39Z after 62 minutes idle (`INACTIVITY`, 60-minute setting). Not restarted by this project |
| GP2 | TERMINATED |
| Lakeflow | **Undeployed.** 50 pipelines visible in the workspace, zero matching `urbanflow` |
| UrbanFlow tables | Exactly the 9 expected, all `MANAGED`. No unexpected table appeared |
| `silver_station_status` schema | **27 columns, `is_operational` ABSENT** - the pre-correction state, as expected |
| `gold_station_shortage` schema | 10 columns, `execution_id` present |
| `gold_rebalancing_priority` schema | **12 columns, `execution_id` ABSENT** - the legacy state the backfill exists for |
| `fact_station_availability` schema | 15 columns |
| `bundle plan --select jobs.urbanflow_silver_gold_test` | `0 to add, 0 to change, 0 to delete, **1 unchanged**` - only that Job is in scope, already deployed |
| Volume `urbanflow_landing` | Contains `checkpoints` and `reports` only - **no `landing/` directory exists** |

Schema checks came from Unity Catalog metadata rather than a query, which is why they were
possible with no cluster. Row counts still require compute and remain unverified since the last
run.

### The blocker: notebook 06 read a path that has never existed

Step 7 of `notebooks/06_historical_trips.py` loaded the station reference from
`{volume_root}/landing/station_information`. The Volume has no `landing/` directory at all, so
the authorized historical run would have **failed at the join step after already writing the
Bronze trips table**, leaving a partial result to clean up.

This was a defect introduced in PR #43 and found only because the Volume was listed before
running. The fix reads the dimension from `dim_station_development_sample`, the Gold table that
already exists with the 40 rows and the exact columns the join needs, which:

- removes the dependency on a directory nobody created;
- avoids landing a second file, keeping the run inside the "copy ONLY the 40-row sample"
  authorization;
- reuses reference data that has already been validated live;
- fails loudly with a named reason if the dimension is missing or empty, instead of reporting
  every trip as unmatched.

A regression test now asserts the notebook reads the table and never that path again.

## Local prediction against the real committed samples (2026-10-01)

Before the live runs, the same checks were executed locally against the actual committed sample
files rather than synthetic fixtures. `tests/test_sample_end_to_end.py` keeps that prediction as
permanent regression coverage. The historical and weather samples then ran successfully on GP1 on
2026-10-02, as described at the top of this document and in the coverage matrix.

**This is a 40-row DEVELOPMENT SAMPLE, not January 2024 data, and this is a LOCAL Spark run,
not an Azure execution.** No Auto Loader, no `availableNow` trigger and no Delta MERGE were
involved; those run only on a cluster.

| Measure | Result |
|---|---|
| Landed rows | 40 |
| Valid / quarantine / duplicate | 40 / 0 / 0, accounted 40, reconciliation **PASS** |
| `ride_id` uniqueness in valid | 40 distinct, unique |
| Station match rate via `short_name` | **40 of 40, 1.0** |
| Join on the UUID instead | **0 rows**, as the module exists to demonstrate |
| Daily demand rows | 40, across 17 distinct days |
| Demand trips total vs valid trips | 40 = 40, reconciles |
| Member / casual | 35 / 5, matching the raw CSV exactly |
| Trip durations | min 1.28 min, max 30.10 min, avg 10.49, median 7.98 - all inside the 1-minute and 24-hour bounds |
| Weather hours parsed | 48, temperature completeness 1.0 |
| Weather join | every trip preserved, coverage reported, **PASS** |
| Duplicated weather hour (injected) | multiplies trips and is caught, **FAIL** as intended |

### What this run actually found

The schema-inference hazard is worse than previously described, and the real file is what showed
it. Inference types `start_station_id` as a double, and most values survive the round-trip by
luck - `7407.13` really does come back as `7407.13`. But any identifier whose decimal part ends
in zero does not: this sample contains **`5470.10` and `6740.10`**, which become `5470.1` and
`6740.1` and then match no GBFS short_name at all.

So inference does not fail and does not corrupt everything. It corrupts 2 of 45 identifiers,
the join still returns a plausible number of rows, and the loss looks exactly like genuinely
missing stations. The test now names those two values rather than describing the risk in general
terms.

## Decisions worth defending out loud

**Lakeflow reads Bronze in batch, not as a stream.** The Silver deduplication keeps the first
arrival of a repeated `event_id`, decided by a deterministic ordering over Kafka timestamp,
partition and offset. That is a non-time-based window, and Spark rejects those on streaming
DataFrames. The choice was either to rewrite the rule as a streaming `dropDuplicates` -
different semantics, non-deterministic tie-break, unbounded state without a watermark - or to
read in batch and keep the rule exactly as the notebook path and the tests have it. Keeping the
rule won. Silver and Gold are materialized views recomputed per update; Bronze stays a streaming
table, so ingestion is still incremental. For ~2,520 observations the recomputation is trivial.

**Expectations warn, they never drop.** A dropping expectation on Silver would delete the rows
the quarantine table exists to preserve, and Bronze would stop reconciling to Silver plus
quarantine plus duplicates. A test asserts `expect_all_or_drop` and `expect_all_or_fail` appear
nowhere.

**The pipeline modules contain no business rules.** They import from `urbanflow.silver` and
`urbanflow.gold`. A test fails if the classification or the priority arithmetic is reimplemented
there, because a Lakeflow pipeline and a notebook that quietly disagree about what `LOW_BIKES`
means is the most expensive failure available here - both look correct in isolation.

**Weather keeps its gaps.** A null reading stays null and is counted. Zero degrees and "unknown"
are different facts, and substituting one for the other invents a cold hour that never happened.
Coverage travels with every figure, because a chart built on half-covered hours looks exactly as
convincing as one built on full coverage.

**Every destructive SQL statement is commented, and a test enforces it.** `VACUUM` permanently
destroys the files time travel and `RESTORE` depend on; applying a row filter silently changes
what every other principal can see. `tests/test_sql_assets.py` asserts that no `VACUUM`,
`OPTIMIZE`, `SET MASK`, `SET ROW FILTER`, `GRANT`, `SET TBLPROPERTIES`, `CLUSTER BY` or
retention-check override is active in any file. The only live statements in the maintenance file
are `DESCRIBE`.

## What remains to go live

| Piece | Blocked on | Billable? |
|---|---|---|
| Full-month historical Auto Loader | The official archive landed in its isolated monthly subdirectory and GP1 already running | Cluster time only; GP1 already exists |
| Full-month weather enrichment | The selected full-month trip execution, so sample and monthly rows remain separate | One public API call plus cluster time |
| Lakeflow pipeline | Explicit approval. Lakeflow runs on **serverless** compute, which this project does not assume is free | **Yes** |
| AI/BI dashboard object | A SQL warehouse to execute the datasets | **Yes** |
| RLS / column masks | Account groups (`urbanflow_admins`, `urbanflow_region_*`) that have not been created, and a decision to change visibility | No, but hard to reverse safely |
| OPTIMIZE / VACUUM | Nothing technically, but VACUUM is irreversible so it needs its own approval | Cluster time |

### The archive download is deliberately not automated

Notebook 06 does **not** download anything. A monthly Citi Bike archive is a large file, and
pulling one onto a shared academy cluster and into a managed Volume is exactly the kind of
unbounded transfer that should be a deliberate, approved act. Step 4 of the notebook lists the
landing directory and raises if it finds no CSV, so a missing archive is a clear error rather
than a successful load of nothing.

The bounded alternative has now run: the committed
`data/samples/historical_trips_202401_sample.csv` (40 real rows) exercised the complete path on
GP1 without a large transfer. Its evidence says plainly that it covers 40 rows rather than a
month.

## Honest limitations

- **Only bounded Phase 3 samples have run on a cluster.** The 40-row historical sample and
  48-hour weather sample validated the Auto Loader, `availableNow`, Delta persistence and
  reconciliation path. The full January archive and Lakeflow pipeline remain unexecuted.
- **Lakeflow modules cannot be imported in tests.** `pyspark.pipelines` only exists inside a
  running pipeline and `spark` is an injected global, so those 22 tests are source- and
  config-level. That is a real limitation, not a workaround; importing a stub would only prove
  the stub works.
- **The station dimension is 40 rows.** It is named `dim_station_development_sample` everywhere
  for that reason. A historical trip that finds no current station is expected, not a defect, and
  the match rate is reported rather than assumed.
- **The weather series is one city coordinate.** Not per-station weather. Every row carries
  `weather_grid_label`.
- **The dashboard is not published.** Its 22 read-only datasets ran successfully after the Phase 2
  correction removed the 89 stale out-of-service priorities. Publishing still requires separate
  approval for billable SQL warehouse compute.

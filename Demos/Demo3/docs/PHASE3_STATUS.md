# Phase 3 - historical trips, weather, Lakeflow, dashboard and governance

Status: **implemented and tested locally. Nothing in Phase 3 has executed in Azure.**

That distinction is the point of this document. Every item below has real tests, several against
a genuine local Spark session, but a passing test proves the code does what it says, not that it
has run against real infrastructure. No row in
[LABS_1_TO_9_COVERAGE.md](LABS_1_TO_9_COVERAGE.md) moved to `Validated live` in Phase 3.

## What was built

| Area | Module | What it does | Tests |
|---|---|---|---|
| Historical trips | `historical.py`, `notebooks/06_historical_trips.py` | Auto Loader with an explicit schema, per-execution checkpoint, three-way valid/quarantine/duplicate split, trip-duration bounds, rider mix, daily demand | 26 + 14 existing |
| Weather | `weather.py`, `notebooks/07_weather_enrichment.py` | Bounded Open-Meteo archive retrieval, hourly normalisation, left join preserving every trip, demand comparison | 31 |
| Lakeflow | `pipeline/silver.py`, `pipeline/gold.py` | Eight declarative tables with 18 non-dropping expectations, reusing the notebook path's own functions | 22 |
| Dashboard | `sql/10`-`sql/13`, `DASHBOARD.md` | 22 read-only datasets, four pages, per-tile caveats | 32 (shared with governance) |
| Governance | `sql/20_governance_rls_cls.sql` | Row filter, two column masks, grants, ABAC tagging, inspection queries | shared |
| Maintenance | `sql/21_maintenance_optimize_vacuum.sql` | Z-ORDER vs liquid clustering, deletion vectors, CDF, column mapping, time travel | shared |

## Local end-to-end validation against the REAL committed samples (2026-10-01)

The Azure runs for Phase 3 are authorized but blocked on compute, so the same checks those runs
perform were executed locally against the actual committed sample files rather than against
synthetic fixtures. `tests/test_sample_end_to_end.py` holds them as permanent coverage.

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

## What each piece needs to go live, and why it has not

| Piece | Blocked on | Billable? |
|---|---|---|
| Historical Auto Loader | An official Citi Bike archive landed and expanded in the Volume, and GP1 running | Cluster time only; GP1 already exists |
| Weather enrichment | The trip table, so it follows the archive | One free public API call plus cluster time |
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

A bounded alternative exists and needs no approval: the committed
`data/samples/historical_trips_202401_sample.csv` (40 real rows) can be copied into the landing
directory to exercise the entire path end to end. That proves the mechanics without a large
transfer, and the evidence report would say plainly that it covers 40 rows rather than a month.

## Honest limitations

- **No Phase 3 code has run on a cluster.** The Auto Loader wiring, the `availableNow` write and
  the Delta MERGE statements are asserted at statement level, because `delta-spark` 3.4.0 does
  not resolve against the local pyspark 4.1.1 and changing that runtime under a 362-test suite
  was not worth it.
- **Lakeflow modules cannot be imported in tests.** `pyspark.pipelines` only exists inside a
  running pipeline and `spark` is an injected global, so those 22 tests are source- and
  config-level. That is a real limitation, not a workaround; importing a stub would only prove
  the stub works.
- **The station dimension is 40 rows.** It is named `dim_station_development_sample` everywhere
  for that reason. A historical trip that finds no current station is expected, not a defect, and
  the match rate is reported rather than assumed.
- **The weather series is one city coordinate.** Not per-station weather. Every row carries
  `weather_grid_label`.
- **The dashboard is not published, and Page 1 should not be** until the Phase 2 correction run
  completes. Until then `gold_station_shortage` holds 746 rows of which 89 are out-of-service
  stations wrongly listed as actionable, and putting those on a supervisor's screen would be
  worse than showing nothing.

# Silver and Gold bounded run - EXECUTED 2026-09-30

## Result: reconciliation PASS, idempotency PROVEN, one defect found and fixed

Both runs succeeded on GP1 and every reconciliation passed. Row counts and event IDs tie
out exactly, and a second identical run left all eight tables untouched, which proves
Delta MERGE idempotency against the real engine rather than a mock.

| Measure | Value |
|---|---|
| Source | `bronze_station_status`, 2,520 rows, execution `urbanflow-20260929T195132Z-r3` |
| Silver / quarantine / duplicates | 2,520 / 0 / 0, accounted 2,520, IDs unique, `PASS` |
| Fact rows | 2,520, matching Silver, distinct IDs, `PASS` |
| Daily summary | 2,520 rows, observation total ties to the fact count |
| Station dimension | 40 rows, the committed sample, as designed |
| Shortages / priorities | 746 each - **this number was wrong, see below** |
| Dry run | `937836701866956` SUCCESS in 86 s, both tasks confirmed no read and no write |
| First real run | `244764181867554` SUCCESS in 314 s |
| Idempotency run | `284553247546526` SUCCESS in 206 s, all eight tables `inserted_rows = 0`, before == after, `table_existed = true` |
| GP1 afterwards | `RUNNING`, `last_restarted` unchanged - never started, restarted or terminated by this project |
| Lakeflow | still not deployed |

### The defect the run exposed

A green Job was not treated as proof, and checking the numbers against Phase 1's
in-memory preview is what caught it. The run produced **746** shortage rows where
`transformations.classify_availability` - this project's own reference implementation of
the same rule - produces **657**.

`silver.py` classified purely on counts. An out-of-service station reports 0 bikes and 0
docks, so it was flagged `LOW_BIKES_AND_DOCKS` even though no amount of rebalancing can
fix it. Measured against the live feed: 2,520 stations, 88 of them not installed or not
renting, and the two implementations differed by exactly 88. The snapshot's gap was 89
because one station changed state in between.

Fixed by gating on `is_installed AND is_renting`, gating dock shortages additionally on
`is_returning`, adding an `is_operational` column, and restoring the reference
vocabulary (`OUT_OF_SERVICE` and `AVAILABLE`). Two regression tests now pin the behaviour,
one of which asserts the Spark and pure-Python implementations agree case by case, since
they drifted apart once already.

**Consequence: `gold_station_shortage` and `gold_rebalancing_priority` currently hold 746
rows, roughly 89 of which are out-of-service stations that should read `OUT_OF_SERVICE`.
Those two tables are stale until the Job is rerun with the corrected code.** The other six
tables are unaffected in row count; `silver_station_status` and `fact_station_availability`
carry corrected `availability_status` values only after a rerun.

## Correction run - prepared, NOT yet executed

Two further defects were found by inspecting the live tables before rerunning, and both
would have made a naive redeploy-and-rerun either fail outright or silently leave wrong
data behind.

### Why a plain rerun would NOT have worked

| Problem | Evidence from the live tables | Fix |
|---|---|---|
| Delta MERGE does not evolve the target schema | `silver_station_status` has 27 columns and no `is_operational`; the corrected contract has 28. The MERGE would have failed on an unresolvable target column | `evolve_delta_schema` runs an explicit, narrow `ALTER TABLE ... ADD COLUMNS` for genuinely missing columns only, before the MERGE. Only ever adds; never drops, renames or retypes |
| UPDATE and INSERT cannot shrink a derived table | `gold_station_shortage` and `gold_rebalancing_priority` hold 746 rows each; the corrected rule produces ~657, so ~89 out-of-service stations would stay listed as actionable forever | `replace_execution_scope` uses one atomic MERGE whose `WHEN NOT MATCHED BY SOURCE AND execution_id = '<id>' THEN DELETE` removes exactly the stale rows for this execution |
| The priority table could not be scoped | `gold_rebalancing_priority` had no `execution_id`, so a cleanup would have had to overwrite the whole table and destroy any other execution's rows | `rebalancing_priority` now carries `execution_id`, and the schema migration adds the column to the existing table |

`spark.databricks.delta.schema.autoMerge.enabled` was deliberately NOT used: it would
silently absorb any future drift, including an accidentally removed column, across every
table the session touches. The explicit migration names each added column in its report.

### Expected results for the correction run

Derived from the ORIGINAL Bronze snapshot, not a freshly fetched feed, so the numbers are
reproducible: 2,520 observations, of which 88 stations were not installed or not renting at
the time of that snapshot.

| Table | Before | After | Change |
|---|---|---|---|
| `silver_station_status` | 2,520 rows, 27 cols | 2,520 rows, 28 cols | `is_operational` added; `availability_status` corrected for out-of-service stations |
| `fact_station_availability` | 2,520 | 2,520 | corrected `availability_status`, row count unchanged |
| `gold_station_shortage` | 746 | ~657 | ~89 stale out-of-service rows DELETED |
| `gold_rebalancing_priority` | 746, 12 cols | ~657, 13 cols | `execution_id` added; ~89 stale rows DELETED |
| `quarantine_station_status` | 0 | 0 | unchanged, no row fails the contract |
| `duplicate_station_status` | 0 | 0 | unchanged |
| `gold_daily_station_summary` | 2,520 | 2,520 | unchanged counts |
| `dim_station_development_sample` | 40 | 40 | unchanged |

The exact post-run shortage total is whatever the corrected rule yields for that snapshot;
657 is the figure Phase 1's in-memory preview produced from the same data, so agreement
with it is the check.

### Commands

```bash
cd Demos/Demo3
python -m urbanflow.cli compute-status --profile dev --require-ready
databricks bundle deploy -t azure --profile dev --select jobs.urbanflow_silver_gold_test
databricks bundle run urbanflow_silver_gold_test -t azure --profile dev     --params run_transform=true,source_execution_id=urbanflow-20260929T195132Z-r3
# then once more, unchanged, to confirm idempotency of the corrected result
```

### Reconciliation to check afterwards

1. `silver_station_status` has `is_operational`, and `schema_migration.added_columns`
   reports it.
2. Silver + quarantine + duplicates still equals 2,520, IDs unique, `PASS`.
3. `fact_station_availability` still equals Silver with distinct IDs.
4. Shortage and priority counts DROPPED to the corrected figure, and
   `stale_rows_removed` reports how many were deleted.
5. No row in either table has `availability_status = 'OUT_OF_SERVICE'`.
6. `scope_matches_source` is true for both scoped tables.
7. The repeat run reports `stale_rows_removed = 0` and unchanged counts everywhere.

### Rollback

The eight tables are new and nothing else reads them, so rollback is `DROP TABLE` followed
by a rerun. The migration only adds nullable columns, so it needs no rollback of its own.
Bronze, the Volume and Phase 1's evidence are never written by this phase.

## Original plan, retained for reruns

[Back to README](../README.md) · [First streaming test](FIRST_STREAMING_TEST.md) · [Cost and safety](COST_AND_SAFETY.md)

Phase 1 (Event Hubs to Bronze) is **validated live**. Everything in this document is
**implemented and tested locally but has never run in Azure**, and no row of Silver or Gold
exists yet. Nothing here may be described as validated until the run below has happened and
reconciled.

## What already exists in Azure

| Resource | State |
|---|---|
| `dbr_dev.parvinbadalov_urbanflow.bronze_station_status` | 2,520 real rows from execution `urbanflow-20260929T195132Z-r3` |
| `dbr_dev.parvinbadalov_urbanflow.urbanflow_landing` | Managed Volume holding the Phase 1 checkpoint and reports |
| Job `404404108673495` | Phase 1 bounded streaming Job, already run twice, not part of this phase |
| GP1 `0702-132442-toro5spu` | Shared cluster. Must already be `RUNNING`; this project never starts it |

## Tables this run would create

All are inside the project's own identity-prefixed schema. Nothing outside
`dbr_dev.parvinbadalov_urbanflow` is written.

| Table | Grain | Written by |
|---|---|---|
| `silver_station_status` | one row per valid observation | `04_bronze_to_silver` |
| `quarantine_station_status` | one row per contract violation | `04_bronze_to_silver` |
| `duplicate_station_status` | one row per removed redelivery | `04_bronze_to_silver` |
| `dim_station_development_sample` | one row per station in the saved 40-row sample | `05_silver_to_gold` |
| `fact_station_availability` | one row per observation, Silver's grain | `05_silver_to_gold` |
| `gold_daily_station_summary` | one row per station per day | `05_silver_to_gold` |
| `gold_station_shortage` | one row per station currently short | `05_silver_to_gold` |
| `gold_rebalancing_priority` | one row per station needing attention, ranked | `05_silver_to_gold` |

## Expected results, including the ones that look wrong but are not

From the 2,520 Bronze rows, with the thresholds both at 2:

- **Silver 2,520, quarantine 0, duplicates 0.** Phase 1 already reconciled exactly, so no
  row should fail the contract. A non-zero quarantine would be a genuine finding, not a
  failure of the run.
- **Fact 2,520 rows**, matching Silver exactly, with `reconcile_gold` returning `PASS`.
- **Daily summary: every row `is_trend_capable = false`.** There is one snapshot, so each
  station has exactly one observation and min, max and average all equal it. This is the
  honest answer, not a bug.
- **Shortages around 657 rows** (Phase 1's in-memory preview saw 278 LOW_BIKES,
  374 LOW_DOCKS and 5 LOW_BIKES_AND_DOCKS).
- **Roughly 2,480 fact rows will have a null `station_name`.** The dimension is built from
  the committed 40-station sample, which is why the table is named
  `dim_station_development_sample`. The fact join is deliberately a LEFT join so a
  reference-data gap never deletes a real observation. This is expected.

## Commands, in order

```bash
cd Demos/Demo3

# 1. Gate: stop unless GP1 is already RUNNING. Never starts it.
python -m urbanflow.cli compute-status --profile dev --require-ready

# 2. Deploy ONLY the Phase 2 Job. Phase 1's Job and Lakeflow are untouched.
databricks bundle plan   -t azure --profile dev --select jobs.urbanflow_silver_gold_test
databricks bundle deploy -t azure --profile dev --select jobs.urbanflow_silver_gold_test

# 3. Dry run first: both notebooks exit before any write when run_transform=false.
databricks bundle run urbanflow_silver_gold_test -t azure --profile dev \
    --params run_transform=false

# 4. The real run.
databricks bundle run urbanflow_silver_gold_test -t azure --profile dev \
    --params run_transform=true,source_execution_id=urbanflow-20260929T195132Z-r3

# 5. Idempotency proof: run step 4 again. Row counts must not change.
```

Step 5 is the part that cannot be proven locally. `delta-spark`'s jars do not resolve in
this development environment (local Spark raises `ClassNotFoundException` for
`DeltaSparkSessionExtension`), so the Delta engine is mocked in
`tests/test_persistence.py`. The MERGE contract is pinned there - a repeat merge must
report `inserted_rows = 0` and must not recreate an existing table - but only a real second
run proves Delta itself behaves that way. Treat the second run as a required step, not an
optional extra.

## Reconciliation to check afterwards

1. `silver + quarantine + duplicates == 2,520` and Silver event IDs are unique.
2. `fact_station_availability` row count equals Silver, with distinct event IDs.
3. Daily-summary observation totals sum back to the fact row count.
4. `reconcile_silver` and `reconcile_gold` both report `PASS`.
5. After the second run, every table's row count is unchanged from the first.

## Cost

GP1 is a shared cluster that is already running and already billing, so the marginal cost
of attaching two short notebook tasks is small; each task is capped at 600 seconds and the
Job at 1,200. This is a target, not an enforceable cap. Stop rather than extend it.

## Failure handling and rollback

- Both notebooks refuse to run on any cluster outside `APPROVED_RUN_CLUSTER_IDS`, and both
  default to `run_transform=false`.
- `merge_delta_table` validates every table identifier and refuses a name that is not one
  to three plain identifier parts, so a malformed or injected name cannot reach SQL.
- It also refuses to MERGE when the source contains duplicate keys, rather than letting
  Delta collapse them silently.
- Rollback is a `DROP TABLE` on the eight tables listed above; they are new and nothing else
  reads them. Bronze, the Volume, the Phase 1 Job and its evidence are never written by this
  phase.
- If a task fails part-way, the tables it had already merged stay valid because MERGE is
  keyed on immutable event IDs; rerunning is safe.

## Not in scope for this run

No Event Hubs producer, no Lakeflow deployment or start, no new cluster, no serverless, no
change to Phase 1 resources, and nothing in `dbr_dev_trial`.

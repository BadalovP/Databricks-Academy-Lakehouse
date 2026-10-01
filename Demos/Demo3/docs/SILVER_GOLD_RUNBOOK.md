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
fix it.

Two different numbers circulate for the size of this gap, and only one of them governs the
correction. A live GBFS request made while diagnosing the defect found **88** stations not
installed or not renting - but that was a different moment, and it is NOT the basis for any
expectation here. The authoritative figure comes from the original snapshot itself: the
`medallion_preview` block recorded in
`evidence/urbanflow-20260929T195132Z-r3.bronze.json` was produced by
`prepare_station_batch` over the very same 2,520 Bronze records this execution consumed from
Kafka, using `classify_availability`, the corrected rule. It reports **657** shortages
(LOW_BIKES 278, LOW_DOCKS 374, LOW_BIKES_AND_DOCKS 5). Against the 746 the Gold run wrote,
the gap is exactly **89**.

Fixed by gating on `is_installed AND is_renting`, gating dock shortages additionally on
`is_returning`, adding an `is_operational` column, and restoring the reference
vocabulary (`OUT_OF_SERVICE` and `AVAILABLE`). Two regression tests now pin the behaviour,
one of which asserts the Spark and pure-Python implementations agree case by case, since
they drifted apart once already.

**Consequence: `gold_station_shortage` and `gold_rebalancing_priority` currently hold 746
rows, exactly 89 of which are out-of-service stations that should read `OUT_OF_SERVICE`.
Those two tables are stale until the Job is rerun with the corrected code.** The other six
tables are unaffected in row count; `silver_station_status` and `fact_station_availability`
carry corrected `availability_status` values only after a rerun.

## Correction run - prepared and approved, BLOCKED on compute

The run below is **approved** but has not executed, across two attempts.

The second attempt, at 2026-10-01 21:22Z, is the instructive one. GP1 had genuinely been started
manually at **08:09:24Z** - the cluster's own `last_restarted_time` confirms it - but nothing
attached to it, so its 60-minute inactivity timer elapsed and it auto-terminated at **09:11:39Z**
with `termination_reason: INACTIVITY`. By the time the run was attempted the cluster had been down
for twelve hours.

The practical lesson for the next attempt: the 60 minutes is a timer on *inactivity*, and it only
resets once a workload attaches. Starting GP1 and then doing something else for an hour loses the
window. Starting it and running the Job immediately keeps it alive, and the Job itself resets the
timer.

This project does not start, restart, resize or terminate a cluster, so both attempts stopped
before any Azure write. The approval stands.

Nothing has been written in the meantime, so the eight tables remain in their pre-correction
state: `silver_station_status` at 2,520 rows and 27 columns with no `is_operational`,
`gold_station_shortage` at 746 rows, and `gold_rebalancing_priority` at 746 rows with no
`execution_id`.

Two further defects were found by inspecting the live tables before rerunning, and both
would have made a naive redeploy-and-rerun either fail outright or silently leave wrong
data behind.

### Why a plain rerun would NOT have worked

| Problem | Evidence from the live tables | Fix |
|---|---|---|
| Delta MERGE does not evolve the target schema | `silver_station_status` has 27 columns and no `is_operational`; the corrected contract has 28. The MERGE would have failed on an unresolvable target column | `evolve_delta_schema` runs an explicit, narrow `ALTER TABLE ... ADD COLUMNS` for genuinely missing columns only, before the MERGE. Only ever adds; never drops, renames or retypes |
| UPDATE and INSERT cannot shrink a derived table | `gold_station_shortage` and `gold_rebalancing_priority` hold 746 rows each; the corrected rule produces 657, so 89 out-of-service stations would stay listed as actionable forever | `replace_execution_scope` uses one atomic MERGE whose `WHEN NOT MATCHED BY SOURCE AND execution_id = '<id>' THEN DELETE` removes exactly the stale rows for this execution |
| The priority table could not be scoped | `gold_rebalancing_priority` had no `execution_id`, so a cleanup would have had to overwrite the whole table and destroy any other execution's rows | `rebalancing_priority` now carries `execution_id`, and the schema migration adds the column to the existing table |
| **A scoped delete cannot match a NULL, so the legacy rows would have escaped it - silently** | Adding `execution_id` to the existing 746-row priority table leaves every one of those rows NULL. The MERGE then updates the 657 that still qualify (filling in their execution id) and leaves the other 89 alone, because `NULL = '<execution>'` evaluates to NULL rather than true. `scope_matches_source` afterwards compares 657 against 657 and **passes**, with 89 broken stations still on the action list | `backfill_execution_id` attributes the legacy rows from verified lineage first; `replace_execution_scope` then refuses outright to run while any row has no execution id; and `verify_execution_scope` audits the whole table rather than the scoped slice |

`spark.databricks.delta.schema.autoMerge.enabled` was deliberately NOT used: it would
silently absorb any future drift, including an accidentally removed column, across every
table the session touches. The explicit migration names each added column in its report.

### How the legacy priority rows were attributed

The 89 stale priority rows can only be deleted once every legacy row carries an execution
id, and assigning them all to the current run because it happens to be the run executing
would be a guess. Two independent pieces of evidence establish their provenance instead:

1. **Recorded at creation.** `evidence/urbanflow-20260929T195132Z-r3.gold.json` shows
   `priorities` with `table_existed: false` and `rows_before: 0`, inserting 746 rows. The
   table was *created* by this execution. The `.gold.run2.json` repeat then shows
   `rows_before: 746, inserted_rows: 0`, so nothing else has written to it since.
2. **Checked at run time, per row.** `backfill_execution_id` takes each legacy row's
   execution id from `gold_station_shortage`, which shares the same grain
   (`station_id`, `observed_at`) and does carry the column for all 746 rows. It refuses to
   write anything unless the lineage has no NULL execution id, every lineage key maps to
   exactly one execution, and *every* legacy row is attributable. An unattributable row
   stops the run and reports the count rather than being adopted.

Rows that already carry an execution id are never reassigned: the update fires only
`WHEN MATCHED AND execution_id IS NULL`.

**The order matters, and it is the subtle part.** Both tables are the same filter over the
same fact, so the legacy priority rows correspond to the legacy *shortage* rows. The
attribution therefore reads the shortage table while it is still uncorrected - once the
shortage replacement has removed the 89 rows that no longer qualify, the evidence for
exactly the rows needing attribution would be gone and all 89 would look unattributable. A
regression test asserts the attribution happens before the shortage replacement, and fails
if the two are reordered.

### Expected results for the correction run

Derived from the ORIGINAL Bronze snapshot, not a freshly fetched feed. The shortage figure
is not an estimate: 657 is what `classify_availability` produced from these same 2,520
records, recorded in `evidence/urbanflow-20260929T195132Z-r3.bronze.json` at ingestion time.

| Table | Before | After | Change |
|---|---|---|---|
| `silver_station_status` | 2,520 rows, 27 cols | 2,520 rows, 28 cols | `is_operational` added; `availability_status` corrected for out-of-service stations |
| `fact_station_availability` | 2,520 | 2,520 | corrected `availability_status`, row count unchanged |
| `gold_station_shortage` | 746 | 657 | 89 stale out-of-service rows DELETED |
| `gold_rebalancing_priority` | 746, 12 cols | 657, 13 cols | `execution_id` added and backfilled for all 746 legacy rows; 89 stale rows DELETED |
| `quarantine_station_status` | 0 | 0 | unchanged, no row fails the contract |
| `duplicate_station_status` | 0 | 0 | unchanged |
| `gold_daily_station_summary` | 2,520 | 2,520 | unchanged counts |
| `dim_station_development_sample` | 40 | 40 | unchanged |

Expected shortage composition, also from that recorded preview: LOW_BIKES 278,
LOW_DOCKS 374, LOW_BIKES_AND_DOCKS 5. A post-run total that is not exactly 657, or a
composition that differs from those three figures, means something other than the
classification change moved - and is a reason to stop rather than to adjust the expectation.

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
4. `legacy_priority_backfill` reports `null_rows_before: 746`,
   `unattributable_rows: 0`, `backfilled_rows: 746`, `null_rows_after: 0`.
5. Shortage and priority counts both DROPPED to exactly 657, and `stale_rows_removed`
   reports 89 for each.
6. No row in either table has `availability_status = 'OUT_OF_SERVICE'`.
7. `derived_table_verification` is `PASS` for both tables, with
   `missing_from_table: 0`, `unexpected_in_table: 0` and **`unassigned_rows: 0`**. The last
   one is the check that the scoped comparison could not make: a non-zero value means legacy
   rows escaped the delete, which is precisely the failure this run exists to prevent.
   Step 8 of the notebook raises rather than printing a warning, so the Job fails loudly.
8. `other_executions_preserved` is true for both tables.
9. The repeat run reports `stale_rows_removed = 0`, `backfilled_rows = 0` and unchanged
   counts everywhere.

A note on what is and is not proven locally: the schema migration, the attribution guards,
every NULL and anti-join count and the whole-table verification all run against a real Spark
session in the test suite. The physical Delta MERGE and DELETE cannot, because delta-spark's
jars do not resolve in this environment, so those statements are asserted at statement level
and their effect is proven only by this live run. That is why the reconciliation above is
checked against the tables themselves rather than trusted from the report.

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
- **Shortages exactly 657 rows**: 278 LOW_BIKES, 374 LOW_DOCKS, 5 LOW_BIKES_AND_DOCKS.
  This is not a target to be approximated - it is what `classify_availability` produced from
  these same 2,520 records, recorded in the Bronze evidence at ingestion time.
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

# LAB 08 - Azure PROD Bronze Duplicate Remediation Plan (NOT EXECUTED)

Prepared 2026-09-19 alongside the payment-idempotency/reconciliation code fix
(`fix/lab08-payment-idempotency-reconciliation`). This is a plan only. No step
in this document has been executed. It requires explicit separate approval,
and even then should be run against Azure PROD only after the code fix in
this PR has been deployed and verified once with a normal (non-refresh) job
run.

## Is a full refresh actually necessary?

**Likely not for correctness, but this is a claim to verify, not a
guarantee — see the validation procedure below before trusting it for Azure
PROD.** `bookings_silver`, `current_bookings_silver` and `payments_silver`
are Lakeflow materialized views. Databricks documents materialized views as
always reflecting the result of their defining query over the current state
of their sources — whether the engine computes that incrementally or by a
full recompute is an internal optimization detail, not something the query
author controls or needs to reason about for correctness. Under that
documented contract, the very next normal (non-refresh) pipeline run after
this PR is deployed should recompute Silver and Gold using the new
`payments_silver` deduplication and `current_bookings_silver`
booking-quality-filter logic, regardless of how many duplicate rows Bronze
already holds. **This has not been empirically validated by an actual
pipeline run in this session** — no bundle deploy or job run was performed,
per this task's constraints — so treat it as a well-supported expectation,
not a confirmed fact, until the validation procedure below has been run
once in a real environment.

**Useful for hygiene, not urgent for correctness.** `bookings_bronze` and
`payments_bronze` currently hold roughly 7x their intended row count
(175,000 raw rows for a 25,000-row logical seed, per
`lab08_photon_fix_and_reconciliation_observation.md`). That bloat costs
extra storage and extra compute on every future Silver recomputation (more
rows to scan and deduplicate), but it should no longer produce incorrect
Gold output once this fix is deployed and the validation below confirms it.
A full refresh to shrink Bronze back to its intended size is a cost/hygiene
decision, not a data-correctness requirement.

**Residual risk this PR reduces but does not fully close:** this PR's
`notebooks/00_seed_raw_data.ipynb` change now writes each raw table to a
single Parquet file at a path derived from `seed_limit`, instead of a
fresh Spark-generated file name on every write. Auto Loader's documented
default `cloudFiles.allowOverwrites=false` means it should not reprocess a
path it has already ingested, so a **rerun with an unchanged `seed_limit`
should stop adding new duplicate rows to Bronze** going forward — this is a
genuine source-level fix, not only a downstream mitigation, but it is based
on documented Auto Loader behavior and has not been empirically confirmed
by an actual run in this session (see the Validation section below). It
does not cover every possible content change (for example,
`samples.wanderbricks` changing upstream without any local `seed_limit`
change keeps the same path and would not be picked up), and it does not
retroactively remove Bronze rows already accumulated from runs before this
fix shipped — that cleanup is the full-refresh procedure later in this
document. Silver-layer deduplication remains a defense-in-depth backstop
for whatever duplication does still reach Bronze; it does not by itself
stop Bronze from accumulating rows.

## Validating that Silver/Gold pick up this fix without a full refresh

This is the procedure to run once, in a controlled DEV target, before
trusting the "no full refresh needed" claim above for Azure PROD.

1. Deploy this PR's code to `personal_dev` (`databricks bundle deploy -t
   personal_dev`) and run the promotion job once normally — no full refresh,
   no checkpoint reset (`databricks bundle run travelops_promotion_job -t
   personal_dev`). This step executes a real deployment/run and was not
   performed as part of this fix — it is the recommended next action, not
   something already done.
2. Query the DEV `payments_silver` row count and compare it to the DEV
   `payments_bronze` row count: the Silver count should be lower (deduped)
   and the `dropDuplicates` logic should be visibly in effect.
3. Query DEV `gold_production_health` and confirm
   `payment_amount_mismatch_count = 0` and `invalid_booking_amount_count =
   0` (the new columns from this PR's `pipeline/gold.py`). If the pipeline
   were somehow still running the old logic, these columns would not exist
   at all, which is an unambiguous, easy-to-spot failure signal.
4. Rerun the promotion job a second time with an unchanged `seed_limit` and
   confirm `bookings_bronze`/`payments_bronze` row counts in DEV do **not**
   grow between the two runs. Growth here would mean the stable-file-name
   fix did not achieve idempotency as expected, even though Silver/Gold
   correctness would still hold via the dedup backstop.

### Recovery if the new logic does not appear after a normal run

If step 3 shows the old schema (missing the new health columns) or stale
reconciliation numbers persist after a normal run, the recovery is a
**table-scoped refresh of the specific Silver/Gold materialized views**
(e.g. `payments_silver`, `current_bookings_silver`, `gold_payment_reconciliation`,
`gold_production_health`) via Lakeflow's per-table refresh (for example
`databricks pipelines start-update --full-refresh-selection <table>`, or the
equivalent "Refresh" action on that specific table in the Lakeflow UI). This
is a materially different and much lower-risk operation than the Bronze
full refresh later in this document: a materialized view reads its sources
with a plain batch read on every computation and owns no Auto Loader
checkpoint, so refreshing one recomputes its content from the current state
of its sources with **no effect on Bronze data, raw files, or Auto Loader
checkpoints at all**. This recovery step was not executed in this session
and requires the same explicit approval as any other production action
before being run against Azure PROD; it is documented here so a stale-result
scenario in a controlled DEV test has a known, safe next step.

## If a full refresh is later approved

### Scope
A full refresh only of the affected Azure PROD Bronze tables
(`bookings_bronze`, `payments_bronze`; optionally `booking_updates_bronze`
and `reviews_bronze`, which share the same ingestion mechanism and were not
independently row-counted in this investigation) in
`dbr_dev.parvinbadalov_lab08_prod`. Not Personal DEV/PROD, not Terraform, not
any other Lab 8 or repository resource.

### Pre-conditions
1. This PR is merged and deployed, and the pipeline has completed at least
   one normal run so Silver/Gold correctness from the code fix is confirmed
   independent of any Bronze cleanup.
2. A snapshot of current state is captured for rollback comparison: row
   counts and a checksum-style aggregate (e.g. `SELECT COUNT(*), SUM(hash(*))`
   or a Delta table version/timestamp) for `bookings_bronze`,
   `payments_bronze`, `current_bookings_silver`, `payments_silver`, and
   `gold_production_health`, plus the Delta history (`DESCRIBE HISTORY`) of
   each affected table so a specific version can be identified if needed.
3. Confirm current raw Volume contents: at the time of this investigation,
   `raw/payments/` contained exactly one live `part-*.parquet` data file
   (old files are removed by each `df.write.mode("overwrite")`, only
   `_committed_*`/`_started_*` transaction markers persist) — so a full
   refresh's Auto Loader re-ingestion should pick up only the current,
   already-referentially-consistent seed, not all historical duplicate
   generations. Re-verify this file listing immediately before refreshing,
   since a concurrent seed run would change it.

### Backups
Delta's built-in time travel is used instead of a separate backup:
`DESCRIBE HISTORY` on each affected table before the refresh gives a
specific version/timestamp that `RESTORE TABLE ... TO VERSION AS OF <n>`
can return to if the refresh produces unexpected results. No external
backup of the raw Volume is needed because the raw seed is fully
reproducible from `samples.wanderbricks` by rerunning
`00_seed_raw_data.ipynb`.

### Execution (not performed)
1. Trigger a Lakeflow full refresh scoped to `bookings_bronze` and
   `payments_bronze` (Lakeflow supports refreshing selected tables rather
   than the whole pipeline). This resets Auto Loader's checkpoint for those
   tables only and reprocesses whatever raw files currently exist under
   their raw paths.
2. Allow the full pipeline update to complete so Silver and Gold recompute
   from the refreshed Bronze tables.
3. Do not touch `booking_updates_bronze`/`reviews_bronze` in the same pass
   unless their duplicate accumulation is independently confirmed — this
   plan does not assume they need the same treatment without verification.

### Validation after refresh
1. Re-run the row-count checks from
   `lab08_photon_fix_and_reconciliation_observation.md`:
   `bookings_bronze`/`payments_bronze` total row counts should drop to
   match the current referentially-consistent seed size (not 175,000).
2. Re-run `01_validate_gold_health.ipynb` (or query
   `gold_production_health` directly) and confirm
   `payment_amount_mismatch_count = 0` and `invalid_booking_amount_count = 0`
   still hold, and that `no_payment_record_count` /
   `pending_or_failed_payment_count` numbers look consistent with a single
   clean seed generation rather than 7x-inflated duplicate-driven noise.
3. Compare `current_booking_count` and the Gold row counts against the
   pre-refresh snapshot from the Pre-conditions step to confirm no
   unexpected data loss.

### Rollback
If validation fails, `RESTORE TABLE <table> TO VERSION AS OF <n>` using the
versions captured in the Pre-conditions step returns Bronze to its
pre-refresh (duplicated but previously-working-via-this-PR's-Silver-fix)
state. Because Silver/Gold fully recompute from Bronze on every run, a
Bronze restore is sufficient to also restore correct downstream state on the
next pipeline run — no separate Silver/Gold rollback step is needed.

### Cost and downtime
The affected tables are small (order of 25,000-175,000 rows each); a full
refresh plus pipeline recompute is expected to be of the same order of
magnitude as a single normal promotion job run (observed at roughly 35
minutes end-to-end for job run `388990177883700`, dominated by cluster
startup rather than data volume). No downtime is required: this is a batch
reporting pipeline queried on demand via the SQL warehouse and dashboards,
not a live-serving system, so a brief window of stale Gold data during the
refresh is low-risk. The classic `Standard_F4` cluster used for
`run_lakeflow_pipeline` is billed for its run duration regardless, so this
remains a bounded, known cost — not an open-ended one.

## Explicitly out of scope for this plan
- Personal DEV/PROD cleanup (not independently confirmed to have the same
  duplication in this investigation; the SQL warehouse there was stopped and
  not started solely to check, per the read-only/no-unnecessary-compute
  instruction for this task).
- Any Terraform, permissions, ownership, or compute configuration change.
- Resetting Auto Loader checkpoints for any table not explicitly named above.

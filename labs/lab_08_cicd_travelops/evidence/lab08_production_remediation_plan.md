# LAB 08 - Azure PROD Bronze Duplicate Remediation Plan (NOT EXECUTED)

Prepared 2026-09-19 alongside the payment-idempotency/reconciliation code fix
(`fix/lab08-payment-idempotency-reconciliation`). This is a plan only. No step
in this document has been executed. It requires explicit separate approval,
and even then should be run against Azure PROD only after the code fix in
this PR has been deployed and verified once with a normal (non-refresh) job
run.

## Is a full refresh actually necessary?

**Not for correctness.** `bookings_silver`, `current_bookings_silver` and
`payments_silver` are Lakeflow materialized views: they are fully
recomputed from whatever is currently in Bronze on every pipeline run, not
incrementally. Once this PR's `payments_silver` deduplication and
`current_bookings_silver` booking-quality-filter fix are deployed, the very
next normal (non-refresh) pipeline run will recompute Silver and Gold
correctly from the existing, already-duplicated Bronze tables — the
dedup/filter logic runs fresh every time regardless of how many duplicate
rows Bronze holds. No data loss or reprocessing risk is introduced by simply
deploying and running this fix normally.

**Useful for hygiene, not urgent for correctness.** `bookings_bronze` and
`payments_bronze` currently hold roughly 7x their intended row count
(175,000 raw rows for a 25,000-row logical seed, per
`lab08_photon_fix_and_reconciliation_observation.md`). That bloat costs
extra storage and extra compute on every future Silver recomputation (more
rows to scan and deduplicate), but it no longer produces incorrect Gold
output once this fix is deployed. A full refresh to shrink Bronze back to
its intended size is a cost/hygiene decision, not a data-correctness
requirement.

**Residual risk this PR does not close:** the underlying mechanism (Auto
Loader treating every notebook-overwritten raw Parquet file as new) is not
fixed at the source. Every future `seed_raw_data` + `run_lakeflow_pipeline`
execution will keep adding another ~25,000-30,000 duplicate rows to Bronze,
forever. This PR's Silver-layer fixes keep results correct despite that
growth, but do not stop the growth itself. Whether to accept ever-growing
Bronze tables long-term (relying on periodic full refreshes for hygiene) or
to invest in a source-level fix (e.g. a stable/deterministic raw file name,
or switching Bronze to a full-snapshot batch read instead of incremental
Auto Loader) is an open architectural decision for a separate change.

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

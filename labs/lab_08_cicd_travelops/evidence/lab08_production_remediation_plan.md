# LAB 08 - Azure PROD Bronze Duplicate Remediation Plan (NOT EXECUTED)

Prepared 2026-09-19 alongside the payment-idempotency/reconciliation code fix
(`fix/lab08-payment-idempotency-reconciliation`), updated the same day after
further review found the identical duplication pattern in
`properties_bronze`, `destinations_bronze`, `users_bronze` and
`reviews_bronze` plus a separate join-cardinality bug in
`gold_property_performance`/`gold_destination_performance`, and updated
again after a further architecture review replaced an XOR-based content
fingerprint with an immutable, write-once seed design (see "Ingestion
architecture" below). This is a plan only. No step in this document has
been executed. It requires explicit separate approval, and even then should
be run against Azure PROD only after the code fix in this PR has been
deployed and verified once with a normal (non-refresh) job run.

## Ingestion architecture: immutable, write-once seed

`00_seed_raw_data.ipynb` treats each of the seven raw tables as a fixed,
immutable snapshot of a static sample dataset (`samples.wanderbricks`), not
a versioned or append-only feed — see the notebook's own architecture
decision cell for the full reasoning. An earlier version of this fix used a
`bit_xor(xxhash64(...))` content fingerprint to decide whether to reseed;
that was replaced because XOR cancels in pairs (a dataset with one copy of
a row and a dataset with three copies of that row can hash to the same
fingerprint, silently missing a multiplicity change) and because it was
solving a problem — versioned snapshots — this project does not actually
have. The current design instead uses an explicit, human-controlled
identity (`SEED_VERSION` and `seed_limit`, both requiring a reviewed
code/config change to alter) verified by an honest, limited signal: the
row count encoded in the seed file's own name. A rerun with an unchanged
identity and an unchanged row count is a true no-op (zero filesystem
operations); a rerun under the same identity with a *different* row count
raises an exception rather than silently keeping stale data or silently
writing an incompatible second snapshot. This does not detect a
same-row-count in-place value change — an explicitly accepted limitation,
justified only because the source is a fixed sample dataset. See
`notebooks/00_seed_raw_data.ipynb`'s introduction cell for the full
scenario-by-scenario behavior (first execution, unchanged rerun, changed/
deleted record, legitimate duplicates, interrupted writes, Auto Loader
checkpoints, historical duplicates).

## Scope: all seven Bronze tables, not just bookings and payments

Read-only verification against live Azure PROD found the same ~7x
repeated-ingestion duplication in every Bronze table sourced by
`00_seed_raw_data.ipynb`, not only `bookings_bronze`/`payments_bronze`:

| Table | Total rows | Distinct business-key values | Ratio |
| --- | ---: | ---: | ---: |
| `bookings_bronze` | 175,000 | 25,000 (`booking_id`) | ~7.0x |
| `payments_bronze` | 175,000 | 21,089 distinct `payment_id`; 24,611 distinct full records | ~7.0x (on full records) |
| `properties_bronze` | 127,141 | 18,163 (`property_id`), confirmed a safe 1:1 key | ~7.0x |
| `destinations_bronze` | 294 | 42 (`destination_id`), confirmed a safe 1:1 key | ~7.0x |
| `users_bronze` | 175,000 | 25,000 (`user_id`), confirmed a safe 1:1 key | ~7.0x |
| `reviews_bronze` | 175,000 | 1,000 distinct `review_id`; 24,999 distinct full records | ~7.0x (on full records) |
| `booking_updates_bronze` | 175,000 | 14,397 distinct `booking_id` | not a clean multiple; consistent with content having varied across historical seed runs, as already seen for payments |

`property_id`, `destination_id` and `user_id` were each verified (comparing
distinct full-business-row counts to distinct key counts) to have zero
cross-record reuse, so `properties_silver`, `destinations_silver` and
`users_silver` now deduplicate on that primary key alone.
`review_id` is far less reliable than even `payment_id`: only 1,000 distinct
values exist across 24,999 genuinely distinct review records (roughly 25
different reviews sharing each `review_id` on average), so `reviews_silver`
deduplicates on a full composite key (`REVIEW_BUSINESS_KEY`), the same
pattern as `payments_silver`, not on `review_id` alone.

## A second, independent bug: join-cardinality revenue inflation

Separately from ingestion duplication, `gold_property_performance` and
`gold_destination_performance` join `current_bookings_silver` to
`properties_silver`/`destinations_silver` (and, for destinations, directly
to `reviews_silver`). Before this fix, an undeduplicated dimension table on
either side of a `LEFT JOIN` multiplies the joined booking row once per
matching dimension row, inflating `SUM(booking_amount)` by that factor. This
was verified live and is severe:

```text
true_total_booking_value (current_bookings_silver alone): $13,892,373.75
via_properties_join_value (undeduped properties_silver):   $97,246,616.25  (~7.0x)
via_reviews_direct_join_value (undeduped, un-aggregated):  $96,751,154.93  (~7.0x)
```

This means the `booking_value` figures in `gold_property_performance` and
`gold_destination_performance` — and by extension anything reading them,
including the dashboard — have been overstated by roughly 7x on live Azure
PROD. This is fixed by two independent changes: deduplicating
`properties_silver`/`destinations_silver` on their primary key (removes the
ingestion-duplication half of the inflation), and pre-aggregating
`reviews_silver` to one row per `booking_id` before joining in
`gold_destination_performance` (removes a second, independent fan-out
source: a booking can legitimately have more than one review, which would
have inflated `booking_value` even with zero duplicate ingestion).
`gold_property_performance`/`gold_review_score`'s `review_count` also
changed from `COUNT(DISTINCT review_id)` to a plain row count, since
`review_id` cannot be trusted to distinguish reviews.

**Do not treat `gold_payment_reconciliation` passing as proof that other
financial metrics are correct.** The payment-reconciliation fix and this
join-cardinality fix are independent bugs in different Gold tables; fixing
one says nothing about the other. `gold_daily_booking_revenue` was checked
and is not affected (it reads only from the already-deduplicated
`current_bookings_silver`, with no joins).

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
`notebooks/00_seed_raw_data.ipynb` writes each table's seed once, under an
explicit `(SEED_VERSION, seed_limit)` identity, and skips the write
entirely on a rerun whose row count matches what was recorded for that
identity — instead of letting Spark allocate a fresh, uniquely-named file
on every write. Auto Loader's documented default
`cloudFiles.allowOverwrites=false` means it should not reprocess a path it
has already ingested, so a **rerun whose content is unchanged should stop
adding new duplicate rows to Bronze** going forward — this is a genuine
source-level fix, not only a downstream mitigation, but it is based on
documented Auto Loader behavior and has not been empirically confirmed by
an actual run in this session (see the Validation section below). It does
not retroactively remove Bronze rows already accumulated from runs before
this fix shipped — that cleanup is the full-refresh procedure later in this
document — and by design it never deletes an existing file: if the row
count for an identity ever changes, the notebook raises rather than
deleting the old file or silently adding an incompatible new one, so a
resolution (bumping `SEED_VERSION` and following the full-refresh procedure
below) is always a deliberate, reviewed action, not automatic. Silver-layer
deduplication remains a defense-in-depth backstop for whatever duplication
does still reach Bronze; it does not by itself stop Bronze from
accumulating rows.

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
   confirm `bookings_bronze`/`payments_bronze`/`properties_bronze`/
   `destinations_bronze`/`users_bronze`/`reviews_bronze` row counts in DEV
   do **not** grow between the two runs, and confirm the raw Volume shows
   exactly one `seed-v1-<seed_limit>-<row_count>rows.snappy.parquet` file
   per table (not two). Growth here would mean the immutable-seed design
   did not achieve idempotency as expected, even though Silver/Gold
   correctness would still hold via the dedup backstop.
5. Compare `SUM(booking_amount)` from `current_bookings_silver` alone
   against `gold_property_performance`'s and `gold_destination_performance`'s
   summed `booking_value` (grouped totals should reconcile to the same
   grand total, modulo bookings with a null `property_id`/`destination_id`).
   This directly checks the join-cardinality fix; do not infer it from
   `gold_payment_reconciliation` alone, since that is a different Gold
   table with an independent bug history.
6. Spot-check `gold_property_performance`/`gold_review_score`'s
   `review_count` against a manual `SELECT COUNT(*) FROM reviews_silver
   WHERE property_id = ...` for one property, to confirm it is no longer
   using `COUNT(DISTINCT review_id)` (which would undercount).
7. Confirm that changing `seed_limit` (a deliberate config change) produces
   a new, additional seed file rather than an error, and that the resulting
   row counts for booking-scoped tables (`payments`, `booking_updates`,
   `reviews`) scale with the new sample instead of staying fixed.

### Recovery if the new logic does not appear after a normal run

If step 3 shows the old schema (missing the new health columns) or stale
reconciliation/performance numbers persist after a normal run, the recovery
is a **table-scoped refresh of the specific Silver/Gold materialized
views** (e.g. `payments_silver`, `reviews_silver`, `properties_silver`,
`destinations_silver`, `users_silver`, `current_bookings_silver`,
`gold_payment_reconciliation`, `gold_property_performance`,
`gold_destination_performance`, `gold_review_score`,
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
A full refresh of **all seven affected Azure PROD Bronze tables** in
`dbr_dev.parvinbadalov_lab08_prod`: `bookings_bronze`, `payments_bronze`,
`booking_updates_bronze`, `properties_bronze`, `destinations_bronze`,
`users_bronze` and `reviews_bronze` — all seven were verified in this round
to carry the same ~7x repeated-ingestion duplication (see the Scope table
above), so there is no basis left to exclude any of them from this plan.
Not Personal DEV/PROD, not Terraform, not any other Lab 8 or repository
resource.

### Pre-conditions
1. This PR is merged and deployed, and the pipeline has completed at least
   one normal run so Silver/Gold correctness from the code fix is confirmed
   independent of any Bronze cleanup.
2. A snapshot of current state is captured for rollback comparison: row
   counts and a checksum-style aggregate (e.g. `SELECT COUNT(*), SUM(hash(*))`
   or a Delta table version/timestamp) for all seven Bronze tables, their
   corresponding Silver tables, and `gold_production_health`, plus the Delta
   history (`DESCRIBE HISTORY`) of each affected table so a specific version
   can be identified if needed.
3. Confirm current raw Volume contents for each of the seven `raw/<table>/`
   paths: at the time of this investigation, each contained exactly one
   live `part-*.parquet` data file (old files are removed by each
   `df.write.mode("overwrite")`, only `_committed_*`/`_started_*`
   transaction markers persist) — so a full refresh's Auto Loader
   re-ingestion should pick up only the current seed, not all historical
   duplicate generations. Re-verify this file listing immediately before
   refreshing, since a concurrent seed run would change it, and since this
   PR's notebook fix can leave more than one content-version file present
   going forward (see the Residual risk note above).

### Backups
Delta's built-in time travel is used instead of a separate backup:
`DESCRIBE HISTORY` on each affected table before the refresh gives a
specific version/timestamp that `RESTORE TABLE ... TO VERSION AS OF <n>`
can return to if the refresh produces unexpected results. No external
backup of the raw Volume is needed because the raw seed is fully
reproducible from `samples.wanderbricks` by rerunning
`00_seed_raw_data.ipynb`.

### Execution (not performed)

1. Trigger a Lakeflow full refresh scoped to all seven Bronze tables
   (`bookings_bronze`, `payments_bronze`, `booking_updates_bronze`,
   `properties_bronze`, `destinations_bronze`, `users_bronze`,
   `reviews_bronze` — Lakeflow supports refreshing selected tables rather
   than the whole pipeline). This resets Auto Loader's checkpoint for those
   tables only and reprocesses whatever raw files currently exist under
   their raw paths.
2. Allow the full pipeline update to complete so Silver and Gold recompute
   from the refreshed Bronze tables.

### Validation after refresh
1. Re-run the row-count checks from
   `lab08_photon_fix_and_reconciliation_observation.md` and this plan's
   Scope table for all seven tables: each should drop to match its current
   referentially-consistent seed size (not the previously observed ~7x
   inflated counts).
2. Re-run `01_validate_gold_health.ipynb` (or query
   `gold_production_health` directly) and confirm
   `payment_amount_mismatch_count = 0` and `invalid_booking_amount_count = 0`
   still hold, and that `no_payment_record_count` /
   `pending_or_failed_payment_count` numbers look consistent with a single
   clean seed generation rather than 7x-inflated duplicate-driven noise.
3. Compare `current_booking_count` and the Gold row counts against the
   pre-refresh snapshot from the Pre-conditions step to confirm no
   unexpected data loss.
4. Repeat the join-cardinality reconciliation from step 5 of the DEV
   validation procedure above (`current_bookings_silver`'s
   `SUM(booking_amount)` vs. `gold_property_performance`'s/
   `gold_destination_performance`'s summed `booking_value`) against the
   post-refresh Azure PROD tables.

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

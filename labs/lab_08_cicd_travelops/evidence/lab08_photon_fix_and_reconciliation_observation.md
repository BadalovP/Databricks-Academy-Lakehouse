# LAB 08 - Azure PROD Photon Fix and Payment Reconciliation Observation

Recorded 2026-09-19. Data below was read directly from the live Azure PROD
workspace (job run history and `dbr_dev.parvinbadalov_lab08_prod` tables via
the shared SQL warehouse `3ed106620db591d9`) as read-only verification for
PR #13 and this documentation update. No writes, repairs, or reruns were
performed to produce this evidence.

## PR #13 fix verification

GitHub Actions run `35404466063` (push to `main`) deployed PR #13's
`photon: false` override for the Azure PROD `travelops_pipeline`. Manual job
run `388990177883700` on job `941995669563439` was then used to verify the
fix, since `LAB08_RUN_AZURE_PROD_JOB=false` keeps GitHub Actions from
triggering Azure PROD execution automatically.

Task attempt history for run `388990177883700` (from `databricks jobs get-run`):

| Task | Attempt | Result | Notes |
|---|---:|---|---|
| `seed_raw_data` | 0 | `SUCCESS` | |
| `run_lakeflow_pipeline` | 0 | `FAILED` | `BAD_REQUEST: Standard_F4 is not supported for Photon as worker` |
| `run_lakeflow_pipeline` | 1 | `SUCCESS` | Ran after PR #13's `photon: false` was live |
| `validate_gold_health` | 0 | `SKIPPED` (`UPSTREAM_FAILED`) | Skipped because `run_lakeflow_pipeline` attempt 0 failed |
| `validate_gold_health` | 1 | `SUCCESS` | |

Overall job run result: `SUCCESS`.

## Gold production health snapshot

Queried directly from `dbr_dev.parvinbadalov_lab08_prod.gold_production_health`:

```
current_booking_count = 25000
duplicate_current_booking_count = 0
payment_mismatch_count = 25000
health_passed = true
evaluated_at = 2026-09-18T23:47:13.733Z
```

The health gate in `notebooks/01_validate_gold_health.ipynb` only asserts
`current_booking_count > 0`, `duplicate_current_booking_count == 0`, and
`health_passed` (itself only requiring `payment_mismatch_count >= 0`, which is
always true). `payment_mismatch_count` is informational and does not gate
CI/CD.

## Payment reconciliation investigation (unresolved)

Queried directly from `dbr_dev.parvinbadalov_lab08_prod.gold_payment_reconciliation`:

| `reconciliation_status` | Row count |
|---|---:|
| `missing_payment` | 16,076 |
| `overpaid` | 8,924 |
| `matched` | 0 |
| `underpaid` | 0 |

Sample of `overpaid` rows (`booking_id`, `booking_amount`, `completed_payment_amount`):

| booking_id | booking_amount | completed_payment_amount | ratio |
|---:|---:|---:|---:|
| 22691 | 305.74 | 2140.18 | 7.000 |
| 596 | 511.09 | 3577.63 | 7.001 |
| 2112 | 142.57 | 997.99 | 7.000 |
| 4551 | 747.63 | 5233.41 | 7.000 |
| 13343 | 320.62 | 2244.34 | 7.000 |

Bronze row-count check:

| Table | Total rows | Distinct key values |
|---|---:|---:|
| `bookings_bronze` | 175,000 | 25,000 distinct `booking_id` |
| `payments_bronze` | 175,000 | 21,089 distinct `payment_id` |

`notebooks/00_seed_raw_data.ipynb` seeds a deterministic sample of `25,000`
rows per source table (`seed_limit=25000`), overwriting the raw Parquet
folder on every run. Bronze tables (`pipeline/bronze.py`) are Lakeflow
streaming tables (`@dp.table`) fed by Auto Loader with
`cloudFiles.includeExistingFiles=true`. Because the Azure PROD application
schema is long-lived and has been deployed/run repeatedly over this lab's
history without a Bronze full refresh, the observed row counts (7x the
intended 25,000-row seed) are consistent with Auto Loader re-ingesting the
same deterministic seed content as new files on each historical run.

`current_bookings_silver` (`pipeline/silver.py`) deduplicates to the latest
row per `booking_id` via a `row_number()` window function, which is why
`duplicate_current_booking_count = 0` despite the underlying duplication.
`payments_silver` has no equivalent dedup step, so
`gold_payment_reconciliation`'s `SUM(amount)` over duplicated completed
payments inflates the reconciled total roughly sevenfold wherever a sampled
booking's payment happens to be present, converting genuine matches into
`overpaid`; bookings whose payments were not present in the independently
sampled 25,000-row payment window remain `missing_payment`. Together these
two effects account for all 25,000 reported mismatches.

This is a raw-ingestion idempotency defect, not evidence of real unpaid
bookings, and it predates and is unrelated to the Photon fix in PR #13. It
was not fixed as part of this documentation task. Personal DEV and Personal
PROD share the same Bronze/Silver code and were not independently re-queried
here to avoid starting additional billable compute (their SQL warehouse was
stopped at the time of this investigation); the same defect should be
assumed possible there until checked.

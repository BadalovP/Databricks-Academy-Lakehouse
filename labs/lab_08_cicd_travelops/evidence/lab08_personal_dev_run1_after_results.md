# LAB 08 - TravelOps

## Personal DEV Run 1 (first controlled integration test) — AFTER results

Purpose:
Records the first, single, authorized execution of `travelops_promotion_job`
on `personal_dev` after deploying `fix/lab08-payment-idempotency-reconciliation`
at commit `aa116f6`, for comparison against
`lab08_personal_dev_before_baseline.md`. The job **FAILED** at its final
health-check task; this document captures exactly what happened and why, per
instruction to stop and capture evidence rather than retry, delete files,
reset checkpoints, or modify code.

## Root cause correction (added after this document's original publication)

This document originally concluded that booking_id 13594's $43.00
reconciliation mismatch was a genuine source-data underpayment. A follow-up
read-only investigation, tracing the original `bookings`, `payments` and
`booking_updates` source rows plus Bronze, Silver and Gold for this one
booking, established that conclusion was wrong: the true final source state
for this booking is $775.56, status confirmed, exactly matching its one
completed payment. That is a $0 mismatch in the actual business narrative.

The $818.56 amount that `current_bookings_silver` selected as the booking's
current state was an earlier, superseded update event, picked only because
it and the true final event happened to share an identical `updated_at`
timestamp (and, having been ingested in the same batch, an identical
`_travelops_ingested_at` too), leaving the previous two-column ordering with
no real tie-break and an arbitrary result. This was a pipeline ordering
defect inside `current_bookings_silver`'s window function in
`pipeline/silver.py`, not a source-data or reconciliation-aggregation
defect. It has been fixed by adding the update event's own
`booking_update_id`, a monotonically increasing per-event identity that was
previously dropped before the union, as a tie-break between `updated_at` and
`_travelops_ingested_at`.

All measured figures below are preserved exactly as originally captured;
only the root-cause narrative in the "Gold production health" and
"Assessment" sections has been corrected.

## Run identifiers

- Job run ID: `922281283343187` (job `373683442065271`, run URL:
  `https://dbc-1750318a-76a9.cloud.databricks.com/jobs/373683442065271/runs/922281283343187?o=7474653929863069`)
- Pipeline update ID: `a765e63d-7e92-419a-a457-63dde2c1c9ad` (pipeline
  `99f54e11-d6b2-4361-b0eb-da12b91a31dc`) — state: `COMPLETED`
- Task results:
  - `seed_raw_data`: **SUCCESS**
  - `run_lakeflow_pipeline`: **SUCCESS**
  - `validate_gold_health`: **FAILED** — `AssertionError: Completed payments
    do not reconcile to booking amounts: {...payment_amount_mismatch_count:
    1...}`
- Overall job result: `INTERNAL_ERROR` / `FAILED` (the third task's assertion
  failure caused the job to report failed, per Databricks' job semantics for
  an unhandled exception in a notebook task; no downstream tasks existed
  after it).

## New raw seed files written (one per table, as expected on a first run)

| table | new file | row count |
|---|---|---:|
| bookings | seed-v1-25000-25000rows.snappy.parquet | 25,000 |
| booking_updates | seed-v1-25000-29147rows.snappy.parquet | 29,147 |
| payments | seed-v1-25000-17406rows.snappy.parquet | 17,406 |
| users | seed-v1-25000-25000rows.snappy.parquet | 25,000 |
| properties | seed-v1-25000-18163rows.snappy.parquet | 18,163 |
| reviews | seed-v1-25000-34529rows.snappy.parquet | 34,529 |
| destinations | seed-v1-25000-42rows.snappy.parquet | 42 |

All 7 old-naming-scheme files remained untouched (the design never deletes
existing files); exactly one new file appeared per table, confirming the
immutable write-once identity check worked as designed on a real run.

## BEFORE vs AFTER: Bronze row counts

| table | BEFORE | AFTER | delta | matches new seed file's row count? |
|---|---:|---:|---:|---|
| bookings_bronze | 400,000 | 425,000 | +25,000 | yes |
| booking_updates_bronze | 400,000 | 429,147 | +29,147 | yes |
| payments_bronze | 400,000 | 417,406 | +17,406 | yes |
| users_bronze | 400,000 | 425,000 | +25,000 | yes |
| properties_bronze | 290,608 | 308,771 | +18,163 | yes |
| reviews_bronze | 400,000 | 434,529 | +34,529 | yes |
| destinations_bronze | 672 | 714 | +42 | yes |

Auto Loader ingested **exactly** the one new file per table and nothing else
— no re-ingestion of the 16 historical generations already in Bronze. This
confirms Auto Loader's checkpoint-based incremental processing behaved as
expected under real execution (previously only inferred, not tested).

## BEFORE vs AFTER: Silver counts and duplicate-key groups

| table | BEFORE rows | AFTER rows | BEFORE dup groups | AFTER dup groups |
|---|---:|---:|---:|---:|
| current_bookings_silver | 25,000 | 25,000 | 0 | 0 |
| payments_silver | 393,776 | 31,631 | 24,611 | **0** |
| reviews_silver | 398,112 | 34,350 | 24,882 | **0** |
| properties_silver | 290,608 | 18,163 | 18,163 | **0** |
| destinations_silver | 672 | 42 | 42 | **0** |
| users_silver | 400,000 | 25,000 | 25,000 | **0** |

Every dedup mechanism in this PR (composite business-key dedup for
payments/reviews, primary-key dedup for properties/destinations/users)
collapsed duplicate groups to **zero** on a real Lakeflow run, confirmed
against live data with a real 16x-duplicated Bronze input. `properties_silver`
and `destinations_silver` now match the new seed file's row count exactly
(18,163 and 42); `payments_silver` (31,631) and `reviews_silver` (34,350) are
larger than the new seed alone because they also correctly preserve genuinely
distinct events found across historically differently-sampled runs — this is
expected composite-key dedup behavior, not a defect.

## Gold payment reconciliation (BEFORE: old 2-category schema; AFTER: new 5-state schema)

| status | BEFORE booking_count | AFTER booking_count |
|---|---:|---:|
| matched | n/a (didn't exist) | **15,862** |
| no_payment_record | n/a | 8,957 |
| pending_or_failed_payment | n/a | 180 |
| underpaid | n/a | **1** |
| overpaid | 8,924 | 0 |
| missing_payment (old category) | 16,076 | n/a (replaced) |

Total AFTER: 25,000 (matches `current_bookings_silver`).

## Gold production health

| column | BEFORE | AFTER |
|---|---|---|
| current_booking_count | 25,000 | 25,000 |
| duplicate_current_booking_count | (n/a, old schema) | 0 |
| invalid_booking_amount_count | (n/a) | 0 |
| no_payment_record_count | (n/a) | 8,957 |
| pending_or_failed_payment_count | (n/a) | 180 |
| matched_payment_count | (n/a) | 15,862 |
| payment_amount_mismatch_count | 25,000 (old `payment_mismatch_count`, always-true bug) | **1** |
| health_passed | true (meaningless — old gate never actually checked anything) | **false** (correctly gated on a genuine mismatch) |

**Corrected**: the one mismatch behind `payment_amount_mismatch_count = 1`
is booking_id 13594, which `gold_payment_reconciliation` shows with
`booking_amount = $818.56` against one completed payment of $775.56
(`completed_payment_count = 1`, `payment_record_count = 1`). This was
originally reported here as a genuine $43.00 underpayment in the sampled
source data. A follow-up investigation (see the correction note above)
found that conclusion was wrong: $818.56 is a superseded intermediate
booking-update amount, not this booking's true final state, which is
$775.56 and matches the payment exactly. The defect is in
`current_bookings_silver`'s "latest state" selection, now fixed. The new
5-state reconciliation model and `health_passed` gate still did their job
correctly here: they surfaced a real discrepancy that the old code's
always-true gate could never have detected — the discrepancy was in this
pipeline's own logic rather than in the source data, which the gate alone
cannot distinguish, but is a much better outcome than the old code's silent
"100% mismatch, health_passed=true" behavior.

## Revenue: source total vs. Gold join outputs

| metric | BEFORE | AFTER |
|---|---:|---:|
| source_total (`current_bookings_silver`) | $13,891,898.75 | $13,902,370.7496 |
| `gold_property_performance` total | $222,270,380.00 (~16.0x) | **$13,902,370.7496 (exact match)** |
| `gold_destination_performance` total | $58,223,933,686.27 (~4,190x) | **$13,902,370.7496 (exact match)** |

The join-cardinality fan-out bug is **fully fixed**: all three totals are now
byte-for-byte equal. (The small BEFORE→AFTER source-total difference reflects
the newly-added 25,000-row bookings sample versus the pre-existing
historically-accumulated set, not an error.)

## Assessment

`seed_raw_data` and `run_lakeflow_pipeline` succeeded cleanly on real
Databricks infrastructure, with real 16x-duplicated historical Bronze data as
input. Every dedup and join-cardinality fix in this PR is confirmed correct
against live execution, not just local pure-Python mirrors. The job's overall
`FAILED` status is caused by `validate_gold_health`'s assertion correctly
firing on `payment_amount_mismatch_count = 1`. As corrected above, that one
mismatch was itself caused by a pipeline ordering defect in
`current_bookings_silver`, not by a genuine source-data underpayment as this
document originally concluded. The ordering defect has since been fixed (see
`pipeline/silver.py` and the regression tests added for it); this document's
originally measured numbers are left unchanged as the historical record of
Run 1.

## Warehouse state

Started (authorized) to run these AFTER queries, stopped again immediately
after. Confirmed final state: `STOPPED`.

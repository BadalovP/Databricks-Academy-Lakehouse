# LAB 08 - TravelOps

## Personal DEV BEFORE baseline (read-only, captured 2026-09-19)

Purpose:
Captures the actual state of Personal DEV's deployed Bronze/Silver/Gold tables
*before* deploying PR #15 (`fix/lab08-payment-idempotency-reconciliation`,
commit `043daff`), so the integration test's first and second controlled runs
have real numbers to compare against instead of assumed or invented values.

Method:
All queries below were run read-only against warehouse `410e5f3b4784e981`
("Serverless Starter Warehouse") in `dbr_dev.parvinbadalov_lab08_dev`, via the
Statement Execution API, after explicit authorization to start that warehouse
for this purpose. No data was modified, no job was run, no pipeline was
refreshed, and no checkpoint was touched. The warehouse was stopped again
immediately after these queries completed (see the end of this document).

Environment note:
Personal DEV's currently deployed job/pipeline reflects whatever was last
merged to `main` (only PR #13's Photon fix), **not** this PR's code. These
numbers are the "before" state this PR is designed to fix, not evidence of a
defect in PR #15 itself.

## Bronze row counts

| table | row_count | seed_limit (config) | observed factor |
|---|---:|---:|---:|
| bookings_bronze | 400,000 | 25,000 | 16.0x |
| booking_updates_bronze | 400,000 | (booking-scoped) | n/a |
| payments_bronze | 400,000 | (booking-scoped) | n/a |
| users_bronze | 400,000 | 25,000 | 16.0x |
| properties_bronze | 290,608 | 25,000 | not a clean multiple |
| reviews_bronze | 400,000 | (booking-scoped) | n/a |
| destinations_bronze | 672 | 25,000 | not a clean multiple (small source table) |

**This is a materially larger duplication factor (16x for the row-order-sampled
tables) than the ~7x previously observed and documented for Azure PROD** in
`lab08_production_remediation_plan.md`. Do not assume the two environments
share a duplication factor; they clearly do not. The exact cause of
`properties_bronze`/`destinations_bronze` not being clean multiples of
25,000 was not investigated further here (out of scope for this baseline
capture).

## Silver row counts and business-key/primary-key duplicate groups

| table | row_count | duplicate groups on its dedup key |
|---|---:|---:|
| bookings_silver (no dedup by design; `current_bookings_silver` is the deduplicated table) | 400,000 | n/a |
| current_bookings_silver | 25,000 | 0 (window-function dedup by `booking_id` works correctly even under 16x Bronze duplication) |
| payments_silver | 393,776 | **24,611 groups** still have >1 row on `(payment_id, booking_id, amount, status, payment_date)` |
| reviews_silver | 398,112 | **24,882 groups** still have >1 row on the full `REVIEW_BUSINESS_KEY` |
| properties_silver | 290,608 | **18,163** duplicate `property_id` groups |
| destinations_silver | 672 | **42** duplicate `destination_id` groups |
| users_silver | 400,000 | **25,000** duplicate `user_id` groups (every user_id has duplicates) |

**Interpretation**: the currently-deployed `payments_silver`/`reviews_silver`/
`properties_silver`/`destinations_silver`/`users_silver` show **zero
deduplication effect** — consistent with running code that predates this
PR's composite-key and primary-key dedup fixes entirely. `current_bookings_
silver`'s window-function approach is a different, older mechanism already
present before this PR and is confirmed working correctly here.

## Gold payment reconciliation (current, pre-PR schema)

| reconciliation_status | booking_count | booking_value | paid_value |
|---|---:|---:|---:|
| missing_payment | 16,076 | $8,940,323.04 | $0.00 |
| overpaid | 8,924 | $4,951,575.71 | $79,225,211.36 |

Total: 25,000 (matches `current_bookings_silver`). Note this is the **old**
2-category model, not this PR's 5-state
(`no_payment_record`/`pending_or_failed_payment`/`matched`/`underpaid`/`overpaid`)
model — there is no "matched" category available in the currently-deployed
schema at all.

## Gold production health (current, pre-PR schema)

| current_booking_count | duplicate_current_booking_count | payment_mismatch_count | health_passed |
|---:|---:|---:|---|
| 25,000 | 0 | 25,000 | **true** |

This is a live, concrete reproduction (on Personal DEV, independent of the
original Azure PROD finding) of the exact defect this whole project began
with: `payment_mismatch_count = 25000` (100% of bookings) while
`health_passed` still reports `true`, because the old gate's
`payment_mismatch_count >= 0` condition is always true.

## Revenue: source total vs. Gold join outputs

| metric | value |
|---|---:|
| source_total (`SUM(booking_amount)` from `current_bookings_silver`) | $13,891,898.75 |
| `gold_property_performance` total `booking_value` | $222,270,380.00 (~16.0x source) |
| `gold_destination_performance` total `booking_value` | $58,223,933,686.27 (~4,190x source) |

The source total closely matches the previously Azure-PROD-verified
$13,892,373.75 figure in `lab08_production_remediation_plan.md` (different
environment, same order of magnitude, as expected for the same seed logic).
`gold_property_performance`'s ~16x inflation is consistent with the Bronze
duplication factor measured above. `gold_destination_performance`'s far
larger inflation reflects the currently-deployed (pre-PR) query joining
`reviews_silver` directly without pre-aggregating it to one row per
`booking_id` first, compounding with the dimension-table duplication — this
is exactly the second, independent join-cardinality bug this PR's
`gold.py` fixes.

## Warehouse state

Started (with explicit authorization) for this baseline capture only, and
stopped again immediately afterward. See the session's final report for the
confirmed final state.

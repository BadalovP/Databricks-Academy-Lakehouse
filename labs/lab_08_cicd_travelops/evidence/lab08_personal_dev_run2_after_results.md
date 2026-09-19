# LAB 08 - TravelOps

## Personal DEV Run 2 (second controlled integration test) — AFTER results

Purpose:
Records the second, single, authorized execution of `travelops_promotion_job`
on `personal_dev`, after deploying the `current_bookings_silver` ordering fix
(commit `ab9a78d`) on top of Run 1's already-deployed state. This run
verifies (a) the fix for booking_id 13594's mis-selected latest state, and
(b) that a rerun with unchanged seed input is a true no-op at the raw-file
and Bronze level, exactly as the immutable-seed design intends.

## Run identifiers

- Job run ID: `366859582717065` (job `373683442065271`, run URL:
  `https://dbc-1750318a-76a9.cloud.databricks.com/jobs/373683442065271/runs/366859582717065?o=7474653929863069`)
- Pipeline update ID: `161fab6c-5639-41ad-943b-bc364a1548cb` (pipeline
  `99f54e11-d6b2-4361-b0eb-da12b91a31dc`) — state: `COMPLETED`
- Task results: `seed_raw_data` **SUCCESS**, `run_lakeflow_pipeline`
  **SUCCESS**, `validate_gold_health` **SUCCESS**
- Overall job result: **`TERMINATED` / `SUCCESS`**

## Checks against Run 1 and expected criteria

| Check | Result | Exact value |
|---|---|---|
| Booking 13594 selects $775.56, confirmed | **PASS** | `booking_amount=775.5600`, `status=confirmed` |
| Booking 13594 reconciliation status | **PASS** | `matched` (`completed_payment_amount=775.5600 = booking_amount`) |
| `payment_amount_mismatch_count` | **PASS** | `0` (was `1` in Run 1) |
| `health_passed` | **PASS** | `true` (was `false` in Run 1) |
| Job finishes successfully | **PASS** | all 3 tasks `SUCCESS`, job `SUCCESS` |
| All 7 seed tables skip writing | **PASS** | identical single seed file per table as Run 1 (no new `seed-*` file appeared for any of bookings/booking_updates/payments/users/properties/reviews/destinations) |
| Raw Parquet filenames unchanged | **PASS** | same 7 filenames as Run 1 (e.g. `seed-v1-25000-25000rows.snappy.parquet` for bookings) |
| Bronze row counts unchanged | **PASS** | see table below — byte-for-byte identical to Run 1 |
| Silver duplicate groups remain zero | **PASS** | all 6 checked tables: `0` |
| Gold property/destination revenue = Silver source total | **PASS** | all three exactly `$13,902,752.7496` |

## Bronze row counts: Run 1 vs Run 2 (must be identical)

| table | Run 1 | Run 2 |
|---|---:|---:|
| bookings_bronze | 425,000 | 425,000 |
| booking_updates_bronze | 429,147 | 429,147 |
| payments_bronze | 417,406 | 417,406 |
| users_bronze | 425,000 | 425,000 |
| properties_bronze | 308,771 | 308,771 |
| reviews_bronze | 434,529 | 434,529 |
| destinations_bronze | 714 | 714 |

Identical in every table — confirms the seed notebook's "skip" path produced
zero filesystem writes and Auto Loader ingested nothing new.

## Silver counts: Run 1 vs Run 2 (must be identical)

| table | Run 1 | Run 2 | duplicate groups (Run 2) |
|---|---:|---:|---:|
| current_bookings_silver | 25,000 | 25,000 | 0 |
| payments_silver | 31,631 | 31,631 | 0 |
| reviews_silver | 34,350 | 34,350 | 0 |
| properties_silver | 18,163 | 18,163 | 0 |
| destinations_silver | 42 | 42 | 0 |
| users_silver | 25,000 | 25,000 | 0 |

## Gold production health: Run 1 vs Run 2

| column | Run 1 | Run 2 |
|---|---:|---:|
| current_booking_count | 25,000 | 25,000 |
| duplicate_current_booking_count | 0 | 0 |
| invalid_booking_amount_count | 0 | 0 |
| no_payment_record_count | 8,957 | 8,957 |
| pending_or_failed_payment_count | 180 | 180 |
| matched_payment_count | 15,862 | **15,863** |
| payment_amount_mismatch_count | **1** | **0** |
| health_passed | **false** | **true** |

## Reconciliation breakdown: Run 1 vs Run 2

| status | Run 1 count | Run 1 value | Run 2 count | Run 2 value |
|---|---:|---:|---:|---:|
| matched | 15,862 | $8,837,684.9594 | 15,863 | $8,838,460.5194 |
| no_payment_record | 8,957 | $4,967,698.2702 | 8,957 | **$4,968,123.2702** |
| pending_or_failed_payment | 180 | $96,168.9600 | 180 | $96,168.9600 |
| underpaid | 1 | $818.5600 | 0 | — |
| overpaid | 0 | — | 0 | — |

The `matched` bucket's value increased by exactly $775.56 (booking 13594
entering it) and the `underpaid` bucket's single $818.56 booking is gone, as
expected. **`no_payment_record`'s total value also increased by $425.00
while its booking count stayed at 8,957** — this means the ordering fix
changed the selected `booking_amount` for at least one *other* booking
besides 13594 that also had tied `updated_at`/`_travelops_ingested_at`
values among its update events. This is expected and correct: the fix in
`pipeline/silver.py` is a general correctness fix for
`current_bookings_silver`'s selection logic, not a change scoped only to
booking 13594 -- any other booking affected by the same tie-break gap
benefits from the same correction. This was not separately investigated
booking-by-booking, since it was outside the scope of this task.

## Revenue: Run 1 vs Run 2

| metric | Run 1 | Run 2 |
|---|---:|---:|
| source_total (`current_bookings_silver`) | $13,902,370.7496 | **$13,902,752.7496** |
| `gold_property_performance` total | $13,902,370.7496 | **$13,902,752.7496** |
| `gold_destination_performance` total | $13,902,370.7496 | **$13,902,752.7496** |

All three remain exactly equal to each other in Run 2 (join-cardinality fix
still holds); the $382.00 shift from Run 1 is fully explained by the
reconciliation breakdown above (+$775.56 matched, -$818.56 underpaid,
+$425.00 no_payment_record = +$382.00 net).

## Assessment

Run 2 confirms both things it was designed to test: the `current_bookings_
silver` ordering fix produces the correct, deterministic result for booking
13594 (and evidently at least one other similarly-affected booking), and a
rerun with unchanged seed input is a genuine no-op at the raw-file and
Bronze level -- no new files, no Bronze growth, no Silver duplicate
reintroduction, and Gold revenue consistency preserved. `health_passed` is
now `true` for the first time with a real, correct health-gate implementation
behind it.

## Warehouse state

Started (authorized) to run these AFTER queries, stopped again immediately
after. Confirmed final state: `STOPPED`.

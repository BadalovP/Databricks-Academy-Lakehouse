# Step 3: strict idempotency validation in Personal DEV (2026-10-12): FAIL on one criterion

**Outcome:** 18 of the 19 required criteria pass. **Criterion 16 fails.** The CDC checkpoint advanced from version 1 to 2. Re-merging the unchanged `batch_001` snapshot left an empty, metadata-only commit (v2) on the CDF source, and `federation_cdc` correctly advanced past it.

| Kind of idempotency | Result |
|---|---|
| **Logical**: no new or changed business record | **PASS**. Every business column in every table is identical before and after. |
| **Audit**: earlier ingestion stamps kept | **PASS**. All 40 `ingested_at` and all 48 `retrieved_at` values are identical. |
| **Physical**: no unnecessary writes | **Partial**. 0 rows updated or copied, and 0 data files or bytes added or removed, in all 18 data tables. But each MERGE still wrote a metadata-only Delta commit (19 tables advanced one version), and the CDC checkpoint table rewrote one file. |

## Change under test (Phase A)

Commit `aab6836` makes the shared MERGE change-aware:
- A matched row is updated only `WHEN MATCHED AND NOT (all non-key columns <=>)`.
- Audit stamps (historical `ingested_at`, weather `retrieved_at`) are inserted for new rows but never compared or updated.
- Inserts and the execution-scoped delete are unchanged. The CDC SQL is untouched.

Testing: 741 tests pass locally, including 15 new ones that run the exact predicate in local Spark. Ruff, Black, the dashboard check and the offline bundle schema all pass, and CI is green.

## Deployment (Phase B)

| Check | Result |
|---|---|
| PR #75 / tree | `aab6836`, CI green, clean; `Demos/Demo3` and `labs/` unchanged |
| Pre-deploy plan | All 7 `skip`; no field change, so code sync only |
| Pre / post validators | PASS / PASS; post-plan all `skip`; change-aware code confirmed in the workspace |

## The run (Phase C)

| Item | Value |
|---|---|
| Job / run | `158911626270744` / run `605056708110586`, new token `urbanflow-final-step3-idempotency-20261012-001` |
| Window | 2026-10-12 17:33:48 → 17:41:48 UTC (480 s), `SUCCESS` |
| Parameters | Identical to the successful step 2: same three execution IDs, `cdc_snapshot=batch_001`, `run_full_month` and `run_lakeflow` off |

| Task | Result | Seconds |
|---|---|---|
| `preflight` | SUCCESS | 49 |
| `land_sources` | SUCCESS (all three files `already_landed`) | 43 |
| `station_source_check` | SUCCESS (40 / 40) | 18 |
| `station_silver` | SUCCESS | 62 |
| `station_gold` | SUCCESS | 98 |
| `historical_trips` | SUCCESS | 73 |
| `weather_enrichment` | SUCCESS | 49 |
| `validate_medallion` | SUCCESS (PASS) | 39 |
| `federation_cdc` | SUCCESS: `events {}`, checkpoint 1 → **2**, reconciliation 0 mismatches | 45 |
| `should_refresh_lakeflow` / `refresh_lakeflow` | SUCCESS / EXCLUDED | 1 / 0 |

## Criteria (Phase D)

| # | Criterion | Result |
|---|---|---|
| 1 | Station Bronze 40 rows, 40 unique events | PASS (no new Delta commit) |
| 2 | Historical Bronze 40 unique trips | PASS (no new Delta commit) |
| 3 | Auto Loader checkpoints unchanged | PASS (no checkpoint file added, removed or changed) |
| 4 | No landing file overwritten or duplicated | PASS (landing files identical) |
| 5 | Station Silver 40, business values unchanged | PASS |
| 6 | 40 facts, 40 dimension rows, 13 shortages, 13 priorities | PASS |
| 7 | Historical Silver 40, `ingested_at` preserved | PASS (all 40 timestamps identical: `2026-10-12 16:49:12.413094`) |
| 8 | Historical Gold demand unchanged | PASS (40 trips) |
| 9 | Weather 48 hours, 40 trips, 4 weather-matched | PASS (48 `retrieved_at` values identical) |
| 10 | Quarantine and duplicate counts zero | PASS |
| 11 | Silver and Gold business columns identical | PASS (column-by-column diff of the new version against the pre-run version: no column differs in any data table) |
| 12 | Matched MERGEs report zero business changes | PASS. Every data MERGE: inserted 0, updated 0, deleted 0, copied 0, output rows 0. |
| 13 | `batch_001` creates no new source CDF events | PASS. `table_changes` for v2 = 0 rows; all-time CDF is still exactly 5 inserts at v1. |
| 14 | CDC history exactly 5 distinct events | PASS |
| 15 | Maintenance Silver 5 rows matching `batch_001` | PASS |
| **16** | **CDC checkpoint remains at version 1** | **FAIL: 1 → 2** |
| 17 | CDC reconciliation zero mismatches | PASS (keys and values, both directions) |
| 18 | Every required task succeeded | PASS |
| 19 | No unrelated resource modified | PASS. All 173 other tables unchanged; Demo 3 and the labs untouched; no active runs, no AI Search endpoint. |

## Delta versions and operation metrics

| Table | Before → after | New commit |
|---|---|---|
| `bronze_station_status`, `bronze_historical_trips` | v1 → v1 | none |
| Station Silver, quarantine, duplicates; fact, dimension, daily summary, shortages, priorities; historical Silver, quarantine, duplicates, trip demand | v2 → v3 | MERGE: inserted 0, updated 0, deleted 0, copied 0; files added 0, removed 0; bytes 0 |
| `dim_weather_hourly`, `gold_weather_demand` | v1 → v2 | MERGE, all zero (as above) |
| `station_maintenance_source` (CDF) | v1 → v2 | MERGE, all zero; **0 CDF events** |
| `maintenance_cdc_history`, `silver_station_maintenance` | v1 → v2 | MERGE, all zero |
| `maintenance_cdc_checkpoint` | v1 → v2 | MERGE: 1 row updated (`processed_version` 1 → 2, `processed_at`), 1 file rewritten (1,471 → 2,226 bytes) |

## Why criterion 16 failed, and the proposed fix (not applied)

**Cause:** a Delta MERGE that matches nothing to change still records a commit. In `11_federation_cdc`, `merge_snapshot` therefore created source version 2 even though the snapshot was identical. The notebook then saw `latest_version (2) > checkpoint (1)`, read the empty range 2..2, reconciled at v2 (PASS), and advanced the checkpoint to 2.

This is consistent CDC behaviour, because the checkpoint names the last version processed. But it contradicts the stated requirement, and it adds a commit on every replay.

**Proposed fix, for review before any change** (it touches the CDC notebook's flow, not its MERGE semantics):
1. Before `merge_snapshot`, count the differences between the snapshot view and the current source (`EXCEPT ALL` both ways, which the reconciliation already does).
2. When the count is zero, skip the source MERGE. No source commit is created, `latest_version` stays at 1, no range is read, and the checkpoint does not move.
3. Optionally apply the same "no change, no MERGE" pre-check in `merge_delta_table` and `replace_execution_scope`, so a fully unchanged replay creates no Delta commits at all. That would make physical write avoidance complete, at the cost of one extra comparison read per table.

Machine-readable record, including every before and after snapshot: [`2026-10-12_personal_dev_step3_idempotency.json`](2026-10-12_personal_dev_step3_idempotency.json).

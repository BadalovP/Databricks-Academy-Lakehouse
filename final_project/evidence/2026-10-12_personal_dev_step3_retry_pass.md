# Step 3 retry: strict idempotency after the CDC no-op fix (2026-10-12): PASS

**Outcome:** all 19 criteria pass. The previous attempt ([step 3, 18/19](2026-10-12_personal_dev_step3_idempotency.md)) is kept as history.

| Kind of idempotency | Result |
|---|---|
| **A. Logical**: no new or changed business record | **PASS**. No column differs in any table. |
| **B. Audit**: earlier ingestion stamps kept | **PASS**. All 40 `ingested_at` (`2026-10-12 16:49:12.413094`) and all 48 `retrieved_at` values identical. |
| **C. Physical data writes avoided** | **PASS**. 0 rows updated, copied or deleted, and 0 data files or bytes added or removed, in every table. |
| **D. Metadata-only commits avoided** | **Partial**. CDC (4 tables) and Bronze (2 tables) wrote **no commit at all**. The 16 shared Silver, Gold, historical and weather tables each still wrote **one metadata-only MERGE commit** (all row and file metrics zero). |

## Code under test

Commit `f869bbb`: `11_federation_cdc` compares the incoming snapshot with the current source in both directions (`EXCEPT ALL` over `maintenance_id, station_id, status, severity, updated_at`, NULL-safe). It runs the existing change-aware source MERGE **only when they differ**.

Processing a version range still depends only on `latest_version > checkpoint`, so pending versions from an interrupted run are processed even when the snapshot matches. Reconcile-before-checkpoint and the forward-only checkpoint are unchanged.

The same commit fixes two vacuous "no write" test assertions (literal backspace characters in their regex).

Tests: 755 pass, including 14 new CDC tests (comparison in local Spark, notebook ordering, flow model with the recovery case). Ruff, Black, the dashboard check and the offline bundle schema all pass. CI is green.

## Deployment

| Check | Result |
|---|---|
| PR head / tree / CI | `f869bbb`, clean, green; `Demos/Demo3` and `labs/` unchanged |
| Pre-deploy plan | All 7 `skip`, no field change (code sync only); pre-validator PASS |
| Post-deploy plan | All 7 `skip`; post-validator PASS; guarded CDC code confirmed in the workspace |

## Run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / run `412521206243476`, new token `urbanflow-final-step3-retry-20261012-001` |
| Window | 2026-10-12 19:29:23 → 19:37:01 UTC (458 s), `SUCCESS` |
| Parameters | Identical to step 2 and step 3: same three execution IDs, `batch_001`, full month and Lakeflow off |

| Task | Result | Seconds |
|---|---|---|
| `preflight` | SUCCESS | 49 |
| `land_sources` | SUCCESS (station, trip and weather files `already_landed`) | 39 |
| `station_source_check` | SUCCESS | 18 |
| `station_silver` | SUCCESS | 63 |
| `station_gold` | SUCCESS | 99 |
| `historical_trips` | SUCCESS | 75 |
| `weather_enrichment` | SUCCESS | 46 |
| `validate_medallion` | SUCCESS (PASS) | 37 |
| `federation_cdc` | SUCCESS: **`source_merged: false`**, differences 0 / 0, checkpoint 2 → 2, events none, reconciliation 0 | 29 |
| `should_refresh_lakeflow` / `refresh_lakeflow` | SUCCESS / EXCLUDED | 1 / 0 |

## CDC

| Item | Before | After |
|---|---|---|
| MERGE skip decision | — | **Skipped** (snapshot rows not in source 0, source rows not in snapshot 0) |
| `station_maintenance_source` version | 2 | **2 (no new commit)** |
| CDF events (all time) | 5 inserts at v1 | 5 inserts at v1 (**no new events**) |
| `maintenance_cdc_history` | 5 events, 5 distinct IDs | identical, no new commit |
| `silver_station_maintenance` | 5 rows (= `batch_001`) | identical, no new commit |
| Checkpoint | `processed_version` 2 | **2**, same `processed_at`, no new commit, no rollback |
| Reconciliation (keys and values, both directions) | 0 mismatches | 0 mismatches; no CDF event missing from history |

## Criteria

| # | Criterion | Result |
|---|---|---|
| 1-2 | Station Bronze 40/40, historical Bronze 40/40 | PASS: no new commit |
| 3-4 | Auto Loader checkpoints and landing files unchanged | PASS: identical |
| 5-6 | Station Silver 40; facts 40, dimension 40, shortages 13, priorities 13 | PASS |
| 7 | Historical Silver 40, `ingested_at` preserved | PASS |
| 8 | Historical Gold demand unchanged | PASS (40 trips) |
| 9 | Weather 48 hours, 40 trips, 4 matched; `retrieved_at` preserved | PASS |
| 10 | Quarantine and duplicates 0 | PASS |
| 11 | Business columns identical | PASS: column-by-column diff against the pre-run version is empty for every table |
| 12 | Matched MERGEs report zero changes | PASS: all row metrics 0, files 0 |
| 13 | No new source CDF events | PASS |
| 14-15 | CDC history 5 distinct; maintenance Silver 5 = `batch_001` | PASS |
| 16 | CDC checkpoint stays at its current version (2), no rollback | PASS |
| 17 | CDC reconciliation zero mismatches | PASS |
| 18 | Every required task succeeded | PASS |
| 19 | No unrelated resource changed | PASS: all 173 other tables unchanged; no active run, no AI Search endpoint |

## Delta commits

| Tables | Before → after | Commit |
|---|---|---|
| `bronze_station_status`, `bronze_historical_trips` | v1 → v1 | none |
| `station_maintenance_source`, `maintenance_cdc_history`, `silver_station_maintenance`, `maintenance_cdc_checkpoint` | v2 → v2 | none |
| Station Silver, quarantine, duplicates; fact, dimension, daily summary, shortages, priorities; historical Silver, quarantine, duplicates, trip demand | v3 → v4 | MERGE, metadata-only: all row and file metrics 0 |
| `dim_weather_hourly`, `gold_weather_demand` | v2 → v3 | MERGE, metadata-only: all row and file metrics 0 |

**Remaining limitation (D):** the shared persistence helpers still issue a MERGE for an unchanged input, and a Delta MERGE that changes nothing records an empty commit. Removing it would need the same "compare first, skip when identical" guard in `merge_delta_table` and `replace_execution_scope`, at the cost of one extra comparison read per table. It is not part of this step.

Machine-readable record: [`2026-10-12_personal_dev_step3_retry_pass.json`](2026-10-12_personal_dev_step3_retry_pass.json).

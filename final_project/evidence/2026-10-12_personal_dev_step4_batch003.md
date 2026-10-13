# Step 4C: corrected CDC checkpoint semantics and `batch_003` (2026-10-12): PASS

**Outcome:** the replay of `batch_003`, which has the same business state as `batch_002`, behaved exactly as the corrected design requires:

- no source MERGE and no new source version;
- the version left by the automatic `OPTIMIZE` (4) was scanned with no CDF rows, so no history or Silver MERGE ran;
- reconciliation found 0 mismatches at version 4;
- the checkpoint moved **3 → 4**;
- the newest version with real data changes is still 3.

Only the checkpoint table received a commit.

This closes the anomaly recorded in [`batch_002`](2026-10-12_personal_dev_step4_batch002.md). The first fix (`27c1956`) was reviewed and superseded before it was deployed.

## Corrected design (commit `8220aaa`)

| Concept | Meaning |
|---|---|
| `processed_version` (checkpoint) | The newest source version that was scanned, applied where needed and reconciled. It passes versions without data changes, so it never falls behind the Delta log. |
| `last_data_change_version` | The newest version with CDF events, read from the audit history (reporting only). |
| `plan_scan` | Selects `checkpoint + 1 … latest`. It refuses a range the log no longer holds (`CdcRetentionGapError`), and refuses a source whose version is below the checkpoint (`CdcStateError`). |
| Retention errors while reading | `DELTA_CHANGE_DATA_FILE_NOT_FOUND`, `DELTA_MISSING_CHANGE_DATA` and missing versions or files become `CdcRetentionGapError` with a re-baseline instruction; the history, Silver and the checkpoint stay unchanged. |

The source table has no retention overrides, so the defaults apply: `delta.logRetentionDuration` 30 days and `delta.deletedFileRetentionDuration` 7 days. Predictive optimization is inherited from the metastore.

## Code-only redeploy

| Check | Result |
|---|---|
| PR head / CI | `8220aaa`, CI green; `Demos/Demo3` and `labs/` identical to `main` |
| Pre-deploy plan | All 7 `skip`, no field change; pre-validator PASS |
| Deploy | `Deployment complete!` |
| Post-deploy plan | All 7 `skip`; post-validator PASS |
| Deployed code | Workspace copy of `11_federation_cdc` contains `plan_scan` and `advance_checkpoint(..., processed_version=scan[1])`; `cdc_sql.py` contains `CdcRetentionGapError` |

## Preconditions (read-only, before the run)

Workspace `https://dbc-1750318a-76a9.cloud.databricks.com`:

| Item | State |
|---|---|
| Source | Version 4 (v3 MERGE, v4 OPTIMIZE) |
| Checkpoint | 3 |
| History | 11 rows, 11 distinct IDs, newest version 3 |
| Silver | 5 rows |
| Active runs (both Jobs) | None |
| Pipeline | Idle |
| AI Search endpoints | None |

## Run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / `500336374896739`, token `urbanflow-final-step4-batch003-20261013-001` |
| Window | 2026-10-12 22:45:09 → 22:48:48 UTC (220 s), `SUCCESS`, no retry |
| Parameters | `run_federation_cdc=true`, `cdc_source=fixture_snapshot`, `cdc_snapshot=batch_003`; station, historical, weather, full month and Lakeflow `false` |

| Task | Result |
|---|---|
| `preflight` | PASS, only `run_federation_cdc` enabled |
| `land_sources` | `NOTHING_TO_LAND` |
| `station_source_check` | `SKIPPED` (station branch disabled) |
| `station_silver`, `station_gold`, `historical_trips`, `weather_enrichment` | `DRY_RUN`: no read, no write |
| `validate_medallion` | PASS, no checks |
| `federation_cdc` (58 s) | PASS: `source_merged: false`, differences 0 / 0, `scanned_range` [4, 4], `events` {}, `changes_applied: false`, checkpoint 3 → 4, `last_data_change_version` 3, source 5 = target 5, all mismatch counts 0 |
| `refresh_lakeflow` | `EXCLUDED` |

## Independent verification (read-only SQL after the run)

| Check | Expected | Actual |
|---|---|---|
| Source version | 4 (no new commit) | 4 |
| CDF rows in version 4 | none | none |
| History | 11 rows, 11 distinct, unchanged | 11 / 11 (v1: 5 inserts; v3: 1 insert, 2 pre, 2 post, 1 delete) |
| Silver | 5 rows equal to `batch_003` | equal |
| `MT-1003` | Absent from Silver; delete kept in history | absent; history v1 `insert`, v3 `delete` |
| Checkpoint | 4 | 4, `processed_at` 2026-10-12 22:48:35.201191 |
| Reconciliation at version 4, both directions | 0 | 0; no CDF event missing from history |

## Delta commits

| Table | Before → after | Commit |
|---|---|---|
| `maintenance_cdc_checkpoint` | v3 → v4 | MERGE, 1 row updated (`processed_version`, `processed_at`) |
| `station_maintenance_source`, `silver_station_maintenance`, `maintenance_cdc_history` | unchanged | none |
| The 16 other Final Project tables | unchanged | none |

Retention-gap handling is covered by offline tests only: a vacuumed change range and a cleaned-up log, each failing closed with the checkpoint unchanged. Reproducing it live would need a VACUUM or log cleanup of the source, which was not run.

Machine-readable record: [`2026-10-12_personal_dev_step4_batch003.json`](2026-10-12_personal_dev_step4_batch003.json).

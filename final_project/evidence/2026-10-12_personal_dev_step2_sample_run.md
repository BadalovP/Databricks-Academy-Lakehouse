# Step 2: first Personal DEV platform sample run (2026-10-12): FAILED

**Outcome:** the run failed after 141 seconds on two defects that offline tests could not see. As instructed, it was not rerun or repaired in the workspace. The defects are fixed in code (see [Corrections](#corrections)). Running again needs a redeploy and a new approval.

| Item | Value |
|---|---|
| Job / run | `158911626270744` / run `304828008837365` (idempotency token `urbanflow-final-step2-20261012-001`) |
| Deployed commit | `4adb9b5` (step 1) |
| Window | 2026-10-12 15:18:54 → 15:21:15 UTC (141 s); final state `INTERNAL_ERROR / FAILED` |
| Parameters | `run_station_pipeline`, `run_historical`, `run_weather`, `run_federation_cdc` = `true`; `station_source=sample_autoloader`, `cdc_source=fixture_snapshot`, `cdc_snapshot=batch_001`; `run_full_month` and `run_lakeflow` = `false` |
| **Station execution ID (keep for step 3)** | **`urbanflow-final-304828008837365`** |
| Historical / weather execution IDs | `urbanflow-final-hist-devsample40` / `urbanflow-final-weather-devsample48h` |

Pre-run checks passed:
- correct host and user;
- no schedule or trigger, `max_concurrent_runs` 1;
- no earlier runs and no active runs in the workspace;
- both schemas and the Volume empty;
- no AI Search endpoint, warehouse stopped.

## Tasks

| Task | Result | Seconds | Notes |
|---|---|---|---|
| `preflight` | SUCCESS | 29 | `development_sample` mode on serverless compute, Event Hubs producer excluded |
| `land_sources` | SUCCESS | 72 | 40 station events landed and ingested into Bronze; trip sample landed |
| `station_source_check` | **FAILED** | 21 | `expected 2520, actual 40` rows and distinct event IDs |
| `historical_trips` | **FAILED** | 38 | `dim_station_development_sample does not exist` |
| `station_silver`, `station_gold`, `weather_enrichment`, `validate_medallion`, `federation_cdc`, `should_refresh_lakeflow`, `refresh_lakeflow` | SKIPPED (upstream failed) | 0 | Never started |

## Data the run left (read-only check after the run)

| Table | Rows | Delta versions |
|---|---|---|
| `workspace.urbanflow_final_dev.bronze_station_status` | 40, all execution `urbanflow-final-304828008837365` | 0 CREATE TABLE, 1 STREAMING UPDATE |
| `workspace.urbanflow_final_dev.bronze_historical_trips` | 40 | 0 CREATE TABLE, 1 STREAMING UPDATE |
| Silver, Gold, weather, CDC tables | do not exist | — |
| Lakeflow schema | empty | — |

The Volume holds `landing/station_events/` and `landing/historical_trips/`. **No CDC result exists:** `federation_cdc` was skipped, so there are no CDF events, no maintenance Silver data, no audit history and no checkpoint to verify.

After the run there were no active Job runs, no AI Search endpoint, no Lakeflow update and no clusters. The read-only validation queries started the existing Serverless Starter Warehouse, which auto-stops after 10 minutes.

## Root causes

1. **Demo 3's fixed size.** `09_station_source_check` (and, downstream, `04_bronze_to_silver` and `05_silver_to_gold`) still asserted the fixed size of Demo 3's Event Hubs execution. The Final Project sample lands one event per sample station: 40. The Bronze data was correct; the expectation was wrong.
2. **Missing task dependency.** `historical_trips` joins trips to `dim_station_development_sample`, which `station_gold` writes. The Job ran `historical_trips` in parallel with the station path, straight after `land_sources`, so on a fresh schema the dimension did not exist yet.

## Corrections

These are local code changes, tested; nothing was deployed.

- `landing.station_bronze_failures`:
  - the sample path expects exactly the sample's event count, derived from the committed station-status file;
  - the Event Hubs path requires a non-empty execution with unique event IDs;
  - both paths require zero null keys.
- Notebook 09 now uses that contract and reads `station_source`, and its exit moved to its own cell. Notebooks 04 and 05 assert a non-empty execution instead of that fixed size; notebook 10 already reconciles every layer against the sample profile.
- `historical_trips` now depends on `land_sources` **and** `station_gold`. Preflight refuses a historical run without the station branch when the dimension is absent.
- New tests cover the contracts, the dependency, and a scan that fails if any notebook code holds a fixed Demo 3 count again.
- A read-only `bundle plan` shows the redeploy would **update one resource** (the platform Job) plus the synced notebook files. No create or delete.

Machine-readable record: [`2026-10-12_personal_dev_step2_sample_run.json`](2026-10-12_personal_dev_step2_sample_run.json).

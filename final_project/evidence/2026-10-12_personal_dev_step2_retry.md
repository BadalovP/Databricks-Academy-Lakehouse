# Step 2 retry: Personal DEV platform sample run (2026-10-12): FAILED at weather

**Outcome:** step 2 did **not** fully pass. Six tasks succeeded and their results were verified independently: preflight, landing, the station Bronze contract, station Silver, station Gold and the historical path. `weather_enrichment` then failed on a third defect: its sample file was never landed in the Volume. So `validate_medallion` and `federation_cdc` did not run. As instructed, there was no automatic retry. The first failed attempt is kept in [`2026-10-12_personal_dev_step2_sample_run.md`](2026-10-12_personal_dev_step2_sample_run.md).

**The original 40-row Bronze datasets were preserved without duplication** (see [Bronze preservation](#bronze-preservation)).

## Phase A: redeploy of commit `98cf63e`

| Check | Result |
|---|---|
| PR #75 head / CI / tree | `98cf63e`, green, clean |
| Bronze before redeploy | Station: 40 rows, 40 event IDs, all execution `urbanflow-final-304828008837365`. Historical: 40 rows, 40 ride IDs. Both at Delta version 1. |
| Checkpoints before redeploy | Station Auto Loader checkpoint `checkpoints/urbanflow_final/autoloader/urbanflow-final-304828008837365/` and historical checkpoint `checkpoints/historical_trips/urbanflow-final-hist-devsample40/` intact, each with batch 0 committed |
| Pre-deploy plan | 1 `update` (platform Job), 6 `skip`, 0 create, 0 delete. Real field updates: `historical_trips.depends_on += station_gold`, `station_source_check.base_parameters.station_source`. All other listed fields are `skip` (empty or backend defaults). |
| Pre / post validators | PASS / PASS. Post-deploy plan: all 7 `skip`. |
| Deployed Job | `historical_trips` depends on `land_sources` and `station_gold`; corrected notebook and `landing.py` sources synced |

## Phase B: retry run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / run `896235701785511`, new idempotency token `urbanflow-final-step2-retry-20261012-001` |
| Window | 2026-10-12 16:16:24 → 16:22:28 UTC (363 s); final state `INTERNAL_ERROR / FAILED` |
| Parameters | As approved: original `source_execution_id=urbanflow-final-304828008837365`, sample historical and weather IDs, `cdc_snapshot=batch_001`, `run_full_month` and `run_lakeflow` = `false` |

| Task | Result | Seconds | Verified output |
|---|---|---|---|
| `preflight` | SUCCESS | 41 | `development_sample`, serverless compute |
| `land_sources` | SUCCESS | 42 | Station and historical files `already_landed`; 40 Bronze rows for the execution |
| `station_source_check` | SUCCESS | 18 | 40 rows = 40 distinct event IDs, 0 null keys (sample-derived contract) |
| `station_silver` | SUCCESS | 66 | Silver 40, quarantine 0, duplicates 0 (Bronze 40 = 40 + 0 + 0) |
| `station_gold` | SUCCESS | 99 | Dimension 40; fact 40; daily summary 40; shortages 13 = priorities 13 |
| `historical_trips` | SUCCESS | 78 | Started 16:20:53, right after `station_gold` ended. Valid 40, quarantine 0, duplicates 0; Gold daily demand 40 rows totalling 40 trips |
| `weather_enrichment` | **FAILED** | 16 | `FileNotFoundError: …/landing/weather/open_meteo_archive_202401.sample.json` |
| `validate_medallion`, `federation_cdc`, `should_refresh_lakeflow`, `refresh_lakeflow` | SKIPPED (upstream failed) | 0 | Never started |

The availability composition matches the profile computed independently from the committed sample: 25 available, 4 low bikes, 9 low docks, 2 out of service. Actionable 13 matches shortages and priorities (13 each), and no out-of-service station is in the action lists.

## Delta versions

Each table this run created has an empty schema-creating `CREATE TABLE AS SELECT` at v0, then one `MERGE` at v1:

| Table | v1 MERGE |
|---|---|
| `silver_station_status`, `fact_station_availability`, `gold_daily_station_summary`, `dim_station_development_sample`, `silver_historical_trips`, `gold_daily_trip_demand` | 40 inserted, 0 updated, 0 deleted |
| `gold_station_shortage`, `gold_rebalancing_priority` | 13 inserted |
| `quarantine_*`, `duplicate_*` (station and historical) | 0 rows |

Weather tables (`dim_weather_hourly`, `gold_weather_demand`) and CDC tables were not created.

## Bronze preservation

| Check | Before retry | After retry |
|---|---|---|
| `bronze_station_status` | 40 rows / 40 event IDs (execution `urbanflow-final-304828008837365`), versions 0-1 | **identical**, no new version |
| `bronze_historical_trips` | 40 rows / 40 ride IDs, versions 0-1 | **identical**, no new version |
| Checkpoint, landing and schema files in the Volume | listed | **identical** (no file added, removed or changed) |

Auto Loader reused both checkpoints and found no new files, so neither stream committed a batch.

## Unrelated objects

The metadata of all 173 other tables in the `workspace` and `dbr_dev` catalogs was snapshotted before and after: **none changed**. After the run there were no active runs and no AI Search endpoint. No Lakeflow update, Event Hubs, Zerobus or PROD action happened. The validation queries used the existing Serverless Starter Warehouse, which auto-stops.

## Root cause and correction (not deployed)

`07_weather_enrichment` reads the committed 48-hour Open-Meteo sample from `<volume>/landing/weather/<weather_sample_file>`, but no task landed it there. Demo 3 had uploaded that file by hand.

- `landing.weather_sample_landing` now decides when and where the sample is landed: with `run_weather=true`, `weather_source=sample_json`, outside full-month mode, and only for a safe `.json` name.
- `land_sources` lands it once, refusing different content.
- A test ties the landing path to the exact path notebook 07 reads.
- A scan found no other notebook that reads a Volume file no task lands.
- A read-only plan shows the redeploy is **1 update** (3 new `land_sources` parameters) plus synced files.

Machine-readable record: [`2026-10-12_personal_dev_step2_retry.json`](2026-10-12_personal_dev_step2_retry.json).

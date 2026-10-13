# Step 2: Personal DEV platform sample run, second retry (2026-10-12): PASS

**Outcome:** step 2 passed in full. Every required task succeeded. Each task's own report was read, and every reconciliation was verified independently with read-only SQL against the tables. The original 40-row Bronze datasets were preserved without duplication.

Engineering history of this step:
- [first attempt](2026-10-12_personal_dev_step2_sample_run.md): a fixed Demo 3 count and a missing task dependency;
- [first retry](2026-10-12_personal_dev_step2_retry.md): the weather sample was never landed;
- this run.

## Phase A: redeploy of commit `2a3c88d`

| Check | Result |
|---|---|
| PR #75 head / CI / tree | `2a3c88d`, green, clean; `Demos/Demo3` and `labs/` unchanged |
| Data before | Bronze 40 + 40, Silver 40, fact 40, shortages = priorities = 13, historical 40: identical to the previous verification. Checkpoints intact. |
| Static review of never-run notebooks 07, 10, 11 | Parameters, input paths and the sample assertions (35 member and 5 casual riders, 17 days, 40 dimension matches, 48 hours, weather Gold counting every trip) checked against the fixtures and code |
| Pre-deploy plan | 1 `update` (platform Job), 6 `skip`. The only real changes were the 3 weather parameters on `land_sources`. 0 create, 0 delete, no permission, schedule or compute change. |
| Pre / post validators | PASS / PASS; post-deploy plan all 7 `skip`; corrected landing code synced |

## Phase B: the run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / run `896549012431425`, new idempotency token `urbanflow-final-step2-retry2-20261012-001` |
| Window | 2026-10-12 16:44:12 → 16:52:17 UTC (485 s); final state `TERMINATED / SUCCESS` |
| Execution IDs | Station `urbanflow-final-304828008837365` (original), historical `urbanflow-final-hist-devsample40`, weather `urbanflow-final-weather-devsample48h` |
| Switches | Station, historical, weather and CDC on (`fixture_snapshot`, `batch_001`); `run_full_month` and `run_lakeflow` off |

| Task | Result | Seconds | Task report |
|---|---|---|---|
| `preflight` | SUCCESS | 44 | `development_sample`, serverless |
| `land_sources` | SUCCESS | 41 | Station and trip files `already_landed`; weather sample `landed` at `landing/weather/open_meteo_archive_202401.sample.json` |
| `station_source_check` | SUCCESS | 19 | 40 rows = 40 distinct event IDs, 0 null keys |
| `station_silver` | SUCCESS | 60 | Verified below |
| `station_gold` | SUCCESS | 96 | Verified below |
| `historical_trips` | SUCCESS | 76 | Started after `station_gold` (16:48:35) |
| `weather_enrichment` | SUCCESS | 49 | Verified below |
| `validate_medallion` | SUCCESS | 40 | **PASS**: every station, historical and weather check (listed in the JSON) |
| `federation_cdc` | SUCCESS | 54 | **PASS**: 5 inserts, checkpoint −1 → 1, source 5 = target 5, 0 missing or different, 0 unexpected, 0 duplicate keys, 0 events missing from history |
| `should_refresh_lakeflow` / `refresh_lakeflow` | SUCCESS / EXCLUDED | 1 / 0 | Lakeflow not run (`run_lakeflow=false`) |

## Independent verification

| Area | Result |
|---|---|
| Station | Bronze 40 = Silver 40 + quarantine 0 + duplicates 0; fact 40; dimension 40. Composition 25 available, 4 low bikes, 9 low docks, 2 out of service, equal to the profile computed independently from the committed snapshot. Shortages 13 = priorities 13 = actionable 13; 0 out-of-service rows in either list. |
| Historical | Bronze 40 = valid 40 + 0 + 0; 40 unique ride IDs; 35 member and 5 casual riders over 17 days; Gold daily demand 40 trips |
| Weather | Sample landed (2,698 bytes); `dim_weather_hourly` 48 rows, 48 unique hours (2024-01-24 00:00 → 2024-01-25 23:00), 0 null temperatures. `gold_weather_demand`: 17 day rows, 40 trips, **4 with weather (coverage 0.10)**, matching the 4 trips inside the weather window. The other days are labelled `UNKNOWN` with coverage 0.0, not hidden. |
| CDC | Source `station_maintenance_source` has Change Data Feed on; `table_changes` = 5 `insert` events at version 1; history 5 events with 5 distinct `change_id`s; Silver target 5 rows equal to `batch_001` row for row; checkpoint `station_maintenance` = 1. Re-running the project's reconciliation SQL (keys **and** values, both directions, `VERSION AS OF 1`): 0 / 0 / 0 / 0. No CDF event missing from history. |

## Delta versions

| Table | Versions |
|---|---|
| `bronze_station_status`, `bronze_historical_trips` | v0 create, v1 streaming update (40 rows each), **no new version** |
| Station Silver/Gold, dimension, daily summary, historical Silver, trip demand | v0 CTAS, v1 MERGE (first retry), **v2 MERGE: 0 inserted, rows updated with identical values** |
| Quarantine and duplicate tables | v2 MERGE with 0 rows |
| `dim_weather_hourly` / `gold_weather_demand` | v0 CTAS, v1 MERGE 48 / 17 inserted |
| `station_maintenance_source`, `silver_station_maintenance`, `maintenance_cdc_history` | v0 create, v1 MERGE 5 inserted |
| `maintenance_cdc_checkpoint` | v0 create, v1 MERGE 1 inserted |

**Idempotency:** a column-by-column diff of v2 against v1 found every column identical in seven re-merged tables. In `silver_historical_trips`, only the audit column `ingested_at` changed (40 rows); all business columns are identical. There are no duplicate business rows and no KPI change.

**Finding (not a failure):** these MERGEs rewrite matched rows on a rerun instead of skipping unchanged ones, as the CDC MERGE does. That causes some write amplification and an `ingested_at` refresh; a conditional MERGE would avoid it.

## Bronze preservation and isolation

| Check | Result |
|---|---|
| Station Bronze | 40 rows / 40 event IDs before and after, same Delta history, no new commit |
| Historical Bronze | 40 rows / 40 ride IDs before and after, same Delta history, no new commit |
| Checkpoints | No checkpoint file added, removed or changed (both streams reused their checkpoints) |
| Landing | Only `landing/weather/open_meteo_archive_202401.sample.json` added |
| Unrelated objects | All 173 other tables in `workspace` and `dbr_dev`: unchanged. No active runs, no AI Search endpoint, no Lakeflow update. Demo 3 and the labs were not touched. |

Machine-readable record: [`2026-10-12_personal_dev_step2_pass.json`](2026-10-12_personal_dev_step2_pass.json).

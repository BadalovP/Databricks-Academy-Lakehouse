# Step 9: Azure PROD-style deployment and sample run (Academy trial `dbr_dev_trial_2`, 2026-10-13): PASS

**Outcome:**
- The bundle created exactly the seven reviewed Final Project resources in the Academy **trial** workspace. This is a PROD-style target, not paid production.
- One bounded sample run of the new platform Job succeeded.
- Independent SQL confirms every figure of the Personal DEV sample: station, historical, weather, quality, CDC and reconciliation.
- The shared `dbr_dev` catalog gained only the two named Final Project schemas.

## Preflight (once, immediately before deploying)

| Check | Result |
|---|---|
| Worktree | Clean at `625916f` |
| Workspace / identity | `https://adb-7405618361895520.0.azuredatabricks.net`, `parvinbadalov@softserve.academy` (Azure CLI login) |
| `bundle validate -t azure_prod` | Pass (`BUNDLE_VAR_warehouse_id=d8b8ff9942968cb8`) |
| `bundle plan` | **7 creates, 0 updates, 0 deletes** |
| `validate_release_plan.py --phase pre` | PASS: reviewed resources only, target-prefixed names, no cluster, schedule or trigger, no collision with the 16 existing Jobs |
| Existing objects | No `*_urbanflow_final_*` schema in `dbr_dev` (130 schemas); no UrbanFlow Job; 0 active runs |

## Deployment (08:12:02 → 08:12:40 UTC)

`bundle deploy -t azure_prod --auto-approve --fail-on-active-runs`: complete. Post-deploy plan: all 7 `skip`, validator PASS.

| Resource | Identifier |
|---|---|
| Schema | `dbr_dev.parvinbadalov_urbanflow_final_prod` (owner: the operator) |
| Schema | `dbr_dev.parvinbadalov_urbanflow_final_lakeflow_prod` |
| Volume | `dbr_dev.parvinbadalov_urbanflow_final_prod.urbanflow_final_landing` (MANAGED) |
| Platform Job | `456376825048239`, `[azure_prod] UrbanFlow 2.0 Platform`, 11 tasks, no schedule or trigger |
| RAG Job | `1075074961516662`, `[azure_prod] UrbanFlow 2.0 Mobility RAG`, 3 tasks, never run |
| Lakeflow pipeline | `f5961757-69be-4800-8afe-dae6987d8bc0`, serverless, triggered (`continuous: false`), idle, never updated |
| Dashboard | `01f1c6ddd47616e8bb703a5342a603ba`, warehouse `d8b8ff9942968cb8` |

## Sample run

| Item | Value |
|---|---|
| Run | `240527908342651`, token `urbanflow-final-azure-prod-sample-20261013-001` |
| Window | 08:13:22 → 08:21:15 UTC (**473 s**), `SUCCESS`, no retry |
| Parameters | Station `sample_autoloader`, historical, weather `sample_json` (`open_meteo_archive_202401.sample.json`), CDC `fixture_snapshot` `batch_001`; full month and Lakeflow off |
| Lineage | Station execution `urbanflow-final-240527908342651` (new). Historical and weather use the committed sample IDs, which the preflight requires in sample mode. All of them live in a different workspace, metastore, schema, Volume and checkpoint than Personal DEV; nothing was reused. |

| Task | Seconds | Result |
|---|---|---|
| `preflight` | 44 | PASS; target `dbr_dev.parvinbadalov_urbanflow_final_prod`, serverless |
| `land_sources` | 51 | Station 40 events, trips and weather landed in the Azure Volume; Bronze 40 |
| `station_source_check` | 23 | 40 rows, 40 distinct event IDs, no nulls |
| `station_silver` / `station_gold` | 53 / 82 | SUCCESS |
| `historical_trips` / `weather_enrichment` | 79 / 45 | SUCCESS |
| `validate_medallion` | 40 | PASS: historical 40 = 40 + 0 + 0, 17 days, 35 member / 5 casual, 40 station matches; no unassigned execution IDs |
| `federation_cdc` | 52 | PASS: 5 inserts, scanned [0, 1], checkpoint −1 → 1, source 5 = target 5, 0 mismatches |
| `refresh_lakeflow` | — | `EXCLUDED` |

## Independent verification (read-only SQL on the Azure warehouse)

| Check | Personal DEV (step 2) | Azure trial |
|---|---|---|
| Station Bronze / Silver | 40 / 40 | **40 / 40** |
| Station dimension | 40 | **40** |
| Availability facts / daily summary | 40 / 40 | **40 / 40** |
| Shortages / priorities | 13 / 13 | **13 / 13** |
| Availability (available / low docks / low bikes / out of service) | 25 / 9 / 4 / 2 | **25 / 9 / 4 / 2** |
| Historical Bronze / valid trips | 40 / 40 | **40 / 40** |
| Weather hours (one NYC coordinate) | 48 | **48** |
| Trips with weather | 4 of 40 | **4 of 40** |
| Weather-demand rows | 17 | **17** |
| Quarantine / duplicates (station and trips) | 0 / 0 | **0 / 0** |
| Reconciliation identities (`quality_station`, `quality_monthly`) | true | **true** |
| CDC source: CDF events | 5 inserts at v1 | **5 inserts at v1** (source v0 CREATE, v1 MERGE; no OPTIMIZE commit) |
| CDC Silver / audit history | 5 / 5 | **5 / 5** (5 distinct IDs) |
| CDC checkpoint | −1 → 1 | **−1 → 1** (one MERGE, after reconciliation) |
| CDC reconciliation at the latest version (both directions) | 0 | **0**; no CDF event missing from the history |
| Maintenance dashboard dataset | — | 5 open tickets; MT-1004 HOLD (LOW_DOCKS, COLLECT_BIKES) |

The CDC comparison uses the change feed itself: event counts per commit version, not version numbers.

## Side effects and state after the run

| Check | Result |
|---|---|
| `dbr_dev` schemas | 130 → 132: added only the two Final Project schemas; none removed |
| Workspace Jobs | Added only the two Final Project Jobs; none removed |
| Active runs | None |
| Pipeline | Idle |
| AI Search endpoints | None |
| Warehouse `d8b8ff9942968cb8` | Existing serverless Small; no resize. **Auto-stopped at 08:33:28 UTC** after the verification queries |
| Personal DEV at the same time | Warehouse stopped, no active runs, no AI Search endpoint |

Machine-readable record: [`2026-10-13_azure_prod_step9_deploy_and_sample_run.json`](2026-10-13_azure_prod_step9_deploy_and_sample_run.json).

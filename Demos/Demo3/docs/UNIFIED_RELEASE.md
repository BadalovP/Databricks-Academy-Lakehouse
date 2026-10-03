# UrbanFlow unified Job and manual release

[← Project README](../README.md)

## Current state - deployed, released and run (2026-10-03)

The unified Job is the project's primary orchestrator, and it has been deployed and executed **by
the release workflow**, not by hand. Full machine-readable evidence:
[`evidence/2026-10-03_unified_release.json`](../evidence/2026-10-03_unified_release.json); raw task
reports: [`evidence/unified/`](../evidence/unified/).

| Fact | Value |
|---|---|
| Unified Job | `991496516229387` - `[azure] UrbanFlow End-to-End`, 7 tasks, GP1 only, no schedule |
| Sample run #1 | `4222809815373` - `TERMINATED / SUCCESS`, 7/7 tasks, 471 s |
| Sample run #2 (idempotency) | `284335864579341` - `TERMINATED / SUCCESS`, 7/7 tasks, 389 s |
| Full-month run | `96337578882467` - `TERMINATED / SUCCESS`, 7/7 tasks, 1,082 s |
| Release workflow runs | `37139449737` (sample + repeat) and `37140618864` (full month), both `success` on `7abc194` |
| Component Jobs | **Retired 2026-10-03.** The four `_test` Jobs were deleted through the bundle after a plan asserted to contain exactly those four deletes; definitions kept undeployed in `resources/retired/component_jobs.yml`, workspace definitions and 11 runs in `evidence/2026-10-03_retired_component_jobs.json` |
| Lakeflow, dashboard, governance, maintenance | Completed separately on 2026-10-03 - see the final-depth section below |

**Idempotency is proven from the write reports rather than the green status.** Run #2 inserted 0
rows and removed 0 in every table, migrated no schema, left every count identical, and found no new
historical file to process; both final validations returned identical checks.

### Two failed attempts, and what they found

Neither failure was retried blindly; each was diagnosed from the run output and fixed by PR first.

| Workflow run | Unified run | Failure | Root cause | Fix |
|---|---|---|---|---|
| `37136659149` | `157686710394279` | `06_weather_enrichment`: no trips for the station execution ID | Databricks gives a **job** parameter precedence over a same-named **task** parameter, so the station `source_execution_id` replaced the weather task's historical mapping | PR #64, plus a bundle-wide test that rejects any shadowed task parameter |
| `37138096688` | `295677984549301` | `07_final_validation`: `bronze_historical_trips` has no `execution_id` | Historical Bronze was one shared table, read whole and re-stamped by every execution. The month would have reported 1,888,125 landed rows and counted the 40 sample rides as duplicates | PR #65: one Bronze table per source namespace, and a guard that refuses a second execution in an owned namespace |

PR #65 also fixed a third full-month defect found during the same review: preflight required
`weather_source=open_meteo_archive`, which the weather notebook does not accept.

In both failures every completed task reported zero inserted rows, and the failing task failed
closed before writing, so neither attempt changed any table.

### REAL JANUARY 2024 CITI BIKE MONTHLY ARCHIVE

| Measure | Value |
|---|---|
| Source CSV rows, measured before the run | 1,888,085 |
| Landed in `bronze_historical_trips_202401_full` | **1,888,085** |
| Valid / quarantine / duplicate | **1,886,318 / 1,767 / 0** - reconciles exactly |
| Quarantine reasons | `MISSING_START_STATION_ID` 1,160, `TRIP_TOO_LONG` 607; no row without a reason |
| Distinct, non-null `ride_id` | 1,886,318 / 0 null |
| Rider mix | member 1,678,496, casual 207,822 |
| Ride starts | 2023-12-31 13:50:28 to 2024-01-31 23:58:30 UTC; ends all fall in January |
| Duration (valid) | min 1.02, median 7.73, mean 11.02, max 1,439.55 minutes |
| Daily demand | 32 start days; 64,794 station-day rows in `gold_daily_trip_demand` |
| Station match rate | **3.43%** (64,635 rides) |
| Weather | 744 complete hours from one Open-Meteo archive request; 1,886,318 trips in and out of the LEFT join; 1,885,944 (99.98%) with weather |

The match rate is **dimension coverage, not a data defect**: the station dimension is the committed
40-station development sample, 39 of which appear in the month, against 2,223 distinct start
stations. It is not evidence of renamed or retired stations. The 374 trips without weather are
exactly the 374 rides that started on 31 December, outside the January weather window. Weather is
one city coordinate, not per-station, and comparisons are descriptive, never causal.

The sample's 40 Bronze rows and 40 Silver rows are unchanged, and Silver holds the two executions
side by side under their own IDs.

**Known limitation:** `silver_historical_trips.source_file` is stamped from `_metadata.file_path`
on the Delta Bronze read, so it records a Bronze Parquet file rather than the source CSV. The CSV
file names are recorded in each historical report's `landed_files`.

## DAG and data contracts

The primary DAB resource key is `jobs.urbanflow_end_to_end` and the Azure display name is
`[azure] UrbanFlow End-to-End`.

```text
01_preflight
├── 02_station_source_check → 03_station_silver → 04_station_gold ─┐
└── 05_historical_trips → 06_weather_enrichment ───────────────────┼→ 07_final_validation
                              05_historical_trips ──────────────────┘
```

The station source check is read-only and requires the preserved 2,520-row Bronze execution. The
Silver, Gold, historical and weather tasks reuse notebooks 04 through 07, keeping their tested
business modules as the source of truth. The final validation task is also read-only. It asserts:

- 2,520 Bronze, Silver and fact rows for the station execution;
- 657 shortage and priority rows, with 278 low-bike, 374 low-dock and 5 low-both rows;
- zero actionable `OUT_OF_SERVICE` rows, duplicate business keys, null business IDs and null
  execution lineage;
- 40 valid, zero quarantine, zero duplicate, 40 unique ride IDs, 40 station matches, 35 member,
  5 casual and 17 days in development-sample mode;
- 48 unique complete weather hours and no trip fanout in development-sample mode;
- generic non-empty, uniqueness, reconciliation and no-fanout rules in full-month mode, without
  inventing a monthly row count or requiring a perfect current-station match.

## Release boundary

The ordinary CI workflow stays read-only. Deployment lives in
`.github/workflows/demo3_urbanflow_deploy.yml`, which has only a `workflow_dispatch` trigger and
requires all of these gates:

1. exact confirmation `DEPLOY_AND_RUN_URBANFLOW`;
2. Ruff, Black, pytest and offline bundle schema validation;
3. protected `azure-release-approval` review;
4. Azure OIDC and a pinned Databricks workspace host;
5. read access to the exact existing bundle root and remote deployment metadata;
6. a read-only GP1 check requiring the existing state `RUNNING`;
7. authenticated bundle validation;
8. a JSON pre-plan containing exactly `resources.jobs.urbanflow_end_to_end` and no delete action;
9. an exact-name Job lookup that prevents a missing deployment state from creating a duplicate;
10. `bundle deploy --select jobs.urbanflow_end_to_end` only;
11. an unchanged selected post-plan, Job API read-back and optional run monitoring;
12. `TERMINATED/SUCCESS` for all seven tasks and a JSON `PASS` from final validation.

The workflow never starts, restarts, resizes or terminates GP1. It contains no Event Hubs producer,
pipeline update, SQL warehouse, dashboard, alert, governance or maintenance action.

## Fresh-runner DAB state

Databricks CLI 1.12.1 returns a versioned JSON plan. An already managed resource includes a remote
deployment metadata path such as:

```text
/Workspace/Users/parvinbadalov@softserve.academy/.bundle/demo3_urbanflow/azure/state/metadata.json
```

That stable bundle name, target and `workspace.root_path` are the normal fresh-runner deployment
state mechanism. `databricks bundle deployment bind` is for adopting an already existing workspace
resource; it is not used to create the first unified Job.

The first selected plan may contain one `create`. Before deployment, the workflow queries the exact
display name and refuses the create when such a Job already exists. Later plans may contain
`update` or `skip`; the post-deploy plan must contain one `skip` and expose the managed Job ID. This
guard addresses the earlier failure mode where an identity without the expected deployment state
would otherwise propose duplicate creates.

The directory ACL was a real prerequisite. On 2026-10-03, with explicit approval, the service
principal identified by `AZURE_CLIENT_ID` was granted `CAN_MANAGE` on object `2449501099480664`
only, by an additive update that was read back to confirm the owner and `admins` entries survived.
The workflow performs no permission mutation itself. Removing that one ACL entry is the rollback if
GitHub will no longer manage this bundle.

## Bounded modes

The default mode uses the existing station snapshot, 40-row historical development sample and
48-hour committed weather sample. It makes no external data request and does not publish Event
Hubs messages.

`full_month=true` changes only parameters. Preflight then requires:

- landing subdirectory `202401-full`;
- a new historical execution ID;
- a new weather execution ID;
- the bounded Open-Meteo archive source (`archive_api`) and January 2024 date window.

Monthly mode also raises the Auto Loader wait from the 15-minute sample default to a bounded
45 minutes. The historical task has a 60-minute ceiling, the complete Job has a 90-minute
ceiling, and the GitHub monitor watches for 105 minutes so it can always record the Job's terminal
state. These are failure bounds rather than expected runtimes; they do not start, restart or extend
GP1's lifecycle.

The workflow does not download or upload the Citi Bike archive. Those remain separate approved
actions that must finish before a full-month dispatch.

## Final depth, 2026-10-03

| Item | Result | Evidence |
|---|---|---|
| Lakeflow | Pipeline `fb8a0b8a-cdf8-45c4-bff6-2d117a516fb9`, isolated schema `parvinbadalov_urbanflow_lakeflow`; update `ba6710ed-bd97-46e0-a0c0-616050e3c9b9` COMPLETED; 22/22 expectations; exact business-result parity | `evidence/2026-10-03_lakeflow_run.json` |
| Dashboard | `01f1bf66a828102f9167c26cdd833277`, four pages, bound to this month's executions, no schedule | [DASHBOARD.md](DASHBOARD.md) |
| Governance | Row filter and masks on disposable copies, rolled back, schema dropped | `evidence/2026-10-03_governance_demo.json` |
| Maintenance | Inspection only; VACUUM deliberately never run | `evidence/2026-10-03_maintenance_inspection.json` |
| Component Jobs | Retired; the unified Job is the only UrbanFlow Job | `evidence/2026-10-03_retired_component_jobs.json` |
| Read-only CI after retirement | Workflow `37151678220` SUCCESS, verifying the unified Job and the isolated pipeline | GitHub Actions |
| Final inventory | One Job, one pipeline, two schemas, one Volume, one dashboard, no alert, no policies | `evidence/2026-10-03_final_inventory.json` |

## Final least-privilege state of the CI service principal

| Object | Level | Why it stays |
|---|---|---|
| Unified Job `991496516229387` | `IS_OWNER` | The release workflow created it and must be able to update it |
| Bundle root directory `2449501099480664` | `CAN_MANAGE` | Required to take the deployment lock and write deployment state; removing it breaks every release |
| Lakeflow pipeline | `CAN_VIEW` | Required by the read-only CI check; granted 2026-10-03 by an additive update, owner and `admins` preserved |

Its earlier `CAN_VIEW` on the four component Jobs ended when those Jobs were deleted, so nothing
obsolete remains to revoke. The principal is shared with Lab 8; no Lab 8 resource was modified.

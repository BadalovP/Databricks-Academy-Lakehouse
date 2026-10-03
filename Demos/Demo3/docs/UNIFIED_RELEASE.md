# UrbanFlow unified Job and manual release

[← Project README](../README.md)

## Current state

This document describes the prepared PR A release architecture. It does not claim a deployment or
a Job run.

The read-only inventory on 2026-10-03 found:

| Resource | Actual state |
|---|---|
| GP1 `0702-132442-toro5spu` | `RUNNING`, DBR `17.3.x-scala2.13`, `USER_ISOLATION` |
| Component Jobs | Four known `_test` Jobs, all on GP1 |
| Primary `[azure] UrbanFlow End-to-End` Job | Not deployed |
| Main schema | `dbr_dev.parvinbadalov_urbanflow`, 16 managed Delta tables |
| Landing Volume | `dbr_dev.parvinbadalov_urbanflow.urbanflow_landing` |
| Lakeflow | No deployed UrbanFlow pipeline; isolated output schema does not yet exist |
| AI/BI and SQL alerts | No UrbanFlow dashboard or alert |
| Shared SQL warehouse | `3ed106620db591d9`, `STOPPED`, five-minute auto-stop |
| GitHub OIDC identity | Active service principal `3ec7e8df-66a2-4102-ab57-e4448b4e0e01`; UC access is inherited through `account users` |
| Existing DAB root ACL | Your user and `admins` have `CAN_MANAGE`; the GitHub service principal is not yet listed |

The four component Jobs remain in place. They are evidence-bearing validation resources and are
not cleanup candidates until the unified Job has passed two bounded sample executions.

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

The current directory ACL is a real prerequisite: the GitHub identity is not listed on object
`2449501099480664`. Before the first dispatch, the existing directory owner or an administrator
must grant that one service principal `CAN_MANAGE` on the UrbanFlow Azure bundle root. The workflow
checks the exact directory before planning and performs no permission mutation itself. Removing the
scoped ACL entry is the rollback after the release if GitHub will no longer manage this bundle.

## Bounded modes

The default mode uses the existing station snapshot, 40-row historical development sample and
48-hour committed weather sample. It makes no external data request and does not publish Event
Hubs messages.

`full_month=true` changes only parameters. Preflight then requires:

- landing subdirectory `202401-full`;
- a new historical execution ID;
- a new weather execution ID;
- the bounded Open-Meteo archive source and January 2024 date window.

The workflow does not download or upload the Citi Bike archive. Those remain separate approved
actions that must finish before a full-month dispatch.

## Live approval still required

Creating the unified Job, running it, repeating it for idempotency, downloading or uploading the
monthly archive, running Lakeflow, starting the SQL warehouse, publishing a dashboard, changing
governance or running maintenance all remain behind the consolidated live approval gate. Merging
this preparation changes only repository files.

# Step 1: Personal DEV bundle deployment (2026-10-12)

**Scope:** the bundle definitions only. No Job, pipeline update, AI Search endpoint or warehouse was started. This deployment proves the bundle converges in a real workspace. It does **not** validate any data processing. That is step 2, which needs its own approval.

| Item | Value |
|---|---|
| Commit | `4adb9b566b1123d5e17fabcf5ac14f9a44e7b8c0` |
| Target / workspace | `personal_dev` / `https://dbc-1750318a-76a9.cloud.databricks.com` (workspace `7474653929863069`) |
| Identity | `parvinbadalov@yahoo.com`, the owner's own login (CLI profile `personal_lab10`). No service principal was used or created. |
| Databricks CLI | 1.12.1 |
| Deployed | 2026-10-12, about 14:16 UTC (schemas created 14:15:59 UTC) |

## Checks

| Check | Result |
|---|---|
| Pre-deploy plan | 7 `create`, 0 `update`, 0 `delete` (the reviewed resources only) |
| `validate_release_plan.py --phase pre` | PASS. Only the 7 Final Project resources; no collision with the 20 existing Jobs. |
| `bundle deploy` | `Deployment complete!` (exit 0) |
| Post-deploy plan | All 7 resources `skip` (deployment converged) |
| `validate_release_plan.py --phase post` | PASS |
| `Demos/Demo3` and `labs/` | Unchanged (tree `583e3ab0…`; identical to `main`) |

## Resources created

| Resource | ID |
|---|---|
| Schema | `workspace.urbanflow_final_dev` (owner `parvinbadalov@yahoo.com`, no tables yet, `prevent_destroy`) |
| Schema (Lakeflow) | `workspace.urbanflow_final_lakeflow_dev` (no tables yet, `prevent_destroy`) |
| Volume | `workspace.urbanflow_final_dev.urbanflow_final_landing` (managed, `prevent_destroy`) |
| Job | `158911626270744` `[personal_dev] UrbanFlow 2.0 Platform`: 11 tasks, no schedule, trigger or continuous mode, no classic clusters, `max_concurrent_runs` 1, timeout 7200 s, **0 runs** |
| Job | `887464717518046` `[personal_dev] UrbanFlow 2.0 Mobility RAG`: 3 tasks, no schedule or trigger, `max_concurrent_runs` 1, timeout 5400 s, **0 runs** |
| Pipeline | `d5e0e599-c195-4c08-8217-d554ebf2b301` `[personal_dev] UrbanFlow 2.0 Medallion Pipeline`: serverless, triggered, development, IDLE, **0 updates** |
| Dashboard | `01f1c647728e1ceeb2b5bd10a8700793` `[personal_dev] UrbanFlow 2.0 Operations`: ACTIVE, warehouse `410e5f3b4784e981` |

## Billable activity, before and after

| | Before (14:15:30 UTC) | After (14:16:41 UTC) |
|---|---|---|
| Active Job runs | none | none |
| AI Search endpoints | none | none |
| Serverless Starter Warehouse | STOPPED | STOPPED |
| Running clusters | none | none |

The deployment uploaded files and created definitions. Nothing ran.

Machine-readable record: [`2026-10-12_personal_dev_step1_deploy.json`](2026-10-12_personal_dev_step1_deploy.json).

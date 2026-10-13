# Step 4A: personal_dev redeploy of the reviewed fixes (2026-10-12): PASS

**Outcome:** the plan held exactly the two approved updates, and the post-deploy plan shows every resource unchanged. Nothing was created or deleted, and no compute, permission, schedule or trigger changed. The dashboard definition was updated but not published, and none of its dataset SQL was run.

## Code deployed

Commit `273a301` on `feature/final-project-urbanflow-2`:

- the preflight accepts a CDC-only and a Lakeflow-only run (`PROCESSING_BRANCHES`);
- the dashboard uses the 12-column `GRID_V1` layout.

## Pre-deploy plan

| Resource | Action | Field updates |
|---|---|---|
| `urbanflow_final_platform` (Job) | update | `preflight` base parameters `run_federation_cdc` and `run_lakeflow` (`{{job.parameters.*}}`) |
| `urbanflow_final_dashboard` | update | `serialized_dashboard` (layout `GRID_V1`) |
| `urbanflow_final_rag` (Job), pipeline, 2 schemas, volume | skip | none |

All other listed fields were `skip` (output-only, empty or backend default). The pre-validator found 0 creates and 0 deletes.

## Deploy and post-deploy plan

| Check | Result |
|---|---|
| `bundle deploy -t personal_dev` | `Deployment complete!` |
| Post-deploy plan | All 7 resources `skip`; post-validator `PASS` |
| Job IDs | Platform `158911626270744`, RAG `887464717518046` (unchanged) |
| Dashboard | Definition contains `layoutVersion: GRID_V1`; not published; no dataset query run |
| Schedules, triggers, permissions, compute | Unchanged (no field in the plan) |

Machine-readable record: [`2026-10-12_personal_dev_step4_redeploy.json`](2026-10-12_personal_dev_step4_redeploy.json).

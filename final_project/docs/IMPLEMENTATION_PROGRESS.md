# UrbanFlow 2.0: implementation progress (handoff record)

Keep this file current so another engineer or agent can continue without restarting. Last updated 2026-10-12.

## Repository state

| Item | Value |
|---|---|
| Repository / branch | `BadalovP/Databricks-Academy-Lakehouse`, `feature/final-project-urbanflow-2` |
| Pull request | #75, **draft**, never merged; CI = `final_project_ci.yml` |
| Worktree | `C:\Users\User\Desktop\databricks-academy-lakehouse-final-project` (do not create another) |
| Guarantees | `Demos/Demo3` tree `583e3ab01e37517d62216b44a1e50fa0231b90de` and `labs/` identical to `main`; only `final_project/` and the two `final_project_*` workflows change |
| Local quality | Ruff and Black clean; **763 passed, 1 skipped** (the skip is a bash-only test on Windows); dashboard `--check` current; offline bundle schema valid |

## Milestones

| Milestone | Status | Evidence |
|---|---|---|
| Offline implementation (stages A–H) | Done | `IMPLEMENTATION_CHECKLIST.md`, commit history |
| DEV preflight: 2 deploy blockers fixed (no env interpolation; no schema-renaming dev mode) | Done | `RELEASE_AND_VALIDATION.md` |
| **Step 1** DEV deploy | **PASS** | `evidence/2026-10-12_personal_dev_step1_deploy.md` |
| **Step 2** platform sample run | **PASS** after 2 failed attempts (3 defects fixed) | `evidence/2026-10-12_personal_dev_step2_*.md` |
| **Step 3** strict idempotency | **PASS 19/19** after an 18/19 attempt (CDC no-op guard added) | `evidence/2026-10-12_personal_dev_step3_*.md` |
| Dashboard GRID_V1 layout, table v2, theme (started by Codex) | Done offline; **not redeployed, no live query** | `docs/DASHBOARD.md` |
| Assistant prompt-injection hardening (started by Codex) | Done offline | `tests/test_rag_rag.py` |
| CDC-only platform runs allowed | Done offline; **not deployed** | commit `e63ad0a` |
| Azure `azure_prod` read-only preflight | **PASS** (validate, plan = 7 creates, validator, no collisions); nothing created | `RELEASE_AND_VALIDATION.md` |
| Steps 4–9 | **Not run** (each needs approval) | — |

## Databricks resources

**Personal DEV** (`https://dbc-1750318a-76a9.cloud.databricks.com`, profile `personal_lab10`):

| Resource | ID |
|---|---|
| Platform Job | `158911626270744` |
| Mobility RAG Job | `887464717518046` (never run) |
| Lakeflow pipeline | `d5e0e599-c195-4c08-8217-d554ebf2b301` (never updated) |
| Dashboard | `01f1c647728e1ceeb2b5bd10a8700793` (deployed with the old layout; never queried live) |
| Schemas / Volume | `workspace.urbanflow_final_dev`, `workspace.urbanflow_final_lakeflow_dev`, `workspace.urbanflow_final_dev.urbanflow_final_landing` (all `prevent_destroy`) |
| SQL warehouse (existing) | `Serverless Starter Warehouse` `410e5f3b4784e981`, auto-stop 10 min |

**Azure trial** (`https://adb-7405618361895520.0.azuredatabricks.net`, `dbr_dev_trial_2`, Azure CLI auth, user `parvinbadalov@softserve.academy`): nothing deployed. Warehouse `d8b8ff9942968cb8` (serverless Small, stopped). This is an Academy **trial** workspace, not paid production.

## Live run IDs (DEV)

| Run | Purpose | Result |
|---|---|---|
| `304828008837365` | Step 2, first attempt | FAILED (Demo 3 fixed count; missing dependency) |
| `896235701785511` | Step 2, retry | FAILED at weather (sample never landed) |
| `896549012431425` | Step 2, second retry | **PASS** |
| `605056708110586` | Step 3 | 18/19 (CDC checkpoint 1 → 2 over an empty commit) |
| `412521206243476` | Step 3, retry | **PASS 19/19** |

Execution IDs: station `urbanflow-final-304828008837365`, historical `urbanflow-final-hist-devsample40`, weather `urbanflow-final-weather-devsample48h`.

## Current DEV data state (verified after `412521206243476`)

Bronze 40 + 40; station Silver 40, facts 40, dimension 40, shortages 13, priorities 13; historical 40; weather 48 hours with 4 matched trips; quarantine and duplicates 0.

CDC: source v2 (5 rows, `batch_001`), history 5 events, Silver 5, **checkpoint 2**. Lakeflow, RAG and Zerobus tables do not exist.

## Known limitations and defects

- **Metadata-only commits.** The 16 shared Silver, Gold, historical and weather tables still record one metadata-only MERGE commit per replay. No data or file changes; this is a performance limitation, not a correctness issue.
- **Federation.** The Lab 10 foreign catalog (`neon_postgres_conn_catalog`, read-only PostgreSQL connection) has no maintenance table, so CDC runs on committed fixture snapshots. A real federated source needs a Final-Project table in an external database, which is an external write needing approval.
- **Governance.** Only one identity is available, so multi-principal row-filter and mask enforcement cannot be demonstrated. The demo shows filtered vs allowed for one principal by changing a mapping row.
- **CD workflow.** It has never run: it must be on `main` for `workflow_dispatch`, and needs the `final-project-*` environments and identities.
- **Dashboard.** The 12-column layout matches the workspace's own `GRID_V1` dashboards, but no dataset has been queried live and rendering is unverified.

## Active cost-bearing resources

None running. No AI Search endpoint exists, and the warehouses are stopped (auto-stop 10 minutes). Jobs and the pipeline bill only when run.

## Pending, each needing explicit approval

See the consolidated proposal in `RELEASE_AND_VALIDATION.md` ("Next approval request"):

1. **Step 4:** redeploy plus CDC `batch_002` then `batch_003`.
2. **Step 5:** one Lakeflow update.
3. **Dashboard:** redeploy plus read-only dataset query validation.
4. **Steps 6–8:** assistant ingest, AI Search endpoint/index, smoke test, evaluation, then delete the endpoint.
5. **Governance:** the demo in an isolated schema.
6. **PROD:** deploy to `azure_prod`, which writes to the shared `dbr_dev` catalog.
7. **Optional:** full month, Zerobus, real Federation source.

## Next steps without approval

Keep the docs synchronized with the evidence. Optional work: extend the compare-first guard to the shared MERGE helpers, to remove metadata-only commits.

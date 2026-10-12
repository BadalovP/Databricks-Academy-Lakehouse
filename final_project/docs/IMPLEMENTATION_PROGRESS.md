# UrbanFlow 2.0: implementation progress (handoff record)

Keep this file current so another engineer or agent can continue without restarting. Last updated 2026-10-13.

## Repository state

| Item | Value |
|---|---|
| Repository / branch | `BadalovP/Databricks-Academy-Lakehouse`, `feature/final-project-urbanflow-2` |
| Pull request | #75, **draft**, never merged; CI = `final_project_ci.yml` |
| Worktree | `C:\Users\User\Desktop\databricks-academy-lakehouse-final-project` (do not create another) |
| Guarantees | `Demos/Demo3` tree `583e3ab01e37517d62216b44a1e50fa0231b90de` and `labs/` identical to `main`; only `final_project/` and the two `final_project_*` workflows change |
| Deployed to Personal DEV | Commit `8220aaa` (step 4C redeploy): CDC logic, Jobs and pipeline as in the repository. Changed since, **not deployed:** the dashboard maintenance datasets (`937b968`, redeploy is the first step of phase D) and the wording of two CDC error messages (`02f515d`, no behaviour change). |
| Presentation | Slides artifact *UrbanFlow 2.0 Demo Day* (private, owner only; 14 slides with notes; exports to .pptx), script in `DEMO_DAY_PRESENTATION.md` |
| Local quality | Ruff, Black and the secret scan clean; **844 passed, 2 skipped**. The skips are a bash-only test on Windows and the Databricks-only Lakeflow event-log dataset. Dashboard `--check` is current; offline bundle schema valid; CI green on PR #75. |

## Milestones

| Milestone | Status | Evidence |
|---|---|---|
| Offline implementation (stages A–H) | Done | `IMPLEMENTATION_CHECKLIST.md`, commit history |
| DEV preflight: 2 deploy blockers fixed (no env interpolation; no schema-renaming dev mode) | Done | `RELEASE_AND_VALIDATION.md` |
| **Step 1** DEV deploy | **PASS** | `evidence/2026-10-12_personal_dev_step1_deploy.md` |
| **Step 2** platform sample run | **PASS** after 2 failed attempts (3 defects fixed) | `evidence/2026-10-12_personal_dev_step2_*.md` |
| **Step 3** strict idempotency | **PASS 19/19** after an 18/19 attempt (CDC no-op guard added) | `evidence/2026-10-12_personal_dev_step3_*.md` |
| Dashboard GRID_V1 layout, table v2, theme (started by Codex) | Deployed in step 4A; **not published, no live query** | `docs/DASHBOARD.md` |
| Assistant prompt-injection hardening (started by Codex) | Done offline | `tests/test_rag_rag.py` |
| CDC-only and Lakeflow-only platform runs allowed | Deployed in step 4A; CDC-only run proven by `343513312512955` | commits `e63ad0a`, `e4b256d` |
| Dashboard SQL analysed against live schemas (24/25 datasets) | Done offline | `tests/test_dashboard_sql_analysis.py` |
| Offline RAG evaluation re-run | Reproduces correctness 0.648 (baseline 0.111), recall 0.938, coverage 0.875 (fixture, not model quality) | `urbanflow-rag evaluate` |
| Operations runbook, approval request, interview demo | Done | `OPERATIONS_RUNBOOK.md`, `RELEASE_AND_VALIDATION.md`, `DEMO_DAY_PRESENTATION.md` |
| Azure `azure_prod` read-only preflight | **PASS** (validate, plan = 7 creates, validator, no collisions); nothing created | `RELEASE_AND_VALIDATION.md` |
| **Step 4A** redeploy (preflight branches, dashboard `GRID_V1`) | **PASS**: 2 updates, post-plan all `skip`; dashboard not published | `evidence/2026-10-12_personal_dev_step4_redeploy.md` |
| **Step 4B** CDC `batch_002` | **CDC PASS; stopped**: automatic `OPTIMIZE` made source v4 (no data). First fix `27c1956` kept the checkpoint behind `OPTIMIZE` versions. It was reviewed, superseded before deploy, and replaced by scan-and-pass semantics with a fail-closed retention guard | `evidence/2026-10-12_personal_dev_step4_batch002.md` |
| CDC checkpoint semantics corrected (`8220aaa`) | Checkpoint = newest scanned and reconciled version; OPTIMIZE-only ranges pass with no MERGE; retention gap fails closed | `docs/ARCHITECTURE.md`, `tests/test_cdc_pipeline.py` |
| **Step 4C** redeploy + CDC `batch_003` | **PASS**: no events, checkpoint 3 → 4 over the `OPTIMIZE` version, reconciliation 0 | `evidence/2026-10-12_personal_dev_step4_batch003.md` |
| **Step 5** Lakeflow | **PASS**: update `a26b4385` COMPLETED; all counts as expected; 22 expectations 0 failed; row-for-row parity | `evidence/2026-10-12_personal_dev_step5_lakeflow.md` |
| CI/CD hardening (Lab 08 comparison) | CD: plan-only mode, approval after a reviewable plan, re-plan before deploy, cost warnings, timeouts, evidence artifact, step summary, run cancellation. CI: secret scan, change-scope job | `.github/workflows/final_project_*.yml`, `docs/RELEASE_AND_VALIDATION.md` |
| Dashboard maintenance review (offline) | Open tickets all listed with HOLD notes; `maintenance_hold` on priorities; checkpoint vs last data change; dimension schema added to the live-schema fixture; behavioural tests. **Not redeployed** (needed before the live check) | `docs/DASHBOARD.md` |
| Steps 6–9 | **Not run** (each needs approval) | — |

## Databricks resources

**Personal DEV** (`https://dbc-1750318a-76a9.cloud.databricks.com`, profile `personal_lab10`):

| Resource | ID |
|---|---|
| Platform Job | `158911626270744` |
| Mobility RAG Job | `887464717518046` (never run) |
| Lakeflow pipeline | `d5e0e599-c195-4c08-8217-d554ebf2b301` (one update: `a26b4385-f0b8-454a-9aad-d9a3424151c7`, COMPLETED; idle) |
| Dashboard | `01f1c647728e1ceeb2b5bd10a8700793` (`GRID_V1` definition deployed in step 4A; not published; never queried live) |
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
| `343513312512955` | Step 4, CDC `batch_002` (CDC only) | CDC PASS; extra automatic `OPTIMIZE` version; stopped before `batch_003` |
| `500336374896739` | Step 4C, CDC `batch_003` (CDC only) | **PASS**; checkpoint 3 → 4 |
| `441981394008476` | Step 5, Lakeflow only | **PASS**; update `a26b4385-f0b8-454a-9aad-d9a3424151c7` |

Execution IDs: station `urbanflow-final-304828008837365`, historical `urbanflow-final-hist-devsample40`, weather `urbanflow-final-weather-devsample48h`.

## Current DEV data state (verified after `441981394008476`)

Bronze 40 + 40; station Silver 40, facts 40, dimension 40, shortages 13, priorities 13; historical 40; weather 48 hours with 4 matched trips; quarantine and duplicates 0.

CDC: source **v4** (v3 MERGE of `batch_002`; v4 automatic `OPTIMIZE`, no data change), 5 rows. History 11 events (11 distinct). Silver 5 = `batch_002` = `batch_003` (`MT-1003` deleted). **Checkpoint 4**; last data change version 3. The 16 other main-schema tables are unchanged since step 3.

Lakeflow schema `urbanflow_final_lakeflow_dev`: 10 datasets from update `a26b4385`. Bronze 40, Silver 40, quarantine 0, duplicates 0, dimension 40, fact 40, summary 40, shortages 13, priorities 13. RAG and Zerobus tables do not exist.

## Known limitations and defects

- **Non-data Delta commits in CDC.** Databricks may commit an `OPTIMIZE` (automatic or predictive optimization) on the CDF source. The checkpoint now means the newest scanned and reconciled version: `OPTIMIZE`-only ranges are scanned with no MERGE, and the checkpoint passes them. A range the Delta log or VACUUM no longer holds fails closed (`CdcRetentionGapError`, recovery in the runbook). This retention path is tested offline only; reproducing it live would need a VACUUM of the source.
- **Metadata-only commits.** The 16 shared Silver, Gold, historical and weather tables still record one metadata-only MERGE commit per replay. No data or file changes; this is a performance limitation, not a correctness issue.
- **Federation.** The Lab 10 foreign catalog (`neon_postgres_conn_catalog`, read-only PostgreSQL connection) has no maintenance table, so CDC runs on committed fixture snapshots. A real federated source needs a Final-Project table in an external database, which is an external write needing approval.
- **Governance.** Only one identity is available, so multi-principal row-filter and mask enforcement cannot be demonstrated. The demo shows filtered vs allowed for one principal by changing a mapping row.
- **CD workflow.** It has never run: it must be on `main` for `workflow_dispatch`, and needs the `final-project-*` environments and identities.
- **Dashboard.** The 12-column layout matches the workspace's own `GRID_V1` dashboards, but no dataset has been queried live and rendering is unverified.

## Active cost-bearing resources

None running. No AI Search endpoint exists, and the warehouses are stopped (auto-stop 10 minutes). Jobs and the pipeline bill only when run.

## Pending, each needing explicit approval

The consolidated proposal for phases D–H is in `RELEASE_AND_VALIDATION.md` ("Next approval request"), with workspace, resources, compute, duration, cost, verification, cleanup and shared effects per phase. Recommended order: E1 (assistant ingest) → D (dashboard redeploy, live queries, rendering) → E2–E6 (AI Search, smoke test, evaluation, **mandatory endpoint deletion**) → F (governance demo) → G (Azure trial deploy, shared `dbr_dev` catalog). H is optional (Federation source, Event Hubs, Zerobus, full month).

## Next steps without approval

Keep the docs synchronized with the evidence. Optional work: extend the compare-first guard to the shared MERGE helpers, to remove metadata-only commits.

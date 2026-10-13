# UrbanFlow 2.0: implementation progress (handoff record)

Keep this file current so another engineer or agent can continue without restarting. Last updated 2026-10-13 (PR #75 merged; operational closeout in progress).

## Repository state

| Item | Value |
|---|---|
| Repository / branch | `BadalovP/Databricks-Academy-Lakehouse`, `main` at merge commit `225545fd1f54607d23996c58daee7ef7e1c924fd` |
| Pull request | [#75](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/75), **merged** 2026-10-13; final CI green |
| Worktree | `C:\Users\User\Desktop\databricks-academy-lakehouse-final-project` (isolated closeout worktree; preserve the original folder and its untracked Terraform file) |
| Guarantees | Protected `Demos/Demo3` tree `583e3ab01e37517d62216b44a1e50fa0231b90de`; Labs 1–12, Demo 3 and shared Terraform are outside the closeout scope |
| Deployed to Personal DEV | The completed dashboard maintenance review and title fix were deployed; all 25 datasets passed and the dashboard was published owner-only. The merged source at `225545f` contains the validated release. |
| Presentation | Slides artifact *UrbanFlow 2.0 Demo Day* (private, owner only; 14 slides with notes; exports to .pptx), script in `DEMO_DAY_PRESENTATION.md` |
| Local quality | Ruff, Black and the secret scan clean; **858 passed, 2 skipped**. The skips are a bash-only test on Windows and the Databricks-only Lakeflow event-log dataset. Dashboard `--check` is current; offline bundle schema valid; CI green on PR #75. |

## Milestones

| Milestone | Status | Evidence |
|---|---|---|
| Offline implementation (stages A–H) | Done | `IMPLEMENTATION_CHECKLIST.md`, commit history |
| DEV preflight: 2 deploy blockers fixed (no env interpolation; no schema-renaming dev mode) | Done | `RELEASE_AND_VALIDATION.md` |
| **Step 1** DEV deploy | **PASS** | `evidence/2026-10-12_personal_dev_step1_deploy.md` |
| **Step 2** platform sample run | **PASS** after 2 failed attempts (3 defects fixed) | `evidence/2026-10-12_personal_dev_step2_*.md` |
| **Step 3** strict idempotency | **PASS 19/19** after an 18/19 attempt (CDC no-op guard added) | `evidence/2026-10-12_personal_dev_step3_*.md` |
| Dashboard GRID_V1 layout, table v2, theme (started by Codex) | **PASS**: deployed, 25/25 datasets queried, title fix verified, published owner-only | `docs/DASHBOARD.md` |
| Assistant prompt-injection hardening (started by Codex) | Done offline | `tests/test_rag_rag.py` |
| CDC-only and Lakeflow-only platform runs allowed | Deployed in step 4A; CDC-only run proven by `343513312512955` | commits `e63ad0a`, `e4b256d` |
| Dashboard SQL analysed against live schemas (24/25 datasets) | Done offline; all 25 datasets later passed live | `tests/test_dashboard_sql_analysis.py`, `evidence/2026-10-13_personal_dev_dashboard_validation.md` |
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
| **E1** assistant ingest | **PASS** (run `870039412110159`) | `evidence/2026-10-13_personal_dev_e1_rag_ingestion.md` |
| **E2–E4** AI Search + smoke test | **PASS** (run `12773923439415`; cleanup guard armed first) | `..._e2_e4_ai_search_smoke.md` |
| **E5** live evaluation | **PASS**: RAG 0.685 vs baseline 0.148, recall 1.0, refusals 18/18 | `..._e5_rag_evaluation.md` |
| **E6** endpoint cleanup | **PASS**: deleted after 25.7 min, none remains | `..._e6_ai_search_cleanup.md` |
| **D** dashboard | **PASS, 25/25 queries**; title defect fixed and deployed (`625916f`); published to the owner only | `..._dashboard_validation.md`, `..._dashboard_title_fix.md` |
| **F** governance demo | **PASS**, cleaned up; one identity | `..._governance_demo.md` |
| Phase G read-only re-check | PASS (7 creates, validator, no collision) | `evidence/2026-10-13_azure_prod_phase_g_preflight.json` |
| **Step 9** Azure PROD-style deploy + sample run | **PASS**: 7 resources; run `240527908342651` matches DEV on every figure | `evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md` |

## Databricks resources

**Personal DEV** (`https://dbc-1750318a-76a9.cloud.databricks.com`, profile `personal_lab10`):

| Resource | ID |
|---|---|
| Platform Job | `158911626270744` |
| Mobility RAG Job | `887464717518046` (runs `870039412110159`, `12773923439415`, `712590508231976`) |
| Lakeflow pipeline | `d5e0e599-c195-4c08-8217-d554ebf2b301` (one update: `a26b4385-f0b8-454a-9aad-d9a3424151c7`, COMPLETED; idle) |
| Dashboard | `01f1c647728e1ceeb2b5bd10a8700793` (title fix deployed at `625916f`; 25/25 datasets queried live; published, owner only, no embedded credentials) |
| AI Search | None. `urbanflow-final-dev` existed 00:09:15–00:34:59 UTC on 2026-10-13 and was deleted; the temporary guard Job `331156530436117` was deleted too |
| Schemas / Volume | `workspace.urbanflow_final_dev`, `workspace.urbanflow_final_lakeflow_dev`, `workspace.urbanflow_final_dev.urbanflow_final_landing` (all `prevent_destroy`) |
| SQL warehouse (existing) | `Serverless Starter Warehouse` `410e5f3b4784e981`, auto-stop 10 min |

**Azure trial** (`https://adb-7405618361895520.0.azuredatabricks.net`, `dbr_dev_trial_2`, Azure CLI auth, user `parvinbadalov@softserve.academy`), deployed 2026-10-13 at `625916f`:
- platform Job `456376825048239` (run `240527908342651`: PASS);
- RAG Job `1075074961516662` (never run);
- pipeline `f5961757-69be-4800-8afe-dae6987d8bc0` (never updated);
- dashboard `01f1c6ddd47616e8bb703a5342a603ba`;
- schemas `dbr_dev.parvinbadalov_urbanflow_final_prod` and `..._final_lakeflow_prod`, Volume `urbanflow_final_landing`. Warehouse `d8b8ff9942968cb8` (serverless Small, stopped). This is an Academy **trial** workspace, not paid production.

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
- **CD workflow.** The environments and the read-only DEV service principal `sp-urbanflow-final-dev` are configured, and the bundle state paths are pinned so the principal reads the existing state. The first dispatch (`38382998636`) failed on a missing `-t` flag before any Databricks call. That is fixed, and the corrected dispatch is recorded in `CI_CD.md`. The Azure OIDC path stays blocked by Academy identity setup.
- **Dashboard.** All 25 datasets passed live, the title-rendering defect was fixed, and the dashboard was published with embedded credentials off and owner-only access.

## Active cost-bearing resources

None running. No AI Search endpoint exists, and the warehouses are stopped (auto-stop 10 minutes). Jobs and the pipeline bill only when run.

## Pending, each needing explicit approval

Phases D, E, F and G are done. Only optional items remain:
- a GitHub CD run, once the environments and identities exist (`CI_CD.md`);
- the external Federation source, Event Hubs, Zerobus and the full month (phase H in `RELEASE_AND_VALIDATION.md`);
- a multi-principal governance test, which needs a second identity.

## Next steps without approval

Keep the docs synchronized with the evidence. Optional work: extend the compare-first guard to the shared MERGE helpers, to remove metadata-only commits.

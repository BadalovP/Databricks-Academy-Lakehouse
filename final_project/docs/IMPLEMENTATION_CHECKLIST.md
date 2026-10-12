# UrbanFlow 2.0 - implementation checklist

Working log for finishing the Final Project from the Codex handoff. Worktree:
`C:\Users\User\Desktop\databricks-academy-lakehouse-final-project`, branch
`feature/final-project-urbanflow-2`, based on `origin/main` `15a2902`.

## Handoff baseline (verified 2026-10-12)

- Codex work preserved in local commit `e2af127` (157 new files + README; nothing outside `final_project/`).
- `Demos/Demo3` tree = `583e3ab01e37517d62216b44a1e50fa0231b90de`, no working changes. Re-check before every commit.
- Composition: 92 files copied unchanged from Demo 3, 10 adapted copies, 56 new source files.
- First local test run: 492 passed, 89 failed, 20 errors. Root causes:
  1. fixtures moved `data/samples` -> `fixtures/samples`, tests and code still use the old path;
  2. tests still target Demo 3 orchestration (`resources/jobs.yml`, `demo3_urbanflow_deploy.yml`);
  3. new notebooks (11, 20-23) miss the repository's notebook documentation contract;
  4. tests expect Demo 3's GP1 shared cluster, the Final Project is serverless-only by design;
  5. smaller: Lakeflow, evidence paths, links, config names, RAG fixtures, dashboard file name.
- `README.md` is still Demo 3's README (title, live results) - must be rewritten; Demo 3 live evidence must not be presented as Final Project evidence.

## Design decisions

| Topic | Decision |
|---|---|
| DEV | Personal workspace `dbc-1750318a-76a9`, `workspace.urbanflow_final_dev` (+ `_lakeflow_dev`) |
| PROD | Academy `dbr_dev_trial_2`, `dbr_dev.parvinbadalov_urbanflow_final_prod` (+ `_lakeflow_prod`); a trial workspace, labelled PROD-style, may expire ~2026-10-20 |
| Compute | Serverless Jobs / serverless Lakeflow / existing serverless SQL warehouse; no clusters |
| Isolation | Unique schemas, volume, checkpoints, Job, pipeline, dashboard, AI Search names; no Demo 3 or Lab resource IDs |
| CDC | Core capability: Federation snapshot (optional) -> local Delta source with CDF -> incremental MERGE, audit, checkpoint |
| Zerobus | Optional, Final-Project-owned tables and identity only; never Lab 11 objects or credentials |

## Stages

- [x] A. Coherent standalone project: paths, names, bundle, Demo 3 assumptions removed (dashboard: stage E)
- [x] B. Federation / CDC complete with tests (replay, deletes, idempotency, reconciliation) - offline
- [x] C. Mobility RAG assistant complete (original docs, chunking, retrieval, citations, evaluation) - offline
- [x] D. Optional Zerobus station-health ingestion (isolated, idempotent) - offline
- [x] E. Medallion, Gold KPIs, quality, governance, dashboard - offline
- [x] F. Lakeflow pipeline + Job orchestration - offline
- [x] G. DEV/PROD bundle targets, `final_project_ci.yml`, `final_project_cd.yml` (CD live run needs identities)
- [x] H. README, architecture, Labs 1-12 coverage, cost/performance, Demo Day presentation
- [ ] I. Live validation (only after explicit approval), evidence
- [ ] J. PR, CI green, review

## Findings to resolve (stage E/F)

- The copied notebooks gated on GP1/GP2 cluster IDs and would fail on serverless: replaced by
  `automation.assert_serverless_compute` (refuses any classic cluster, accepts serverless).
- Codex's Job had no `base_parameters`, so tasks would ignore Job parameters (Silver/Gold would
  silently skip): restore the task-to-Job parameter mapping.
- Sample mode in preflight/validation is pinned to Demo 3 execution IDs, and the station path
  assumes Bronze rows Demo 3 landed via Event Hubs. A fresh Final Project environment needs its
  own bounded ingestion task: sample snapshot -> Volume -> Auto Loader (DEV) or Event Hubs (Azure),
  plus landing of the trip sample for the historical path.

## Progress log

- 2026-10-12: handoff inspected, checkpoint commit `e2af127`, `.gitignore` added, venv created, baseline test run recorded.
- 2026-10-12: `2cf6bf1` serverless gate, Job parameter wiring, land_sources + Auto Loader, release validators, CI/CD.
- 2026-10-12: stage B - `cdc_sql.py` builders, fictional maintenance snapshots, notebook 11 rebuilt
  (reconcile before a forward-only checkpoint), Python replay tests; bundle syncs the snapshots.
- 2026-10-12: stage C - mobility prompts, opt-in system scope (offline recall 0.625 -> 0.938);
  notebooks 20-23 to the documentation contract with `rag/pipeline.py` and tests.
- 2026-10-12: stage D - Zerobus: Lab 11 isolation guard, delivery with acknowledgments, Silver
  MERGE proven idempotent, dry-run producer (<= 30 records), optional notebook 12 (not in the Job).
- 2026-10-12: stage E - found that all dashboard/governance/maintenance SQL read Demo 3's schema;
  SQL made target-neutral, one generated dashboard per target, fifth page (CDC + assistant),
  governance demo target-aware and dry run by default.
- 2026-10-12: stage F - CI and CD never installed the Databricks CLI before `validate_bundle.py`;
  validator now merges included resource files; Lakeflow refresh task bounded; CD sample run
  includes fixture-mode CDC.
- 2026-10-12: stage H - Demo 3 evidence docs removed (kept in history and in `Demos/Demo3`);
  new README, coverage matrix, architecture, cost/performance, release/validation, dashboard and
  Demo Day docs; documentation tests check the doc set, Demo 3 identifiers and the CD gates.

## Remaining (needs the owner's decision)

- Stage I live validation: every step in `RELEASE_AND_VALIDATION.md` needs explicit approval
  (billable serverless runs; AI Search endpoint; schemas in the shared `dbr_dev` catalog).
- CD identities: a `personal_dev` service principal with an OAuth secret (owner-controlled) and an
  `azure_prod` OIDC service principal (Academy account admin; not needed for operator deploys).

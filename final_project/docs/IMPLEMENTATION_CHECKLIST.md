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

- [ ] A. Coherent standalone project: paths, names, bundle, tests green, Demo 3 assumptions removed
- [ ] B. Federation / CDC complete with tests (replay, deletes, idempotency, reconciliation)
- [ ] C. Mobility RAG assistant complete (original docs, chunking, retrieval, citations, evaluation)
- [ ] D. Optional Zerobus station-health ingestion (isolated, idempotent)
- [ ] E. Medallion, Gold KPIs, quality, governance, dashboard
- [ ] F. Lakeflow pipeline + Job orchestration
- [ ] G. DEV/PROD bundle targets, `final_project_ci.yml`, `final_project_cd.yml`
- [ ] H. README, architecture, Labs 1-12 coverage, cost/performance, Demo Day presentation
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

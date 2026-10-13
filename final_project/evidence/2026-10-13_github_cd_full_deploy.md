# GitHub CD full protected deployment (2026-10-13): PASS

[← Final Project README](../README.md#17-cicd) · [CI/CD design](../docs/CI_CD.md) · [Release and validation](../docs/RELEASE_AND_VALIDATION.md)

This is the permanent, non-sensitive record of the Final Project's GitHub Actions release to **Personal DEV**. It covers the read-only plan run, the failed deploy attempt and the passing protected deployment with a bounded sample Job. GitHub keeps the run artifacts only until **2026-11-12**, so the facts needed later are copied here. No secret, token or credential value appears in this file.

## Outcome

| Item | Value |
|---|---|
| Passing run | [38400575076](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076), `workflow_dispatch` on `main` |
| Commit | `0d48970956e6502f0d969e06f0820f7e75d3f9c8` (PR #78) |
| Inputs | `target=personal_dev`, `mode=deploy_and_sample_run`, `sample_profile=replay_recorded`, typed confirmation `RELEASE_URBANFLOW_FINAL_PERSONAL_DEV` |
| Deploying identity | Service principal `sp-urbanflow-final-dev` (application ID `9e50ac7f-fe72-4768-bd9c-8a9bee33d981`), OAuth M2M, environment `final-project-personal_dev` |
| Protected approval | `BadalovP` on `final-project-approval` at **12:55:46 UTC**. The reviewer checked the plan artifact, identity, workspace and that no Job was running |
| Databricks sample run | **`112906299302442`**: SUCCESS in 457 s, no retry, Lakeflow refresh excluded |
| Business data | Unchanged (details below) |
| Result | **PASS** |

## The 13 steps of the passing run

| # | Job → step | Result |
|---|---|---|
| 1 | static-validation → typed confirmation | PASS |
| 2 | static-validation → cost and scope warning | Printed |
| 3 | static-validation → secret scan, Ruff, Black, pytest, dashboard `--check`, offline bundle schema | PASS: **867 passed, 1 skipped** (Linux) |
| 4 | plan → pinned workspace host and OAuth M2M | PASS |
| 5 | plan → verify the deploying identity | `Acting as sp-urbanflow-final-dev` |
| 6 | plan → `bundle validate`, `bundle plan`, plan validator, Job-name collision check | PASS: 7 / 7 `skip`, zero field changes |
| 7 | plan → upload the plan for review | `final-project-plan-personal_dev-38400575076` |
| 8 | approve-release → protected reviewer gate | Approved 12:55:46 UTC |
| 9 | release → re-plan immediately before deploying | PASS (validator `--phase pre`) |
| 10 | release → `bundle deploy --fail-on-active-runs` (takes the deployment lock) | PASS |
| 11 | release → post-deploy plan must be unchanged | PASS: 7 / 7 `skip` (validator `--phase post`) |
| 12 | release → one bounded sample run (`run-now` with idempotency token, 40-minute bound, cancel on overrun) and `validate_job_run.py` | Run `112906299302442` SUCCESS; validator PASS |
| 13 | release → upload release evidence (`if: always()`) | `final-project-release-personal_dev-38400575076` |

## Why `replay_recorded`, and what the run actually wrote

The platform Job's defaults describe a **first run on an empty target**: a new station execution and maintenance snapshot `batch_001`. Personal DEV was already at `batch_003`. A default run would have reverted the CDC state to `batch_001`, emitting real update and delete events, and would have added a second station execution that doubles the dashboard counts. PR #78 added the `sample_profile` input. Its default, `replay_recorded`, replays the target's recorded executions, and step 3 of the live validation had already proved that this configuration is idempotent.

Run request (`run-request.json`, from `scripts/cd_sample_request.py`): `source_execution_id = urbanflow-final-304828008837365`, `cdc_snapshot = batch_003`, Lakeflow refresh off.

Checked against a baseline recorded at 12:20 UTC, before the dispatch:

| Area | Observed after the run |
|---|---|
| Landing and Bronze | Every file reported `already_landed`; the Bronze tables have **no new commit** |
| Maintenance CDC | Snapshot equal to the source, so **no MERGE ran**. Source version 4, checkpoint 4 with its original timestamp, history 11 events, Silver equals `batch_003`, reconciliation 0. No CDC table committed |
| 16 shared Silver/Gold tables | Each wrote **one metadata-only Delta `MERGE` commit with 0 rows changed**. Row counts identical, no duplicates |
| RAG tables | No commit |
| Workspace objects | Only the bundle state files changed (the deploy itself). Dashboard and Job definitions unchanged |
| After the run | No active run, no new pipeline update, no AI Search endpoint, no running cluster. Warehouse auto-stopped 13:23:16 UTC |

So "business data unchanged" means every row of every table is identical. It does **not** mean the run made no Delta commits: the 16 metadata-only `MERGE` commits are a known and documented property of the change-aware MERGE.

## Earlier runs, kept as evidence

| Run | Commit | Mode | Result | Cause → fix |
|---|---|---|---|---|
| [38382998636](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38382998636) | `225545f` | `plan_only` | FAIL, no Databricks call | `databricks current-user me` without `-t` in a bundle folder with no default target → `-t "$TARGET"` on identity checks ([evidence](2026-10-13_github_cd_plan_only_closeout.md)) |
| [38389951946](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38389951946) | `34b458b` | `plan_only` | FAIL after a converged 7/7 `skip` plan | Same missing `-t` on `jobs list` → `-t` on every workspace command, with a test (PR #77) ([evidence](2026-10-13_github_cd_plan_only_run2.md)) |
| [38392729496](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38392729496) | `e497841` | `plan_only` | **PASS** | Static validation (860 passed, 1 skipped; secret scan 221 files, 0 findings), SP sign-in, 7/7 `skip`, validator PASS, approval and release skipped. Zero writes, zero compute. Artifact `final-project-plan-personal_dev-38392729496` |
| [38397086064](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38397086064) | `0d48970` | `deploy_and_sample_run` | FAIL at deploy, nothing written | Approval granted 12:36:12 UTC; `Failed to acquire deployment lock: access denied`. Folder `CAN_EDIT` cannot create the lock file → temporary `CAN_MANAGE` (below) |
| [38400575076](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076) | `0d48970` | `deploy_and_sample_run` | **PASS** | This record |

After the failed deploy, no lock file was left behind, no state file had changed since the 08:14 UTC operator deploy, and no Job had run.

## Permission timeline for `sp-urbanflow-final-dev` (Personal DEV only)

| When | Change | Why |
|---|---|---|
| Closeout, before run 38392729496 | Read-only baseline: `CAN_VIEW` on both Jobs, pipeline and warehouse; `CAN_READ` on dashboard and the bundle folder; `USE CATALOG` on `workspace`, `USE SCHEMA` on the two schemas, `READ VOLUME` on the Volume | `bundle plan` makes GET requests only |
| Before run 38397086064 | Bundle folder `CAN_READ` → `CAN_EDIT`; `CAN_MANAGE_RUN` on the platform Job | Deploy files and start one run |
| Before run 38400575076 (owner-approved) | Bundle folder → **`CAN_MANAGE`**, on `/Workspace/Users/parvinbadalov@yahoo.com/.bundle/urbanflow_final_project/personal_dev` only, no parent-folder entry | The CLI needs to create `deploy.lock` |
| After run 38400575076 | **Restored:** `CAN_READ` on the folder and `CAN_VIEW` on the platform Job | Least privilege; `plan_only` still works |

At no point did the principal receive an admin role, resource ownership, or any privilege on data beyond the read grants above. The platform Job runs as the owner, not as the principal. The principal's OAuth secret was transferred straight into the GitHub environment and never printed. It expires on **2026-10-14 at 10:22 UTC**, so a later CD run needs a new secret.

## Artifacts (expire 2026-11-12)

| Artifact | ID | Contents |
|---|---|---|
| `final-project-plan-personal_dev-38392729496` | 11810446794 | Plan-only `pre-plan.json`, `pre-result.json` |
| `final-project-plan-personal_dev-38400575076` | 11814684652 | Reviewed pre-approval plan and verdict |
| `final-project-release-personal_dev-38400575076` | 11815272386 (97 KB) | Re-plan, post-plan, verdicts, run request, run JSON, final validation output |

## Cost

An estimate, since billing records lag: 457 s of serverless compute, similar to the 458 s idempotency replay that measured 1.19 DBU. That is about **1.2 DBU, roughly $0.42 at list price**.

## Scope of this result

- **Demonstrated:** GitHub Actions planned, gated, deployed and ran a bounded Job on **Personal DEV** as a dedicated service principal.
- **Not demonstrated:** a GitHub Actions deployment to the **Azure trial**. Azure was deployed and validated by the operator with their own login ([step 9](2026-10-13_azure_prod_step9_deploy_and_sample_run.md)). Azure GitHub OIDC is **untested**: the identity needs Academy account administration.

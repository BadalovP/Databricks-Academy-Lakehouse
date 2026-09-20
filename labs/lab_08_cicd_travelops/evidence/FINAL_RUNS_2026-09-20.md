# Lab 08 — Final deployment run record (20 September 2026)

> **Scope:** Recorded GitHub Actions results and screenshots for a supervisor. This page does not claim an independent audit of production data. The original historical log index remains in [README.md](README.md).

| Run | Mode / event | Verified result | Interpretation |
|:--|:--|:--|:--|
| [#39](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/35479987880) | Manual OIDC + final Azure job | Success | First integrated full chain, including final OIDC job. |
| [#41](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/35481666973) | Push, docs/workflow only | Success | Tests/change detection succeeded; deployment jobs skipped. |
| [#42](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/35482773314) | Manual PAT | Failed at Azure PAT preflight | Standalone CLI resolved Personal DEV host; **no Azure deployment** in this attempt. |
| [PR #20](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/20) | Workflow fix | Merged before later runs | Moves standalone preflight outside bundle directory; validates resolved Azure host before authentication. |
| **[#45](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/35483922379)** | **Manual PAT + final Azure job** | **Success** | DEV/Personal PROD deploy/validate, approvals, PAT preflight, Azure PAT deploy and Azure PAT run succeeded. OIDC/Terraform skipped. |
| **[#46](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/35485851420)** | **Manual OIDC + final Azure job** | **Success** | DEV/Personal PROD deploy/validate, approvals, Terraform Plan/Apply, Azure deploy/validate and OIDC job succeeded. PAT path skipped. |

Both #45 and #46 were checked at the **job/step outcome level** in GitHub Actions. The user-provided screenshots of #45 and #46 show the overall successful run and selected-path jobs. The #45 Azure pipeline-task screenshot also reports success and shows a 20-dataset Lakeflow graph. These are separate executions, not one workflow exercising both credentials.

## Evidence images

- [PAT end-to-end GitHub result](images/07_pat_full_cicd_success.png) — #45.
- [PAT-triggered Azure Lakeflow task](images/08_pat_lakeflow_success.png) — #45's Azure application pipeline.
- [OIDC end-to-end GitHub result](images/09_oidc_full_cicd_success.png) — #46.
- [Workflow input screenshots](images/01_github_workflow_overview.png), [auth choice](images/06_auth_mode_choices.png), [approval](images/05_production_approval_gate.png).

## Remaining verification boundaries

- Earlier Bronze duplicates are not automatically cleaned by rerunning the pipeline; the scoped remediation plan is **unexecuted**.
- The successful Azure health task indicates its assertions passed; a separately captured read-only query of the exact Azure Gold health-row values was not supplied in this evidence package.
- Success of the integrated workflows does not independently verify repair of the older **standalone** Azure-run monitoring workflow.

Do not commit tokens, state files, or unsanitized runner logs. Do not trigger new production workloads to improve the appearance of this evidence page.

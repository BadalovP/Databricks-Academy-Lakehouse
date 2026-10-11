# Lab 10 read-only audit — 2026-10-12

This record contains only sanitized identifiers and results from read-only APIs. No SQL warehouse, serverless compute, Job run, Terraform apply, table write, permission change, or external-source mutation was started.

## Source control

| Check | Result |
|---|---|
| Local branch | `feature/lab10-azure-prod` at `8ffb21a851c636070100921f68fc7cf08c420ceb` before these local edits |
| Current `origin/main` | `a6c8c2a`; includes merged Lab 12 PRs #70 and #71 |
| Branch relation | 2 commits ahead and 2 commits behind `origin/main` |
| PR | #69, open draft, mergeable, clean merge state |
| PR CI | run `38140721000`, both Terraform roots passed |
| Working tree | Existing Lab 10 workflow, Terraform and documentation edits were preserved; no reset, stash, clean or rebase was used |

## Personal DEV — live metadata

| Object | Verified result |
|---|---|
| Workspace | `https://dbc-1750318a-76a9.cloud.databricks.com` (`7474653929863069`) |
| Job | `453391098761164`, three ordered Git notebook tasks, no cluster fields and no schedule |
| Job runs | `1058102967727375` SUCCESS; `674533552504139` SUCCESS; all three tasks succeeded in both |
| Connection | `neon_postgres_conn`, PostgreSQL, `ACTIVE`, `read_only = true` |
| Foreign catalog | `neon_postgres_conn_catalog`, foreign catalog on `neondb` |
| Delta objects | `customers_delta`, `customers_target`, `customer_change_history`, `cdc_checkpoint` all present |
| Row statistics | source 12, target 12, history 12, checkpoint 1 |
| CDF | enabled on `customers_delta` |
| CDC versions | persisted history spans commit versions 3 through 10; checkpoint column statistics show value 10 |
| Operations | history metadata reports four distinct `_change_type` values; Delta CDF's four row-level values are insert, delete, update pre-image and update post-image |
| Idempotency evidence | second full Job run succeeded; history remains 12 unique persisted events and the implementation advances the checkpoint only after two-way reconciliation |

The historical accepted validation recorded source/target differences as zero and the checkpoint equal to source version 10. The saved older task outputs predate the notebook's `dbutils.notebook.exit` JSON result and therefore do not expose that summary through the Jobs output API. A fresh query or Job run was deliberately not started.

## Terraform state and drift

| Root | Verified result |
|---|---|
| Personal DEV | HCP workspace `badalov-lakehouse-labs/lab10-personal-dev` exists in local execution mode, is unlocked, and holds exactly `databricks_job.lab10` with ID `453391098761164` |
| Personal DEV plan | `No changes. Your infrastructure matches the configuration.` |
| Ignored local state | A legacy backup also identifies Job `453391098761164`; state files are ignored and were not printed, edited or committed |
| Azure PROD | HCP workspace `lab10-azure-prod` returns HTTP 404 and does not exist |
| Azure PROD Job | No Job ID `453391098761164` and no Job named `[Azure PROD] Lab 10 Federation & CDC` exists in `dbr_dev_trial_2` |

The pre-existing untracked `.github/terraform/lab10-azurerm-backend.tf` is an Azure Storage backend template from an earlier investigation. It is not referenced by either Lab 10 Terraform root, is not part of the HCP state design, and should not be staged for PR #69 without a separate decision.

## Azure PROD target — live metadata

| Check | Result |
|---|---|
| Azure subscription | `subscr-ita-vkoldov` (`419d681c-4d7e-48a1-ba31-24afe1f4e486`), tenant `1b8b4c43-4a62-45e3-bd37-55ded2a5d965`, Enabled |
| Workspace | `dbr_dev_trial_2`, ID `7405618361895520`, host `https://adb-7405618361895520.0.azuredatabricks.net`, provisioning `Succeeded` |
| Current user | `parvinbadalov@softserve.academy`, workspace `admins` member |
| Metastore | `7af05576-c79e-4f56-b84f-ead80be5c8b6` |
| Connection | `parvinbadalov_lab10_neon_conn`, PostgreSQL, `ACTIVE`, `read_only = true`, owned by the current user |
| Foreign catalog | `parvinbadalov_lab10_neon_catalog` does not exist |
| PROD schema | `dbr_dev.parvinbadalov_lab10_prod` does not exist |
| Metastore grant | `account users` has `USE_MARKETPLACE_ASSETS`; no `CREATE CATALOG` grant is visible |
| Intended deployer | `github-databricks-academy-deployer` is not assigned to the workspace; account-level inspection is blocked for non-account-admin users |

## GitHub OIDC and workflow state

| Check | Result |
|---|---|
| Required environment | `lab10-azure` does not exist |
| Intended Databricks federation policy | Not verifiable/usable until an account admin creates or confirms the dedicated service principal and policy |
| Existing Lab 08 identity | Azure managed identity `github-lab08-travelops` trusts only `repo:BadalovP/Databricks-Academy-Lakehouse:environment:azure-prod`; it has a shared storage role and is not assigned to `dbr_dev_trial_2` |
| Personal CD plan | GitHub run `38102000753` succeeded; apply skipped |
| Personal CD apply | GitHub run `38102681544` succeeded with the Job-ID and apply guards; zero resource changes |
| Azure CD / run | Never successfully executed; the workflow is not on `main` |

## Remaining approval gates

1. Databricks account/metastore admin creates the foreign catalog and dedicated OIDC service principal/federation policy.
2. User approves HCP workspace and GitHub environment creation/configuration.
3. User approves catalog binding/grants and the one-time `00_setup` data-writing run.
4. Review the plan-only CD result before separately approving Terraform apply.
5. Review the deployed Job before separately approving its first billed serverless run.

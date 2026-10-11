# Lab 10 manual Azure deployment readiness - 2026-10-12

Investigation of deploying the Azure Lab 10 Job to `dbr_dev_trial_2` with the user's own Azure CLI
identity after the user created the foreign catalog. Read-only checks, local tests and Terraform plan
only: no apply, no table or catalog change, no Job run, no Neon write, no commit.

## Foreign catalog (read-only)

| Check | Result |
|---|---|
| Catalog | `parvinbadalov_lab10_neon_catalog`, `FOREIGN_CATALOG` on connection `parvinbadalov_lab10_neon_conn`, database `neondb` |
| Owner / creator | `parvinbadalov@softserve.academy` (created by the user) |
| Isolation | `OPEN`, **no workspace bindings** - visible from every workspace on the shared metastore |
| Grants | `account users`: `BROWSE` only |
| Schemas / tables | `public`: `demo_customers`, `lab10_customers`, `lab10_orders` (all `FOREIGN`, PostgreSQL) |
| User's metastore privileges (grants API) | still only `USE_MARKETPLACE_ASSETS`; creation succeeded regardless, so the workspace-admin path allowed it without a listed grant |

Live Neon data through Lakehouse Federation (SQL, Serverless Starter Warehouse, run as the user):

| Table | Result |
|---|---|
| `lab10_customers` | 12 rows, `customer_id` 1001-1012 (12 distinct), 10 countries, 0 null e-mails |
| `lab10_orders` | 20 rows across 11 customers; columns `order_id, customer_id, order_date, amount, status`; 0 orders without a customer |
| `demo_customers` | 3 rows |

These match the Personal DEV source (12 customers, 20 orders).

## Findings for a manual deployment

1. **Branch:** the Azure Job reads notebooks from Git (`git_branch`, default `main`). On `main` the
   notebooks have no widgets (they ignore `catalog`/`schema` and use the Personal DEV names) and
   `00_setup` does not exist. A pre-merge deployment must use `-var git_branch=feature/lab10-azure-prod`.
2. **Git credential:** Databricks requires Git integration for the identity that runs a Git-sourced
   Job. Personal DEV has a `gitHubOAuth` credential; the user has **none** in `dbr_dev_trial_2`. The
   user must link GitHub (OAuth) once in that workspace's UI before any Git-sourced run.
3. **Serverless auto-retry (fixed locally):** serverless Jobs retry failed tasks by default, and the
   Azure Job did not disable it. Each task now sets `disable_auto_optimization = true`; the validator
   requires it and a test (23rd) proves a missing setting is rejected.
4. **Identity:** a local `terraform apply` with `DATABRICKS_AUTH_TYPE=azure-cli` creates the Job as
   the user (workspace admin), so it runs as the user: owner of the catalog and the connection, with
   `ALL_PRIVILEGES` on `dbr_dev`. No additional grant and no service principal is needed for this
   manual path. The GitHub OIDC workflows are unchanged and remain the automated path.

## Terraform

| Check | Result |
|---|---|
| `fmt -check`, `validate` (Azure root) | pass |
| Offline validator / tests | PASS / 23 passed |
| HCP `lab10-azure-prod` | local execution, Terraform 1.15.8, unlocked, empty state (before and after the plans) |
| `terraform plan -var git_branch=feature/lab10-azure-prod` (Azure CLI identity) | `1 to add, 0 to change, 0 to destroy`: `databricks_job.lab10_azure_prod`, branch `feature/lab10-azure-prod`, `disable_auto_optimization = true` on all 3 tasks, no cluster or schedule, `max_concurrent_runs = 1`, timeout 1800 s, `run_as` defaults to the creator |

## What needs an account administrator

Only the automated CI/CD path: the dedicated service principal `github-databricks-academy-deployer`,
its workspace assignment and its GitHub federation policy. Everything required for a manual, audited
deployment and the CDC acceptance test is within the user's own rights.

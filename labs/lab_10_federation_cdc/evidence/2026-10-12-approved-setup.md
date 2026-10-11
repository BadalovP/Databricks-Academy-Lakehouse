# Lab 10 approved setup steps - 2026-10-12

Actions approved by the user on 2026-10-12 and their verified results. Not approved and not done:
the Personal DEV CDC acceptance test, any Terraform apply, any Job run, any grant or catalog change.

## PR #69 updated

| Item | Result |
|---|---|
| Commit | `4652fae` - 11 Lab 10 files (4 workflows, README, ADMIN_REQUEST, Azure `versions.tf`, 2 evidence files, validator, tests); no Co-Authored-By trailer; `.github/terraform/lab10-azurerm-backend.tf` deliberately not staged |
| Integrate `main` | merge commit `18b4efb` (`git merge --no-ff origin/main`; clean, no rebase or force); the PR diff against `main` contains only Lab 10 files (20 files) |
| Push | fast-forward `8ffb21a..18b4efb` to `feature/lab10-azure-prod` |
| PR | title and description refreshed; still **draft**, not merged |
| CI | run `38183002475`: both legs passed - validator PASS, 22 tests OK, Terraform valid (`terraform`, `terraform_azure_prod`) |

## GitHub environment

`lab10-azure` created at 2026-10-11T23:07:08Z (it did not exist before): no protection rules, no
branch policy, 0 secrets, 0 variables. Still to add after the service principal exists:
variable `DATABRICKS_CLIENT_ID`, secret `HCP_TERRAFORM_TOKEN`; recommended: restrict deployments to
`main`.

## HCP Terraform workspace

`badalov-lakehouse-labs/lab10-azure-prod` (`ws-xceM4QUJ5HC9G4sy`) created (it returned 404 before),
mirroring `lab10-personal-dev`: execution mode **local**, Terraform 1.15.8, auto-apply off. State is
empty.

## Local read-only plan (Azure CLI identity, not OIDC)

From `terraform_azure_prod/` with `DATABRICKS_AUTH_TYPE=azure-cli`: HCP backend initialised;
`terraform plan` -> **`1 to add, 0 to change, 0 to destroy`**: `databricks_job.lab10_azure_prod`
`[Azure PROD] Lab 10 Federation & CDC`, three notebook tasks (`01_federation_validation`,
`02_incremental_cdc_sync`, `03_final_validation`) with `catalog=dbr_dev`,
`schema=parvinbadalov_lab10_prod`, `foreign_catalog=parvinbadalov_lab10_neon_catalog`. Afterwards the
HCP workspace was unlocked with empty state, and `dbr_dev_trial_2` still had no Lab 10 Job. This proves
the Terraform root, backend and Job definition against the real workspace; it does not prove the
GitHub OIDC token exchange, which needs the deployer service principal.

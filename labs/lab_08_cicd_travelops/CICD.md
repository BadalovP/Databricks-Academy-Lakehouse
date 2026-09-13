# LAB 08 - TravelOps CI/CD

## Pull Requests

PR workflows run unit tests, ruff, black, strict bundle validation, Personal Terraform validation with a required no-change plan, and a read-only Azure PROD Terraform plan. PRs never apply Terraform, deploy Azure PROD, or run Personal PROD. Personal DEV deployment on PR is intentionally omitted; it can be added later as an explicitly enabled credentialed rehearsal.

## Main

Merges to `main` use separate jobs and `needs:` dependencies so the Actions graph shows: Unit Tests -> Bundle Validate -> Deploy DEV -> Validate DEV -> Deploy Personal PROD -> Validate Personal PROD -> Terraform Plan -> Terraform Apply -> Deploy Azure PROD -> Validate Azure PROD.

The Terraform apply job performs a read-only effective-permissions check first, generates a fresh plan, rejects every change outside the four allowed Azure PROD additions, applies only that saved fresh plan, and requires a final detailed-exit-code plan of `0`.

The Azure PROD application job is disabled by default. The final job runs only when repository variable `LAB08_RUN_AZURE_PROD_JOB` is explicitly set to `true` after deployment is approved.

## Repository Variables

Set these as GitHub repository variables:

- `DATABRICKS_PERSONAL_HOST`
- `DATABRICKS_AZURE_HOST`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `LAB08_RESOURCE_GROUP_NAME`
- `LAB08_PERSONAL_VOLUME_GRANTEE`
- `LAB08_AZURE_VOLUME_GRANTEE`
- `LAB08_AZURE_STORAGE_ACCOUNT_NAME` (`lab08travelops63e621` for the current state)
- `TF_STATE_RESOURCE_GROUP`
- `TF_STATE_STORAGE_ACCOUNT`
- `TF_STATE_CONTAINER`
- `LAB08_RUN_AZURE_PROD_JOB` (leave unset or `false` in current safety mode)

## Repository Secrets

Azure and Azure Databricks jobs use GitHub OIDC plus Azure CLI authentication. Personal Databricks requires one scoped secret:

- `DATABRICKS_PERSONAL_TOKEN`

## GitHub Environments

Use `personal-dev`, `personal-prod`, and `azure-prod` for scoped variables and auditability. Configure GitHub-to-Azure federated credentials for the workflow subjects that use `azure-prod`. Do not add mandatory human approval to `azure-prod` if the academy final criterion requires automatic main-to-PROD deployment.

## Remote State Safety

Checked-in Terraform roots retain separate local backends for independent workstation use. GitHub Actions stages each root with `.github/terraform/lab08-azurerm-backend.tf` and uses distinct Azure Blob keys: `lab08/personal.tfstate` and `lab08/azure-prod.tfstate`.

Before enabling CI plans, bootstrap the backend outside these Lab 8 roots and migrate both existing local states into those keys. Grant the OIDC principal least-privilege Blob access to the state container. The workflow refuses to plan if the Personal state is missing either Volume/grant or if Azure state is missing the storage account, filesystem, access connector, or storage credential. This prevents an ephemeral runner from trying to recreate existing resources.

The OIDC principal must also be added to the Azure Databricks workspace with the workspace and Unity Catalog privileges required by the bundle and Terraform-owned UC objects. This is separate from its Azure control-plane roles.

## Concurrency

The workflow uses concurrency groups so personal PROD, Terraform PROD, and Azure PROD deployment do not collide with another run.

## Current Azure Blocker

The current identity still cannot run `Microsoft.Authorization/roleAssignments/write` on storage account `lab08travelops63e621`: effective permissions contain `*` but exclude `Microsoft.Authorization/*/Write`. No matching connector role assignment exists at that scope. Personal Terraform is independent and remains unchanged.

## Personal Ownership Model

Personal Terraform owns `dbr_dev.parvinbadalov.lab08_dev_travelops_raw` and `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. DAB owns the DEV and PROD pipelines, jobs, dashboard, alerts, notebooks, application configuration and Bronze/Silver/Gold datasets. Personal DEV publishes to `dbr_dev.parvinbadalov_lab08_dev`; Personal PROD publishes to `dbr_dev.parvinbadalov_lab08_prod` by re-adopting the historical PROD pipeline that already owned that namespace. Do not delete old schemas, volumes, tables, pipelines, or Lakeflow internals as part of CI/CD.

If a historical Personal PROD pipeline is pointed at a new raw source Volume, normal refresh can fail because retained Auto Loader source state still refers to the previous landing path. For this migration, a one-time supported Lakeflow full refresh was used only after confirming the new raw Volume was complete, batch-readable and replayable. CI/CD should continue to use normal bundle deploy/run after that migration step; it must not manually delete checkpoint files or old UC objects.

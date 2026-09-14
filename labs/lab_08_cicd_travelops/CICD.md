# LAB 08 - TravelOps CI/CD

## Pull Requests

PR workflows always run unit tests, ruff, black, and strict bundle validation. When `LAB08_ENABLE_AZURE_OIDC=true`, they also run Personal Terraform validation and read-only Personal/Azure PROD remote-state plans. The temporary PAT path never runs on a PR. PRs never apply Terraform, deploy Azure PROD, run Azure PROD, or run Personal PROD.

## Main

Merges to `main` use separate jobs and `needs:` dependencies. Both modes first run Unit Tests -> Bundle Validate -> Deploy DEV -> Validate DEV -> Deploy Personal PROD -> Validate Personal PROD. `LAB08_ENABLE_AZURE_OIDC=true` continues through Terraform Plan -> Terraform Apply -> Deploy Azure PROD -> Validate Azure PROD. `LAB08_ENABLE_AZURE_OIDC=false` instead runs one temporary PAT job that validates, plans, deploys, runs, and verifies Azure PROD without invoking Azure CLI or Terraform.

The advanced Terraform apply job retains its `roleAssignments/write` permission guard, requires the complete imported Azure PROD state inventory, generates a fresh plan, refuses apply unless that plan is unchanged, applies only that saved fresh plan, and requires a final detailed-exit-code plan of `0`. The temporary PAT path bypasses this Azure control-plane requirement by skipping Terraform entirely.

The advanced Azure PROD application job runs only when both `LAB08_ENABLE_AZURE_OIDC=true` and `LAB08_RUN_AZURE_PROD_JOB=true`. The temporary PAT path runs the promotion job automatically after a delete-free bundle plan and deployment, then requires strict validation and a final `0 add, 0 change, 0 delete` plan.

## Repository Variables

Temporary PAT mode requires repository variable `LAB08_ENABLE_AZURE_OIDC=false` and `DATABRICKS_AZURE_HOST`, plus the two tokens listed below. It uses the bundle's Personal workspace host and known storage account as fallbacks, so the current four-setting configuration is sufficient.

The complete variable inventory for both modes is:

- `DATABRICKS_PERSONAL_HOST` (optional override; defaults to the Personal workspace configured in the bundle)
- `DATABRICKS_AZURE_HOST`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `LAB08_RESOURCE_GROUP_NAME`
- `LAB08_PERSONAL_VOLUME_GRANTEE`
- `LAB08_AZURE_VOLUME_GRANTEE`
- `LAB08_AZURE_STORAGE_ACCOUNT_NAME` (required by advanced OIDC mode; the temporary PAT path defaults to current resource `lab08travelops63e621`)
- `TF_STATE_RESOURCE_GROUP`
- `TF_STATE_STORAGE_ACCOUNT`
- `TF_STATE_CONTAINER`
- `LAB08_ENABLE_AZURE_OIDC` (`false` for the temporary PAT path; `true` for OIDC/Terraform)
- `LAB08_RUN_AZURE_PROD_JOB` (leave unset or `false` in current safety mode)

Azure identity, resource-group, storage-account, and remote-state variables are consumed only by the preserved advanced OIDC/Terraform jobs.

## Repository Secrets

Personal Databricks always requires one scoped secret:

- `DATABRICKS_PERSONAL_TOKEN`

The temporary Azure path additionally requires:

- `DATABRICKS_AZURE_TOKEN`

When `LAB08_ENABLE_AZURE_OIDC=true`, Azure jobs use GitHub OIDC plus Azure CLI authentication and do not use the PAT. When it is `false`, only the temporary Azure DAB job receives `DATABRICKS_AZURE_TOKEN`; Terraform and managed-identity code remain present but are skipped.

## GitHub Environments

Use `personal-dev`, `personal-prod`, and `azure-prod` for scoped variables, secrets, and auditability. Configure GitHub-to-Azure federated credentials for the advanced workflow subjects that use `azure-prod`. The temporary PAT can be scoped to the `azure-prod` environment. Do not add mandatory human approval to `azure-prod` if the academy final criterion requires automatic main-to-PROD deployment.

## Remote State Safety

Checked-in Terraform roots retain separate local backends for independent workstation use. GitHub Actions stages each root with `.github/terraform/lab08-azurerm-backend.tf` and uses distinct Azure Blob keys: `lab08/personal.tfstate` and `lab08/azure-prod.tfstate`.

Remote state is used only by the advanced OIDC/Terraform path. The temporary PAT path does not initialize, read, migrate, or mutate Terraform state and therefore does not require `roleAssignments/write`; the advanced path keeps that permission check unchanged.

Before enabling CI plans, bootstrap the backend outside these Lab 8 roots and migrate both existing local states into those keys. Grant the OIDC principal least-privilege Blob access to the state container. The workflow refuses to plan if Personal state is missing either Volume/grant or if Azure state is missing any of its nine final resources: storage account, filesystem, access connector, imported RBAC assignment, storage credential, external location, external Volume, singular Volume grant, or application schema. This prevents an ephemeral runner from trying to recreate existing resources.

The OIDC principal must also be added to the Azure Databricks workspace with the workspace and Unity Catalog privileges required by the bundle and Terraform-owned UC objects. This is separate from its Azure control-plane roles.

## Concurrency

The workflow uses concurrency groups so personal PROD, Terraform PROD, and Azure PROD deployment do not collide with another run.

## Current Azure Status

The Azure administrator created the `Storage Blob Data Contributor` assignment for connector principal `dbb45359-22b9-4744-8467-2ea8633bd999` at storage account `lab08travelops63e621`. Assignment `c0e24625-edc3-444a-8112-a7327380a96a` is imported as `azurerm_role_assignment.uc_storage_blob_data_contributor`; Terraform must not create a duplicate. The first Azure promotion run failed safely on the seed notebook's missing-schema guard. Terraform now owns `dbr_dev.parvinbadalov_lab08_prod`, and the authorized rerun completed seed, pipeline, and health validation successfully. Infrastructure and bundle plans remain idempotent. Advanced-mode application runs require `LAB08_RUN_AZURE_PROD_JOB=true`; temporary PAT mode runs the promotion job automatically. Before the next advanced Actions run, migrate or import `databricks_schema.travelops_prod` into the `lab08/azure-prod.tfstate` backend key so the nine-address safety guard sees the same ownership as the local state.

## Personal Ownership Model

Personal Terraform owns `dbr_dev.parvinbadalov.lab08_dev_travelops_raw` and `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. DAB owns the DEV and PROD pipelines, jobs, dashboard, alerts, notebooks, application configuration and Bronze/Silver/Gold datasets. Personal DEV publishes to `dbr_dev.parvinbadalov_lab08_dev`; Personal PROD publishes to `dbr_dev.parvinbadalov_lab08_prod` by re-adopting the historical PROD pipeline that already owned that namespace. Do not delete old schemas, volumes, tables, pipelines, or Lakeflow internals as part of CI/CD.

If a historical Personal PROD pipeline is pointed at a new raw source Volume, normal refresh can fail because retained Auto Loader source state still refers to the previous landing path. For this migration, a one-time supported Lakeflow full refresh was used only after confirming the new raw Volume was complete, batch-readable and replayable. CI/CD should continue to use normal bundle deploy/run after that migration step; it must not manually delete checkpoint files or old UC objects.

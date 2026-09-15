# LAB 08 - TravelOps

TravelOps is a standalone Lab 8 project that demonstrates CI/CD promotion for a Databricks lakehouse. Pull requests run quality gates and bundle validation, with read-only Terraform plans added when advanced OIDC mode is enabled. Merges to main promote Personal DEV and Personal PROD before selecting either the advanced Terraform deployment or temporary PAT deployment for Azure PROD.

Azure PROD infrastructure, DAB resources, and the application workload are complete and idempotent. The first grading run failed safely because the required application schema was absent; Terraform now owns that schema, and the authorized rerun succeeded with a green production-health result.

## Requirement Matrix

| Requirement | Implementation |
|---|---|
| Dedicated DAB | `labs/lab_08_cicd_travelops/databricks.yml` |
| DEV and PROD targets | `personal_dev`, `personal_prod`, `azure_prod` |
| PR tests/lint/validate | `.github/workflows/lab08_cicd.yml` |
| Personal PROD rehearsal | `personal_prod` target and post-run zero-change plan |
| Azure PROD deploy on main | Advanced Terraform/OIDC path or temporary Databricks PAT path |
| Assets as code | `resources/`, `pipeline/`, `notebooks/`, `dashboards/`, `sql/`, `terraform/personal`, `terraform/azure-prod` |
| Idempotency | deterministic raw overwrite, DAB state, Terraform `prevent_destroy` |
| No duplicate root ownership | root `databricks.yml` is not modified for Lab 8 |

## Source Database

Phase 0 discovered `samples.wanderbricks` in the personal workspace. Tables used:

| Table | Row count |
|---|---:|
| bookings | 72,247 |
| booking_updates | 83,068 |
| payments | 49,638 |
| users | 124,509 |
| properties | 18,163 |
| reviews | 99,793 |
| destinations | 42 |

## Architecture

`samples.wanderbricks` is copied by `notebooks/00_seed_raw_data.ipynb` into a Terraform-owned raw Volume. Lakeflow ingests those Parquet folders with Auto Loader into Bronze, promotes validated records to Silver, and publishes Gold operational datasets for dashboarding and health checks.

Bronze, Silver and Gold are DAB-owned Lakeflow datasets because application data lifecycle belongs to Databricks. Raw uses Volumes because it models file landing zones and gives Auto Loader a cloud-file source. Personal DEV and Personal PROD raw volumes are managed volumes owned by `terraform/personal` in `dbr_dev.parvinbadalov`. The Personal application schemas are separate: `dbr_dev.parvinbadalov_lab08_dev` and `dbr_dev.parvinbadalov_lab08_prod`. Azure raw uses an external Volume owned by `terraform/azure-prod` so PROD can land data in ADLS Gen2 created and permissioned by Terraform.

Run bundle commands from this directory:

```powershell
cd labs\lab_08_cicd_travelops
databricks bundle validate --strict -t personal_dev
databricks bundle deploy -t personal_dev
databricks bundle run travelops_promotion_job -t personal_dev
```

## Datasets

Personal DEV and Personal PROD use the same unprefixed logical table names in different schemas. DEV receives a new DAB-owned pipeline targeting `dbr_dev.parvinbadalov_lab08_dev`. PROD re-adopts the historical pipeline `4d36399f-2f34-46f8-a2d1-b9340fd1556a` because it already owns the unprefixed objects in `dbr_dev.parvinbadalov_lab08_prod` and Databricks pipeline target schemas are immutable.

Bronze: `bookings_bronze`, `booking_updates_bronze`, `payments_bronze`, `users_bronze`, `properties_bronze`, `reviews_bronze`, `destinations_bronze`.

Silver: `bookings_silver`, `current_bookings_silver`, `payments_silver`, `users_silver`, `properties_silver`, `reviews_silver`, `destinations_silver`.

Gold: `gold_daily_booking_revenue`, `gold_property_performance`, `gold_destination_performance`, `gold_payment_reconciliation`, `gold_review_score`, `gold_production_health`.

## Environments

`personal_dev` publishes application outputs to `dbr_dev.parvinbadalov_lab08_dev` in the personal workspace and reads raw data from the Terraform-owned managed Volume `dbr_dev.parvinbadalov.lab08_dev_travelops_raw`. It may be deployed and run automatically after Personal Terraform is applied.

`personal_prod` publishes application outputs to `dbr_dev.parvinbadalov_lab08_prod` in the same personal workspace and reads raw data from the Terraform-owned managed Volume `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. The historical PROD pipeline is bound into the current DAB deployment so existing PROD tables and Lakeflow internals are reused rather than recreated.

During the ownership migration, the adopted Personal PROD pipeline initially failed with `INVALID_PARAMETER_VALUE.LOCATION_OVERLAP`. Detailed Lakeflow events showed the active Auto Loader source pointed at the new Terraform-owned raw Volume, while retained streaming offsets still referenced the previous raw Volume path in `dbr_dev.parvinbadalov_lab08_prod.lab08_travelops_raw`. The code does not configure explicit `cloudFiles.schemaLocation` or `checkpointLocation`; Lakeflow owns checkpoint metadata under target table `_dlt_metadata`. Because the new raw Volume contained a complete replayable seed and ordinary batch reads succeeded, a supported Personal PROD full refresh was used to rebuild the rehearsal pipeline without deleting schemas, volumes, pipelines, or checkpoints manually.

`azure_prod` uses the paid Azure workspace `https://adb-7405604503619901.1.azuredatabricks.net` in production mode. Terraform owns the application schema `dbr_dev.parvinbadalov_lab08_prod`, while DAB owns the Bronze/Silver/Gold datasets created within it. The Terraform-owned external raw Volume remains `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. The identical Personal PROD and Azure PROD logical volume name is intentional because they live in different physical workspaces. The authorized Azure PROD rerun completed successfully.

## Ownership

Terraform owns raw Volumes in every environment. `terraform/personal` owns the two Personal managed raw Volumes. `terraform/azure-prod` owns Azure storage, raw filesystem, access connector, RBAC, storage credential, external location, the Azure PROD external raw Volume, and the Azure PROD application schema. DAB owns the Lakeflow pipeline, job, notebooks, dashboard, alert, application configuration and Bronze/Silver/Gold datasets. Neither Terraform nor DAB owns the existing `dbr_dev.parvinbadalov` raw schema. The root bundle remains available for Labs 1-7 and does not own Lab 8 resources.

## CI/CD Graph

Pull requests run Unit Tests -> Bundle Validate and, only when `LAB08_ENABLE_AZURE_OIDC=true`, read-only Terraform plans. They never apply, deploy, or run PROD. Pushes to `main` first promote Personal DEV and Personal PROD, then select one explicit Azure mode: `true` preserves the advanced OIDC/Terraform/deploy path; `false` uses a temporary Azure Databricks PAT to validate, deploy, run, and verify Azure PROD without running Terraform. The advanced application-run job also requires `LAB08_RUN_AZURE_PROD_JOB=true`.

## Current Status

Personal DEV and Personal PROD are complete and their final bundle plans are unchanged. Both Terraform states have been migrated to separate Azure Blob keys with preserved lineages, complete resource inventories, and final remote-backed `No changes` plans. The Azure RBAC assignment plans exactly `no-op`. The PAT deployment path remains active with `LAB08_ENABLE_AZURE_OIDC=false`. The registered GitHub managed identity now has minimum access to the pinned Azure bundle root, existing job and pipeline, and referenced SQL warehouse. DAB records the service-principal management grant while preserving the existing human owner; no extra Unity Catalog grant was added because the principal already has effective access through the existing `account users` catalog grant. Advanced run `35021845707` completed the Terraform stages and exposed an owner-permission reconciliation at Azure DAB deployment; the target now leaves the existing human `IS_OWNER` entry implicit so the next authorized OIDC run can verify the corrected no-op plan. Evidence logs live in `evidence/logs/`.

## One-Time Configuration

GitHub repository variables, secrets, OIDC federation, PAT fallback, and remote-state migration required by the workflow are documented in `CICD.md`. Do not commit secret values. Set `LAB08_ENABLE_AZURE_OIDC` explicitly; an unset value activates neither Azure deployment path.

## Documentation Convention

Every Lab 8 Python, YAML, SQL, PowerShell/Shell and Terraform file starts with an ownership/purpose comment or docstring. Every notebook starts with an introduction Markdown cell and each executable cell has an explanation immediately before it.

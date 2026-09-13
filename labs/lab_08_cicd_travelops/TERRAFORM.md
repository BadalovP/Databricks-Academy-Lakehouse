# LAB 08 - TravelOps Terraform

Terraform is split into independent roots:

- `terraform/personal` owns Personal DEV and Personal PROD raw managed Volumes.
- `terraform/azure-prod` owns Azure PROD storage, filesystem, access connector, RBAC, storage credential, external location, and the Azure PROD external raw Volume.

Neither root creates, imports, renames, replaces, or manages the existing `dbr_dev.parvinbadalov` schema. Both roots reference it read-only through `data.databricks_schema`.

Terraform does not create, import, or manage the existing Azure Databricks workspace. It also does not own Databricks jobs, pipelines, dashboards, alerts, notebooks, or Bronze/Silver/Gold datasets; those belong to the Databricks Asset Bundle.

## Commands

Personal:

```powershell
cd labs\lab_08_cicd_travelops\terraform\personal
terraform init
terraform fmt -check
terraform validate
terraform plan -out personal.tfplan
terraform apply -auto-approve personal.tfplan
terraform plan
```

Azure PROD:

```powershell
cd labs\lab_08_cicd_travelops\terraform\azure-prod
terraform init
terraform fmt -check
terraform validate
terraform plan -out lab08.tfplan
terraform apply -auto-approve lab08.tfplan
terraform plan
```

Stop before apply if any plan destroys resources, replaces existing resources, or changes non-Lab-8 resources.

## Migration Notes

The required Personal managed Volumes did not exist in `dbr_dev.parvinbadalov` before this migration, so no Personal import was required. Existing DAB-created raw Volumes in old Lab 8 schemas were not deleted or recreated.

Personal Terraform apply created:

- `dbr_dev.parvinbadalov.lab08_dev_travelops_raw`
- `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`

The second Personal Terraform plan returned `No changes`.

Azure PROD remains blocked on Azure RBAC permission `Microsoft.Authorization/roleAssignments/write`, required to assign `Storage Blob Data Contributor` to the access connector managed identity. A read-only effective-permissions query confirmed that `Microsoft.Authorization/*/Write` is excluded and that no connector role assignment currently exists at the storage-account scope.

The current Azure state owns exactly:

- `azurerm_storage_account.raw`
- `azurerm_storage_data_lake_gen2_filesystem.raw`
- `azurerm_databricks_access_connector.uc`
- `databricks_storage_credential.raw`

The fresh plan is `4 to add, 0 to change, 0 to destroy`. Its only additions are `azurerm_role_assignment.uc_storage_blob_data_contributor`, `databricks_external_location.raw`, `databricks_volume.azure_prod_raw`, and `databricks_grant.azure_prod_raw_volume_rw`. Direct read-only checks confirmed the role assignment, external location, and external Volume do not yet exist, so these are genuinely new rather than unmanaged existing resources.

The Azure PROD raw Volume grant is modeled with singular `databricks_grant.azure_prod_raw_volume_rw` instead of authoritative `databricks_grants`, so Terraform manages only the Lab 8 principal's `READ_VOLUME` and `WRITE_VOLUME` privileges.

Personal DAB deployment is independent of Azure. Personal DEV and Personal PROD publish to dedicated application schemas while Terraform-owned raw Volumes remain in `dbr_dev.parvinbadalov`, so the previous shared-schema quota workaround is no longer part of the design.

The Personal PROD Lakeflow pipeline needed a one-time supported full refresh after Terraform ownership moved the raw source to `dbr_dev.parvinbadalov.lab08_prod_travelops_raw`. Terraform state did not change: the final Personal plan remained `No changes`, and Terraform still owns only the raw managed Volumes and grants. The full refresh affected DAB-owned Lakeflow data/state only and did not create, delete, import, or replace Terraform resources.

## Remote State

Each checked-in root uses a separate local state for academy portability. CI stages the root without its local backend and adds `.github/terraform/lab08-azurerm-backend.tf`, using Azure Blob keys `lab08/personal.tfstate` and `lab08/azure-prod.tfstate`. Before CI is enabled, both local states must be migrated once into those keys and verified. Terraform state files and saved plans must not be committed.

# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/azure-prod/outputs.tf
#
# Purpose:
# Exposes non-secret Azure PROD Terraform outputs consumed by Databricks Asset
# Bundle deployment and GitHub Actions.
#
# Terraform owns:
# - output contracts for Azure PROD infrastructure
#
# Terraform DOES NOT own:
# - application bundle resource IDs
# - secrets or tokens
#
# Safety:
# Outputs contain resource names and URIs only; no credentials are emitted.
# ---------------------------------------------------------------------

output "storage_account_name" {
  description = "Storage account name to pass to the azure_prod bundle target."
  value       = azurerm_storage_account.raw.name
}

output "raw_filesystem_name" {
  description = "ADLS Gen2 filesystem backing the TravelOps raw external location."
  value       = azurerm_storage_data_lake_gen2_filesystem.raw.name
}

output "raw_external_volume_uri" {
  description = "External Volume URI used by the azure_prod Databricks bundle target."
  value       = "abfss://${azurerm_storage_data_lake_gen2_filesystem.raw.name}@${azurerm_storage_account.raw.name}.dfs.core.windows.net/raw"
}

output "storage_credential_name" {
  description = "Unity Catalog storage credential created for TravelOps."
  value       = databricks_storage_credential.raw.name
}

output "external_location_name" {
  description = "Unity Catalog external location created for TravelOps."
  value       = databricks_external_location.raw.name
}

output "azure_prod_raw_volume_full_name" {
  description = "Azure PROD raw external Volume full name."
  value       = "${var.databricks_catalog}.${var.databricks_schema}.${databricks_volume.azure_prod_raw.name}"
}

output "azure_prod_application_schema_full_name" {
  description = "Azure PROD application schema created before the DAB promotion job runs."
  value       = databricks_schema.travelops_prod.id
}

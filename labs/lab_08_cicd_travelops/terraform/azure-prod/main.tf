# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/azure-prod/main.tf
#
# Purpose:
# Creates Azure PROD infrastructure and owns the Azure PROD external raw Volume
# used by the TravelOps Databricks Asset Bundle.
#
# Terraform owns:
# - Lab 8 ADLS Gen2 storage account
# - raw landing filesystem
# - Databricks access connector
# - RBAC from the access connector to the Lab 8 storage account
# - UC storage credential and external location
# - dbr_dev.parvinbadalov.lab08_prod_travelops_raw external Volume
#
# Terraform DOES NOT own:
# - existing Azure Databricks workspace
# - existing dbr_dev.parvinbadalov schema
# - old Lab resources
# - Databricks Lakeflow pipeline
# - application jobs
# - dashboards
#
# Safety:
# Existing resources are never imported or recreated automatically. `prevent_destroy`
# protects stateful storage, identity, external location and raw Volume metadata.
# ---------------------------------------------------------------------

locals {
  storage_account_name = lower(substr(replace("${var.name_prefix}${substr(md5(var.azure_subscription_id), 0, 6)}", "-", ""), 0, 24))
}

data "databricks_schema" "travelops_existing_schema" {
  name = "${var.databricks_catalog}.${var.databricks_schema}"
}

# Storage keeps raw Azure PROD files outside the workspace-managed UC storage
# root so the lab demonstrates an external Volume pattern.
resource "azurerm_storage_account" "raw" {
  name                     = local.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = var.tags

  lifecycle {
    prevent_destroy = true
  }
}

# The raw filesystem is the ADLS Gen2 container backing the Azure PROD external
# location. It is intentionally narrow in scope for cost and permission control.
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "lab08-travelops"
  storage_account_id = azurerm_storage_account.raw.id

  lifecycle {
    prevent_destroy = true
  }
}

# The Databricks access connector provides a managed identity for Unity Catalog
# to access ADLS without embedding secrets in code or CI logs.
resource "azurerm_databricks_access_connector" "uc" {
  name                = "${var.name_prefix}-uc-connector"
  resource_group_name = var.resource_group_name
  location            = var.location
  identity {
    type = "SystemAssigned"
  }
  tags = var.tags

  lifecycle {
    prevent_destroy = true
  }
}

# RBAC is limited to the new Lab 8 storage account. This is currently blocked
# until the Azure principal can write role assignments at this scope.
resource "azurerm_role_assignment" "uc_storage_blob_data_contributor" {
  scope                = azurerm_storage_account.raw.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# The storage credential is the Unity Catalog abstraction that lets Databricks
# reference the access connector identity.
resource "databricks_storage_credential" "raw" {
  name = var.storage_credential_name
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }
  comment = "LAB 08 TravelOps Azure PROD raw storage credential managed by Terraform."

  lifecycle {
    prevent_destroy = true
  }
}

# The external location scopes the credential to the Lab 8 raw filesystem.
resource "databricks_external_location" "raw" {
  name            = var.external_location_name
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.raw.name}@${azurerm_storage_account.raw.name}.dfs.core.windows.net/raw"
  credential_name = databricks_storage_credential.raw.name
  comment         = "LAB 08 TravelOps Azure PROD raw landing external location."
  depends_on      = [azurerm_role_assignment.uc_storage_blob_data_contributor]

  lifecycle {
    prevent_destroy = true
  }
}

# The Azure PROD raw external Volume has the same logical name as Personal PROD,
# but it lives in a different physical Databricks workspace.
resource "databricks_volume" "azure_prod_raw" {
  catalog_name     = var.databricks_catalog
  schema_name      = var.databricks_schema
  name             = var.azure_prod_raw_volume_name
  volume_type      = "EXTERNAL"
  storage_location = databricks_external_location.raw.url
  comment          = "LAB 08 TravelOps Azure PROD raw external Volume managed by Terraform."

  lifecycle {
    prevent_destroy = true
  }
}

resource "databricks_grant" "azure_prod_raw_volume_rw" {
  volume     = databricks_volume.azure_prod_raw.id
  principal  = var.volume_grantee
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]
}

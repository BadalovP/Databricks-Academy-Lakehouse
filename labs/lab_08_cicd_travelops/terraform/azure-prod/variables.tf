# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/azure-prod/variables.tf
#
# Purpose:
# Declares all Azure and Databricks inputs required by the Azure PROD Terraform
# root.
#
# Terraform owns:
# - variable contracts only
#
# Terraform DOES NOT own:
# - secret values
# - existing Databricks workspace identity
# - existing dbr_dev.parvinbadalov schema
#
# Safety:
# Defaults are limited to discovered non-secret values. CI supplies sensitive or
# target-specific values through GitHub Variables/Secrets.
# ---------------------------------------------------------------------

variable "azure_subscription_id" {
  description = "Azure subscription containing the existing Databricks workspace."
  type        = string
  default     = "419d681c-4d7e-48a1-ba31-24afe1f4e486"
}

variable "resource_group_name" {
  description = "Resource group containing the existing Azure Databricks workspace."
  type        = string
  default     = "PL_24_Databricks"
}

variable "location" {
  description = "Azure region discovered for the existing Databricks workspace."
  type        = string
  default     = "eastus"
}

variable "name_prefix" {
  description = "Short prefix used for new Lab 8 Azure resources."
  type        = string
  default     = "lab08travelops"
}

variable "databricks_host" {
  description = "Paid Azure Databricks workspace URL."
  type        = string
  default     = "https://adb-7405604503619901.1.azuredatabricks.net"
}

variable "databricks_profile" {
  description = "Local Databricks CLI profile used by the Terraform Databricks provider. Set to an empty string in CI to use DATABRICKS_HOST/DATABRICKS_TOKEN."
  type        = string
  default     = "AZURE_DEV"
}

variable "databricks_catalog" {
  description = "Existing Unity Catalog catalog where Lab 8 objects are registered."
  type        = string
  default     = "dbr_dev"
}

variable "databricks_schema" {
  description = "Existing Unity Catalog schema used by Azure PROD TravelOps."
  type        = string
  default     = "parvinbadalov"
}

variable "storage_credential_name" {
  description = "Databricks storage credential name created for Lab 8."
  type        = string
  default     = "lab08_travelops_storage_credential"
}

variable "external_location_name" {
  description = "Databricks external location name created for Lab 8 raw landing data."
  type        = string
  default     = "lab08_travelops_raw_external_location"
}

variable "azure_prod_raw_volume_name" {
  description = "Azure PROD external raw Volume name."
  type        = string
  default     = "lab08_prod_travelops_raw"
}

variable "volume_grantee" {
  description = "Principal granted read/write access to the Azure PROD raw external Volume."
  type        = string
  default     = "parvinbadalov@softserve.academy"
}

variable "tags" {
  description = "Tags applied to new Azure resources for ownership and cost tracking."
  type        = map(string)
  default = {
    lab     = "08"
    project = "TravelOps"
    owner   = "parvinbadalov"
  }
}

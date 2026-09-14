# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/personal/variables.tf
#
# Purpose:
# Declares Personal Databricks inputs for raw managed Volume ownership.
#
# Terraform owns:
# - variable contracts for the Personal Terraform root
#
# Terraform DOES NOT own:
# - existing dbr_dev.parvinbadalov schema values
# - Databricks authentication secrets
#
# Safety:
# Defaults are non-secret values discovered during Lab 8 implementation.
# ---------------------------------------------------------------------

variable "databricks_host" {
  description = "Personal Databricks workspace URL."
  type        = string
  default     = "https://dbc-1750318a-76a9.cloud.databricks.com"
}

variable "databricks_profile" {
  description = "Local Databricks CLI profile for the personal workspace. Set to an empty string in CI to use DATABRICKS_HOST/DATABRICKS_TOKEN."
  type        = string
  default     = "personal-yahoo"
}

variable "catalog_name" {
  description = "Existing Unity Catalog catalog. Terraform reads it but does not create it."
  type        = string
  default     = "dbr_dev"
}

variable "schema_name" {
  description = "Existing Unity Catalog schema. Terraform reads it but does not create, rename or replace it."
  type        = string
  default     = "parvinbadalov"
}

variable "volume_grantee" {
  description = "Principal granted read/write access to the Personal raw volumes."
  type        = string
  default     = "parvinbadalov@yahoo.com"
}

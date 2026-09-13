# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/personal/versions.tf
#
# Purpose:
# Pins Terraform and Databricks provider versions for independently managing
# Personal DEV and Personal PROD raw managed volumes.
#
# Terraform owns:
# - provider constraints for the Personal Terraform root
#
# Terraform DOES NOT own:
# - the existing dbr_dev.parvinbadalov schema
# - Databricks Lakeflow pipelines, jobs, dashboards or managed tables
#
# Safety:
# This root is independent of Azure PROD, so Azure RBAC blockers cannot prevent
# Personal DEV/PROD raw Volume management.
# ---------------------------------------------------------------------

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.68"
    }
  }
}

provider "databricks" {
  host    = var.databricks_host
  profile = var.databricks_profile != "" ? var.databricks_profile : null
}

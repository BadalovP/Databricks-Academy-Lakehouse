# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/azure-prod/versions.tf
#
# Purpose:
# Pins Terraform and provider versions for Azure PROD infrastructure and Unity
# Catalog resource management.
#
# Terraform owns:
# - provider version constraints for the Azure PROD Terraform root
#
# Terraform DOES NOT own:
# - the existing Azure Databricks workspace
# - Databricks application jobs, pipelines, dashboards or managed tables
#
# Safety:
# AzureRM provider auto-registration is disabled because the current identity
# cannot register unrelated resource providers at subscription scope.
# ---------------------------------------------------------------------

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.68"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id                 = var.azure_subscription_id
  resource_provider_registrations = "none"
}

provider "databricks" {
  host    = var.databricks_host
  profile = var.databricks_profile != "" ? var.databricks_profile : null
}

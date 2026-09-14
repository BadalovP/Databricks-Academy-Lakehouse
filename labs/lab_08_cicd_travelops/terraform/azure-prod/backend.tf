# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/azure-prod/backend.tf
#
# Purpose:
# Keeps Azure PROD state separate from Personal Databricks state.
#
# Terraform owns:
# - local state placement for Azure PROD infrastructure
#
# Terraform DOES NOT own:
# - shared remote backend infrastructure
# - backend access secrets
#
# Safety:
# CI may pass remote backend settings after bootstrap. State files are ignored
# and must never be committed.
# ---------------------------------------------------------------------

terraform {
  backend "local" {
    path = "terraform-azure-prod.tfstate"
  }
}

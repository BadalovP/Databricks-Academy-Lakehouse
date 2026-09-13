# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/personal/backend.tf
#
# Purpose:
# Keeps Personal Terraform state separate from Azure PROD state.
#
# Terraform owns:
# - local state placement for Personal raw Volume resources
#
# Terraform DOES NOT own:
# - shared remote backend infrastructure
# - secret values
#
# Safety:
# The state file is ignored by Git and must not be committed.
# ---------------------------------------------------------------------

terraform {
  backend "local" {
    path = "terraform-personal.tfstate"
  }
}

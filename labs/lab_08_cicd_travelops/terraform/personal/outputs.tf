# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/personal/outputs.tf
#
# Purpose:
# Exposes non-secret Personal Terraform outputs consumed by operators and CI.
#
# Terraform owns:
# - output contracts for Personal raw Volumes
#
# Terraform DOES NOT own:
# - application bundle outputs
# - secret values
#
# Safety:
# Outputs contain Unity Catalog object names only.
# ---------------------------------------------------------------------

output "personal_dev_raw_volume_full_name" {
  description = "Personal DEV raw managed Volume full name."
  value       = "${var.catalog_name}.${var.schema_name}.${databricks_volume.personal_dev_raw.name}"
}

output "personal_prod_raw_volume_full_name" {
  description = "Personal PROD raw managed Volume full name."
  value       = "${var.catalog_name}.${var.schema_name}.${databricks_volume.personal_prod_raw.name}"
}

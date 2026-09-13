# ---------------------------------------------------------------------
# LAB 08 - TravelOps
# File: terraform/personal/main.tf
#
# Purpose:
# Owns the Personal DEV and Personal PROD raw managed Volumes used by the
# TravelOps Databricks Asset Bundle.
#
# Terraform owns:
# - dbr_dev.parvinbadalov.lab08_dev_travelops_raw
# - dbr_dev.parvinbadalov.lab08_prod_travelops_raw
# - least-privilege grants on those volumes
#
# Terraform DOES NOT own:
# - existing catalog dbr_dev
# - existing schema dbr_dev.parvinbadalov
# - previous DAB-created Lab 8 raw volumes in old schemas
# - pipelines, jobs, notebooks, dashboards or managed B/S/G tables
#
# Safety:
# The schema is read through a data source. `prevent_destroy` protects raw
# volumes from accidental deletion during ownership migration.
# ---------------------------------------------------------------------

data "databricks_schema" "travelops_existing_schema" {
  name = "${var.catalog_name}.${var.schema_name}"
}

# Personal DEV raw files land in a managed Volume under the required shared
# schema. The Volume name isolates DEV from Personal PROD in the same workspace.
resource "databricks_volume" "personal_dev_raw" {
  catalog_name = var.catalog_name
  schema_name  = var.schema_name
  name         = "lab08_dev_travelops_raw"
  volume_type  = "MANAGED"
  comment      = "LAB 08 TravelOps Personal DEV raw managed Volume managed by Terraform."

  lifecycle {
    prevent_destroy = true
  }
}

# Personal PROD rehearses production promotion while staying in the personal
# workspace. It intentionally uses a distinct raw Volume from Personal DEV.
resource "databricks_volume" "personal_prod_raw" {
  catalog_name = var.catalog_name
  schema_name  = var.schema_name
  name         = "lab08_prod_travelops_raw"
  volume_type  = "MANAGED"
  comment      = "LAB 08 TravelOps Personal PROD raw managed Volume managed by Terraform."

  lifecycle {
    prevent_destroy = true
  }
}

# Grants are scoped to raw Volume file access rather than broad workspace or
# catalog administration.
resource "databricks_grants" "personal_dev_raw" {
  volume = databricks_volume.personal_dev_raw.id

  grant {
    principal  = var.volume_grantee
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

resource "databricks_grants" "personal_prod_raw" {
  volume = databricks_volume.personal_prod_raw.id

  grant {
    principal  = var.volume_grantee
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

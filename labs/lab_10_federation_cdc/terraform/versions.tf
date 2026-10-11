
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.80.0, < 2.0.0"
    }
  }
}

provider "databricks" {
  # Pin the target Databricks workspace.
  # Local execution: use DATABRICKS_CONFIG_PROFILE=personal_lab10.
  # GitHub Actions CD: authenticate with DATABRICKS_AUTH_TYPE=pat
  # and DATABRICKS_TOKEN from the GitHub Actions secret.
  host = "https://dbc-1750318a-76a9.cloud.databricks.com"
}


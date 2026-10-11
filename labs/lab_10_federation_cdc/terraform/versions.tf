
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
  # Pin the workspace so automation cannot silently target another profile.
  # Local use: set DATABRICKS_CONFIG_PROFILE=personal_lab10.
  # GitHub Actions: use DATABRICKS_AUTH_TYPE=github-oidc and a federated
  # Databricks service principal; no Databricks secret is required.
  host = "https://dbc-1750318a-76a9.cloud.databricks.com"
}

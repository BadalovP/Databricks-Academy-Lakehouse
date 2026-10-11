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
  # Pinned to the Academy Azure Databricks workspace dbr_dev_trial_2, the Lab 10
  # Azure PROD-style target. A different workspace cannot be targeted by
  # changing an environment variable. Data lives in the shared dbr_dev catalog
  # (same metastore), not in this workspace.
  # GitHub Actions: native Databricks workload identity federation with
  # DATABRICKS_AUTH_TYPE=github-oidc (no Azure secret and no Databricks PAT).
  # Local plan: az login, then DATABRICKS_AUTH_TYPE=azure-cli.
  host = "https://adb-7405618361895520.0.azuredatabricks.net"
}

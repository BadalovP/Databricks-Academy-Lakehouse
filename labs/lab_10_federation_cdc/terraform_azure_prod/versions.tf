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
  # Pinned to the shared Academy Azure Databricks workspace. A different
  # workspace cannot be targeted by changing an environment variable.
  # GitHub Actions: Azure OIDC (azure/login) + DATABRICKS_AUTH_TYPE=azure-cli.
  # Local plan: az login, then DATABRICKS_AUTH_TYPE=azure-cli.
  host = "https://adb-7405604503619901.1.azuredatabricks.net"
}

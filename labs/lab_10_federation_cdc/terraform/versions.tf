
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
  profile = "personal_lab10"
}

locals {
  notebook_root = "labs/lab_10_federation_cdc"

  # Every task receives the Azure PROD names; the notebooks default to Personal DEV.
  parameters = {
    catalog = var.catalog
    schema  = var.schema
  }
}

# Serverless Jobs compute: no task declares existing_cluster_id, new_cluster
# or job_cluster_key, and the Job defines no job_cluster block.
resource "databricks_job" "lab10_azure_prod" {
  name                = "[Azure PROD] Lab 10 Federation & CDC"
  description         = "Neon Lakehouse Federation and incremental Delta CDF sync on serverless compute. Managed by Terraform (HCP workspace lab10-azure-prod)."
  max_concurrent_runs = 1
  timeout_seconds     = 1800

  tags = {
    project     = "lab10"
    environment = "azure-prod"
    managed_by  = "terraform"
  }

  git_source {
    url      = "https://github.com/BadalovP/Databricks-Academy-Lakehouse.git"
    provider = "gitHub"
    branch   = var.git_branch
  }

  task {
    task_key = "01_federation_validation"

    notebook_task {
      source          = "GIT"
      notebook_path   = "${local.notebook_root}/01_federation_validation.ipynb"
      base_parameters = merge(local.parameters, { foreign_catalog = var.foreign_catalog })
    }
  }

  task {
    task_key = "02_incremental_cdc_sync"

    depends_on {
      task_key = "01_federation_validation"
    }

    notebook_task {
      source          = "GIT"
      notebook_path   = "${local.notebook_root}/02_incremental_cdc_sync.ipynb"
      base_parameters = local.parameters
    }
  }

  task {
    task_key = "03_final_validation"

    depends_on {
      task_key = "02_incremental_cdc_sync"
    }

    notebook_task {
      source          = "GIT"
      notebook_path   = "${local.notebook_root}/03_final_validation.ipynb"
      base_parameters = local.parameters
    }
  }
}

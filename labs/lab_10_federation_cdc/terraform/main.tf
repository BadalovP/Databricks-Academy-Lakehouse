
resource "databricks_job" "lab10" {
  name                = "[Personal DEV] Lab 10 Federation & CDC"
  description         = "Neon Federation and incremental Delta CDC"
  max_concurrent_runs = 1
  timeout_seconds     = 1200

  git_source {
    url      = "https://github.com/BadalovP/Databricks-Academy-Lakehouse.git"
    provider = "gitHub"
    branch   = "main"
  }

  task {
    task_key = "01_federation_validation"

    notebook_task {
      source        = "GIT"
      notebook_path = "labs/lab_10_federation_cdc/01_federation_validation.ipynb"
    }
  }

  task {
    task_key = "02_incremental_cdc_sync"

    depends_on {
      task_key = "01_federation_validation"
    }

    notebook_task {
      source        = "GIT"
      notebook_path = "labs/lab_10_federation_cdc/02_incremental_cdc_sync.ipynb"
    }
  }

  task {
    task_key = "03_final_validation"

    depends_on {
      task_key = "02_incremental_cdc_sync"
    }

    notebook_task {
      source        = "GIT"
      notebook_path = "labs/lab_10_federation_cdc/03_final_validation.ipynb"
    }
  }
}

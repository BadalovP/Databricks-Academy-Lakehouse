# Azure PROD state lives in its OWN HCP Terraform workspace.
# Personal DEV keeps lab10-personal-dev; the two states never mix.
terraform {
  cloud {
    organization = "badalov-lakehouse-labs"

    workspaces {
      name = "lab10-azure-prod"
    }
  }
}

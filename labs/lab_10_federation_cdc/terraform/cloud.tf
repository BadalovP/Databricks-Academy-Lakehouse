
terraform {
  cloud {
    organization = "badalov-lakehouse-labs"

    workspaces {
      name = "lab10-personal-dev"
    }
  }
}

variable "catalog" {
  description = "Unity Catalog catalog holding the isolated Lab 10 schema."
  type        = string
  default     = "dbr_dev"
}

variable "schema" {
  description = "Isolated Lab 10 schema; created once by 00_setup."
  type        = string
  default     = "parvinbadalov_lab10_prod"
}

variable "foreign_catalog" {
  description = "Foreign catalog on connection parvinbadalov_lab10_neon_conn (requested from an Academy admin)."
  type        = string
  default     = "parvinbadalov_lab10_neon_catalog"
}

variable "git_branch" {
  description = "Branch the Job reads notebooks from."
  type        = string
  default     = "main"
}

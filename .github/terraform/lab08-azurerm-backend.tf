# LAB 08 - TravelOps CI backend
#
# Purpose:
# Replaces the checked-in local backend only in temporary GitHub Actions copies
# so Personal and Azure PROD use separate, durable Azure Blob state keys.
#
# Ownership:
# This template configures state access only; it does not create or own the
# shared backend resource group, storage account, container, or credentials.
#
# Safety:
# Backend values are supplied by GitHub Variables after state is migrated. The
# workflow refuses unsafe plans when required imported resources are absent.
terraform {
  backend "azurerm" {}
}

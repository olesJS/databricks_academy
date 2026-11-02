terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.14.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id                 = var.subscription_id
  resource_provider_registrations = "none"
}

data "azurerm_storage_account" "landing" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_storage_data_lake_gen2_path" "landing_prod" {
  storage_account_id = data.azurerm_storage_account.landing.id
  filesystem_name    = var.container_name
  path               = var.landing_path
  resource           = "directory"
}
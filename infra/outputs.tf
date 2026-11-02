output "landing_path" {
  value = "abfss://${var.container_name}@${data.azurerm_storage_account.landing.name}.dfs.core.windows.net/${var.landing_path}"
}
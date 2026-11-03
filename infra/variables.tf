variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group of the storage account"
  type        = string
}

variable "storage_account_name" {
  description = "Existing ADLS Gen2 storage account"
  type        = string
}

variable "container_name" {
  description = "Existing container that the external location already covers"
  type        = string
}

variable "landing_path" {
  description = "New folder for the PROD landing volume"
  type        = string
  default     = "oles0305-prod/landing"
}


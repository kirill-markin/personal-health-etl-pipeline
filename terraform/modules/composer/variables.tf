variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "environment_name" {
  description = "The Cloud Composer environment name"
  type        = string
}

variable "service_account" {
  description = "The service account email"
  type        = string
}

variable "oura_api_token" {
  description = "Oura Ring API token"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "GCS bucket name for Oura data"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID for Oura data"
  type        = string
}

variable "location" {
  description = "GCP location for resources"
  type        = string
}
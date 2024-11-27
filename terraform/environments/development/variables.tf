variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "The GCP location"
  type        = string
  default     = "EU"
}

variable "bucket_name" {
  description = "The GCS bucket name"
  type        = string
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
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
  description = "The Oura Ring API token"
  type        = string
}

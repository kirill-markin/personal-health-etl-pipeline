variable "composer_bucket" {
  description = "The Cloud Composer DAGs bucket name"
  type        = string
}

variable "bucket_name" {
  description = "The GCS bucket name"
  type        = string
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
  type        = string
}

variable "location" {
  description = "The GCP location"
  type        = string
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}
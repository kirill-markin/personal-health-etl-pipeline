terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.11.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "2.4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "storage" {
  source      = "../../modules/storage"
  project_id  = var.project_id
  bucket_name = var.bucket_name
  location    = var.location
}

module "bigquery" {
  source     = "../../modules/bigquery"
  project_id = var.project_id
  dataset_id = var.dataset_id
  location   = var.location
}

module "composer" {
  source           = "../../modules/composer"
  project_id       = var.project_id
  region           = var.region
  environment_name = var.environment_name
  service_account  = var.service_account
  oura_api_token   = var.oura_api_token
  bucket_name      = module.storage.bucket_name
  dataset_id       = module.bigquery.dataset_id
  location         = var.location
}

module "dags" {
  source          = "../../modules/dags"
  composer_bucket = module.composer.composer_dag_bucket
  bucket_name     = module.storage.bucket_name
  dataset_id      = module.bigquery.dataset_id
  location        = var.location
  project_id      = var.project_id
}

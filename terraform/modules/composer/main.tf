resource "google_composer_environment" "composer_env" {
  name    = var.environment_name
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2"
      
      env_variables = {
        AIRFLOW_VAR_PROJECT_ID = var.project_id
        AIRFLOW_VAR_OURA_API_TOKEN = var.oura_api_token
        OURA_API_TOKEN = var.oura_api_token
      }
    }
  }

  labels = {
    environment = "development"
    managed_by  = "terraform"
  }
}

resource "google_storage_bucket_object" "airflow_variables" {
  name    = "data/airflow_variables.json"
  bucket  = split("/", google_composer_environment.composer_env.config.0.dag_gcs_prefix)[2]
  content = jsonencode({
    oura_gcp_config = {
      bucket_name         = var.bucket_name
      dataset_id         = var.dataset_id
      location          = var.location
      project_id        = var.project_id
      raw_data_path     = "raw/oura/{data_type}/{date}"
      processed_data_path = "processed/oura/{data_type}/{date}"
    }
  })
  content_type = "application/json"
}
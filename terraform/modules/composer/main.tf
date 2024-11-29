resource "google_composer_environment" "composer_env" {
  name    = var.environment_name
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2"
      
      pypi_packages = {
        "apache-airflow-providers-google" = ">=10.3.0"
      }
      
      env_variables = {
        GCP_PROJECT_ID = var.project_id
        AIRFLOW_VAR_PROJECT_ID = var.project_id
        AIRFLOW_VAR_OURA_API_TOKEN = var.oura_api_token
        OURA_API_TOKEN = var.oura_api_token
        AIRFLOW_VAR_OURA_GCP_CONFIG = jsonencode({
          bucket_name         = var.bucket_name
          dataset_id         = var.dataset_id
          location          = var.location
          project_id        = var.project_id
          raw_data_path_str     = "raw/oura/{data_type}/{start_date}_{end_date}"
        })
      }
    }
  }

  labels = {
    environment = "development"
    managed_by  = "terraform"
  }
}

# Create Airflow variables by uploading a JSON file to the Composer bucket
resource "google_storage_bucket_object" "airflow_variables" {
  name    = "data/airflow_variables.json"
  bucket  = split("/", google_composer_environment.composer_env.config[0].dag_gcs_prefix)[2]
  content = jsonencode({
    oura_gcp_config = {
      bucket_name         = var.bucket_name
      dataset_id         = var.dataset_id
      location          = var.location
      project_id        = var.project_id
      raw_data_path_str     = "raw/oura/{data_type}/{start_date}_{end_date}"
    }
  })
  content_type = "application/json"
}

# Enable Secret Manager API
resource "google_project_service" "secretmanager_api" {
  project = var.project_id
  service = "secretmanager.googleapis.com"

  disable_on_destroy = false
}

# Wait for API to be enabled
resource "time_sleep" "wait_for_secretmanager" {
  depends_on = [google_project_service.secretmanager_api]
  create_duration = "60s"
}

# Add explicit dependency to the secret including the wait
resource "google_secret_manager_secret" "oura_config" {
  depends_on = [time_sleep.wait_for_secretmanager]
  
  secret_id = "oura_gcp_config"
  project   = var.project_id

  replication {
    auto {}
  }
}

# Add the secret version
resource "google_secret_manager_secret_version" "oura_config_version" {
  secret      = google_secret_manager_secret.oura_config.id
  secret_data = jsonencode({
    bucket_name         = var.bucket_name
    dataset_id         = var.dataset_id
    location          = var.location
    project_id        = var.project_id
    raw_data_path_str     = "raw/oura/{data_type}/{start_date}_{end_date}"
  })
}

# Grant Composer service account access to the secret
resource "google_secret_manager_secret_iam_member" "composer_secret_access" {
  secret_id = google_secret_manager_secret.oura_config.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_composer_environment.composer_env.config[0].node_config[0].service_account}"

  depends_on = [
    google_composer_environment.composer_env,
    google_secret_manager_secret.oura_config
  ]
}

# Also grant access to the user-provided service account for development
resource "google_secret_manager_secret_iam_member" "dev_secret_access" {
  secret_id = google_secret_manager_secret.oura_config.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.service_account}"
}
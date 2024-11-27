output "airflow_uri" {
  value = google_composer_environment.composer_env.config[0].airflow_uri
  description = "The URI of the Apache Airflow Web UI hosted within the Cloud Composer environment"
}

output "dag_gcs_prefix" {
  value = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  description = "The Cloud Storage prefix of the DAGs for the Cloud Composer environment"
}

output "composer_dag_bucket" {
  value = split("/", google_composer_environment.composer_env.config[0].dag_gcs_prefix)[2]
  description = "The GCS bucket where DAGs should be uploaded"
}
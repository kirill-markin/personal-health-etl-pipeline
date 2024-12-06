output "deployed_dags" {
  description = "Map of deployed DAG files in Cloud Composer"
  value       = google_storage_bucket_object.dags
}

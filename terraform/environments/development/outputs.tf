output "env_file_content" {
  value = <<-EOT
    GOOGLE_APPLICATION_CREDENTIALS=${path.cwd}/.config/development/terraform-sa-key.json
    PROJECT_ID=${var.project_id}
    REGION=${var.region}
    LOCATION=${var.location}
    ENVIRONMENT_NAME=${var.environment_name}
    BUCKET_NAME=${var.bucket_name}
    DATASET_ID=${var.dataset_id}
    OURA_API_TOKEN=${var.oura_api_token}
  EOT
  sensitive = true
}

output "composer_dag_bucket" {
  value = module.composer.composer_dag_bucket
  description = "The GCS bucket where DAGs should be uploaded"
}
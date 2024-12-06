# Deploy DAGs
resource "google_storage_bucket_object" "dags" {
  for_each = fileset("../../../composer_dags/dags", "**/*.py")
  
  name         = "dags/${each.value}"
  source       = "../../../composer_dags/dags/${each.value}"
  content_type = "application/x-python"
  bucket       = var.composer_bucket
}

# FIXME
# Add config files to a separate configs folder in Composer
resource "google_storage_bucket_object" "airflow_variables" {
  name         = "data/airflow_variables.json"
  content      = jsonencode({
    oura_gcp_config = {
      bucket_name         = var.bucket_name
      dataset_id         = var.dataset_id
      location          = var.location
      project_id        = var.project_id
      raw_data_path_str     = "raw/oura/{data_type}/{start_date}_{end_date}"
    }
  })
  content_type = "application/json"
  bucket       = var.composer_bucket
}

# FIXME
# Add schema files to Composer
resource "google_storage_bucket_object" "schemas" {
  for_each = fileset("../../../schemas", "**/*.json")
  
  name         = "data/schemas/${each.value}"
  source       = "../../../schemas/${each.value}"
  content_type = "application/json"
  bucket       = var.composer_bucket
}

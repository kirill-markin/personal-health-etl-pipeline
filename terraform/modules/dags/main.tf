# Add local data source for DAG files
data "local_file" "dag_files" {
  for_each = fileset("../../../composer_dags/dags", "**/*.py")
  filename = "../../../composer_dags/dags/${each.value}"
}

# Deploy DAGs to Composer
resource "google_storage_bucket_object" "dag" {
  for_each = data.local_file.dag_files

  name         = "dags/${each.key}"
  content      = coalesce(each.value.content, " ")
  content_type = "application/x-python"
  bucket       = var.composer_bucket

  metadata = {
    deployed_by = "terraform"
    deploy_time = timestamp()
  }
}

# For data sources - maintain package structure
resource "google_storage_bucket_object" "data_sources" {
  for_each = fileset("../../../data_sources", "**/*.py")
  
  # Ensure __init__.py files are included
  name         = "plugins/data_sources/${each.value}"
  source       = "../../../data_sources/${each.value}"
  content_type = "application/x-python"
  bucket       = var.composer_bucket
}

# Add config files to a separate configs folder in Composer
resource "google_storage_bucket_object" "airflow_variables" {
  name         = "data/airflow_variables.json"
  content      = jsonencode({
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
  bucket       = var.composer_bucket
}
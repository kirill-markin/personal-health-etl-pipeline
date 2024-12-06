resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_id
  project                     = var.project_id
  location                    = var.location
  delete_contents_on_destroy  = true

  access {
    role          = "OWNER"
    user_by_email = "kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  labels = {
    environment = "development"
  }
}

# Tables for Oura data grouped by day
resource "google_bigquery_table" "oura_day" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = var.project_id
  table_id   = "oura_day"

  deletion_protection = false  # Set to true in production

  schema = file("../../../schemas/oura/oura_day.json")
}

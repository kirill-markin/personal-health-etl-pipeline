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

# Tables for Oura data
resource "google_bigquery_table" "oura_sleep" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = var.project_id
  table_id   = "oura_sleep"

  deletion_protection = false  # Set to true in production

  schema = file("../../../schemas/oura/oura_sleep.json")
}

resource "google_bigquery_table" "oura_activity" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = var.project_id
  table_id   = "oura_activity"

  deletion_protection = false  # Set to true in production

  schema = file("../../../schemas/oura/oura_activity.json")
}

resource "google_bigquery_table" "oura_readiness" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = var.project_id
  table_id   = "oura_readiness"

  deletion_protection = false  # Set to true in production

  schema = file("../../../schemas/oura/oura_readiness.json")
}
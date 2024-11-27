output "dataset_id" {
  description = "The ID of the created dataset"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "dataset_location" {
  description = "The location of the created dataset"
  value       = google_bigquery_dataset.dataset.location
}
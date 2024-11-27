output "bucket_name" {
  description = "The name of the GCS bucket"
  value       = google_storage_bucket.data_bucket.name
}

output "bucket_url" {
  description = "The URL of the GCS bucket"
  value       = google_storage_bucket.data_bucket.url
}
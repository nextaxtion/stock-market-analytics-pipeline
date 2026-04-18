output "gcs_bucket_name" {
  description = "Name of the GCS data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.data_lake.url
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.warehouse.dataset_id
}

output "bigquery_dataset_project" {
  description = "GCP project containing the BigQuery dataset"
  value       = google_bigquery_dataset.warehouse.project
}

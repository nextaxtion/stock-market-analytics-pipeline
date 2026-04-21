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

output "artifact_registry_url" {
  description = "Docker image registry URL prefix"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}"
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL for the Cloud Composer environment"
  value       = google_composer_environment.composer.config[0].airflow_uri
}

output "composer_dags_bucket" {
  description = "GCS bucket where Composer reads DAG files"
  value       = google_composer_environment.composer.config[0].dag_gcs_prefix
}

output "pipeline_runner_sa" {
  description = "Service account email used by Cloud Run jobs"
  value       = google_service_account.pipeline_runner.email
}

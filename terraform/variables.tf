variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key file"
  type        = string
  default     = "/home/nextaxtion/deZoomcampWeek2/secrets/gcp-key.json"
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for the data lake"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID for the data warehouse"
  type        = string
  default     = "dezoomcampds"
}

variable "ar_repository" {
  description = "Artifact Registry repository ID for Docker images"
  type        = string
  default     = "stock-pipeline"
}

variable "composer_name" {
  description = "Cloud Composer 2 environment name"
  type        = string
  default     = "stock-market-composer"
}

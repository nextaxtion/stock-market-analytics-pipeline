variable "project_id" {
  description = "GCP project ID"
  type        = string
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

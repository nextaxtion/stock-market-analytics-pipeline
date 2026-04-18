terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# GCS Bucket — Data Lake
# ---------------------------------------------------------------------------
# Stores raw CSV files and Spark-processed Parquet output before loading to BQ.
# Lifecycle rule removes raw data after 90 days to control storage costs.

resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  storage_class = "STANDARD"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90 # days — raw landing zone data only needs to persist during pipeline runs
    }
  }
}

# ---------------------------------------------------------------------------
# BigQuery Dataset — Data Warehouse
# ---------------------------------------------------------------------------
# All tables (raw, staging, core, aggregation) live in this single dataset.
# Location must match the GCS bucket region for data transfer jobs.

resource "google_bigquery_dataset" "warehouse" {
  dataset_id                  = var.bq_dataset_id
  location                    = var.region
  description                 = "Stock market analytics — raw tables, dbt models, and aggregation layers"
  delete_contents_on_destroy  = true
}

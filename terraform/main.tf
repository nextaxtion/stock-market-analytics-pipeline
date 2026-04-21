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
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# ---------------------------------------------------------------------------
# Required GCP APIs
# ---------------------------------------------------------------------------

resource "google_project_service" "apis" {
  for_each = toset([
    "composer.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
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

# ---------------------------------------------------------------------------
# Artifact Registry — Docker image repository
# ---------------------------------------------------------------------------
# Stores the container images for all Cloud Run Jobs.
# Images are named: <region>-docker.pkg.dev/<project>/<repo>/<image>:latest

resource "google_artifact_registry_repository" "images" {
  depends_on    = [google_project_service.apis]
  repository_id = var.ar_repository
  location      = var.region
  format        = "DOCKER"
  description   = "Docker images for stock market pipeline Cloud Run jobs"
}

# ---------------------------------------------------------------------------
# Service Account — Cloud Run job runner
# ---------------------------------------------------------------------------
# A dedicated SA (not the default compute SA) scoped to only what the
# pipeline needs: read/write GCS, read/write BigQuery.

resource "google_service_account" "pipeline_runner" {
  account_id   = "pipeline-runner"
  display_name = "Stock Market Pipeline Runner (Cloud Run)"
}

resource "google_project_iam_member" "runner_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# Composer 2 requires its own service agent to have the V2 Extension role
# so it can impersonate node SAs and set IAM policies on them.
resource "google_project_iam_member" "composer_agent_v2_ext" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
  depends_on = [google_project_service.apis]
}

# Composer worker role — required for the node SA running Composer tasks.
resource "google_project_iam_member" "runner_composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# Cloud Services SA needs Editor for Composer environment provisioning.
resource "google_project_iam_member" "cloud_services_editor" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${data.google_project.project.number}@cloudservices.gserviceaccount.com"
  depends_on = [google_project_service.apis]
}

# GKE node SA roles required for Composer pods to become healthy.
resource "google_project_iam_member" "runner_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_monitoring_viewer" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_metadata_writer" {
  project = var.project_id
  role    = "roles/stackdriver.resourceMetadata.writer"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "runner_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

data "google_project" "project" {
  project_id = var.project_id
}

# ---------------------------------------------------------------------------
# Secret Manager — Kaggle credentials
# ---------------------------------------------------------------------------
# Stores KAGGLE_USERNAME and KAGGLE_KEY so they are never hardcoded in the
# Cloud Run Job definition. The pipeline-runner SA can access them at runtime.

resource "google_secret_manager_secret" "kaggle_username" {
  secret_id = "kaggle-username"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "kaggle_key" {
  secret_id = "kaggle-key"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------------------
# Cloud Run Job: Ingest (Kaggle download + GCS upload)
# ---------------------------------------------------------------------------

resource "google_cloud_run_v2_job" "ingest_job" {
  name       = "stock-ingest"
  location   = var.region
  depends_on = [google_project_service.apis]

  template {
    template {
      service_account = google_service_account.pipeline_runner.email
      timeout         = "2400s"
      max_retries     = 1

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/stock-ingest:latest"

        env {
          name  = "GCS_BUCKET"
          value = var.bucket_name
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name = "KAGGLE_USERNAME"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.kaggle_username.secret_id
              version = "latest"
            }
          }
        }
        env {
          name = "KAGGLE_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.kaggle_key.secret_id
              version = "latest"
            }
          }
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "8Gi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

# ---------------------------------------------------------------------------
# Cloud Run Job: Spark Processing
# ---------------------------------------------------------------------------

resource "google_cloud_run_v2_job" "spark_job" {
  name       = "stock-spark-processing"
  location   = var.region
  depends_on = [google_project_service.apis]

  template {
    template {
      service_account = google_service_account.pipeline_runner.email
      timeout         = "7200s"
      max_retries     = 1

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/stock-spark:latest"

        env {
          name  = "GCS_BUCKET"
          value = var.bucket_name
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }

        resources {
          limits = {
            cpu    = "4"
            memory = "16Gi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

# ---------------------------------------------------------------------------
# Cloud Run Job: BigQuery Load
# ---------------------------------------------------------------------------

resource "google_cloud_run_v2_job" "bq_load_job" {
  name       = "stock-bq-load"
  location   = var.region
  depends_on = [google_project_service.apis]

  template {
    template {
      service_account = google_service_account.pipeline_runner.email
      timeout         = "1800s"
      max_retries     = 1

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/stock-bq-load:latest"

        env {
          name  = "GCS_BUCKET"
          value = var.bucket_name
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "BQ_DATASET"
          value = var.bq_dataset_id
        }
        env {
          name  = "BQ_LOCATION"
          value = var.region
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

# ---------------------------------------------------------------------------
# Cloud Run Job: dbt Transformations
# ---------------------------------------------------------------------------

resource "google_cloud_run_v2_job" "dbt_job" {
  name       = "stock-dbt-run"
  location   = var.region
  depends_on = [google_project_service.apis]

  template {
    template {
      service_account = google_service_account.pipeline_runner.email
      timeout         = "1800s"
      max_retries     = 1

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/stock-dbt:latest"

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "BQ_DATASET"
          value = var.bq_dataset_id
        }
        env {
          name  = "BQ_LOCATION"
          value = var.region
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].template[0].containers[0].image]
  }
}

# ---------------------------------------------------------------------------
# Cloud Composer 2 — Managed Airflow
# ---------------------------------------------------------------------------
# Orchestrates the full pipeline. All heavy compute runs in Cloud Run Jobs;
# Composer only needs a small env to schedule and monitor tasks.

resource "google_composer_environment" "composer" {
  name       = var.composer_name
  region     = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2.9.3"

      env_variables = {
        # GCS_BUCKET is reserved by Composer — use STOCK_ prefix instead.
        STOCK_GCS_BUCKET = var.bucket_name
        STOCK_PROJECT_ID = var.project_id
        STOCK_BQ_DATASET = var.bq_dataset_id
        STOCK_GCP_REGION = var.region
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2.0
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 1.0
        count     = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    node_config {
      # Composer 3 still uses node_config for the service account.
      service_account = google_service_account.pipeline_runner.email
    }
  }

  depends_on = [
    google_project_service.apis,
    google_service_account.pipeline_runner,
    google_project_iam_member.runner_storage,
    google_project_iam_member.runner_bq_editor,
    google_project_iam_member.runner_bq_job,
    google_project_iam_member.composer_agent_v2_ext,
    google_project_iam_member.runner_composer_worker,
    google_project_iam_member.cloud_services_editor,
    google_project_iam_member.runner_log_writer,
    google_project_iam_member.runner_metric_writer,
    google_project_iam_member.runner_monitoring_viewer,
    google_project_iam_member.runner_metadata_writer,
  ]
}

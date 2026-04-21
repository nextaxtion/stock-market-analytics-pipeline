"""
Airflow DAG: stock_market_pipeline
===================================
Orchestrates the full batch data pipeline for the stock market analytics project.

Pipeline Flow:
--------------
  ingest_to_gcs          (Cloud Run Job: stock-ingest)
        ↓
  spark_processing        (Cloud Run Job: stock-spark-processing)
        ↓
  load_to_bigquery        (Cloud Run Job: stock-bq-load)
        ↓
  dbt_transformations     (Cloud Run Job: stock-dbt-run)

All compute runs as Cloud Run Jobs (serverless containers). Cloud Composer 2
acts purely as the scheduler and monitor — it submits job executions and waits
for them to complete, using CloudRunExecuteJobOperator.

Auth:
-----
  Cloud Composer uses its own Service Account with roles/run.developer.
  Each Cloud Run Job runs as the pipeline-runner SA (storage + BQ access).
  No key files are used anywhere — Workload Identity throughout.

Deployment:
-----------
  Upload this file to the Composer DAGs bucket:
    gsutil cp airflow/dags/stock_market_dag.py \
      $(terraform -chdir=terraform output -raw composer_dags_bucket)/dags/

  Trigger manually from the Airflow UI or:
    gcloud composer environments run stock-market-composer \
      --location us-central1 dags trigger -- stock_market_pipeline
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

# ---------------------------------------------------------------------------
# Configuration (injected by Composer as env variables — see Terraform)
# ---------------------------------------------------------------------------

GCP_PROJECT = os.environ.get("STOCK_PROJECT_ID", os.environ.get("GCP_PROJECT_ID", "dezoomcamp-486216"))
GCP_REGION  = os.environ.get("STOCK_GCP_REGION", os.environ.get("GCP_REGION", "us-central1"))

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "nextaxtion",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline: Kaggle → GCS → Spark → BigQuery → dbt (via Cloud Run Jobs)",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual trigger — appropriate for batch/demo
    catchup=False,
    tags=["stock-market", "dezoomcamp", "batch", "cloud-run"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 + 2: Ingest — Kaggle download + GCS upload
    # -----------------------------------------------------------------------
    # Cloud Run Job: stock-ingest
    # Runs scripts/ingest_data.py inside the stock-ingest container.
    # Idempotent: checks if files already exist in GCS before downloading.
    # Timeout: 40 minutes (Kaggle download + gsutil upload of ~2 GB)

    ingest_to_gcs = CloudRunExecuteJobOperator(
        task_id="ingest_to_gcs",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="stock-ingest",
        deferrable=False,
        polling_period_seconds=30,
        execution_timeout=timedelta(minutes=40),
    )

    # -----------------------------------------------------------------------
    # Task 3: Spark Processing
    # -----------------------------------------------------------------------
    # Cloud Run Job: stock-spark-processing  (4 vCPU / 16 GiB)
    # Runs spark/process_stock_data.py --mode gcs inside the stock-spark container.
    # Reads from gs://<bucket>/security-market-raw-data/
    # Writes Parquet to gs://<bucket>/processed/stock_prices.parquet/
    # Timeout: 2 hours (full 8K tickers, ~28M rows)

    spark_processing = CloudRunExecuteJobOperator(
        task_id="spark_processing",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="stock-spark-processing",
        deferrable=False,
        polling_period_seconds=60,
        execution_timeout=timedelta(hours=2, minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 4: Load to BigQuery
    # -----------------------------------------------------------------------
    # Cloud Run Job: stock-bq-load
    # Runs scripts/load_to_bigquery.py inside the stock-bq-load container.
    # Creates raw_daily_prices (partitioned + clustered) and raw_symbols_meta.
    # Timeout: 30 minutes

    load_to_bigquery = CloudRunExecuteJobOperator(
        task_id="load_to_bigquery",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="stock-bq-load",
        deferrable=False,
        polling_period_seconds=30,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 5: dbt Transformations
    # -----------------------------------------------------------------------
    # Cloud Run Job: stock-dbt-run
    # Runs dbt run && dbt test via docker/dbt_run_and_test.sh
    # Builds staging → core → aggregation layers in BigQuery.
    # Timeout: 30 minutes

    dbt_transformations = CloudRunExecuteJobOperator(
        task_id="dbt_transformations",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="stock-dbt-run",
        deferrable=False,
        polling_period_seconds=30,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Pipeline dependency chain
    # -----------------------------------------------------------------------

    (
        ingest_to_gcs
        >> spark_processing
        >> load_to_bigquery
        >> dbt_transformations
    )


"""
Airflow DAG: stock_market_pipeline
===================================
Orchestrates the full batch data pipeline for the stock market analytics project.

What is a DAG?
--------------
A DAG (Directed Acyclic Graph) defines a workflow as a sequence of tasks with
dependencies. Airflow reads this file, draws the pipeline visually, and executes
each task in order. If a task fails, downstream tasks are skipped, and Airflow
retries the failed task (configurable).

Pipeline Flow:
--------------
  download_from_kaggle
        ↓
  upload_to_gcs
        ↓
  run_spark_processing
        ↓
  load_to_bigquery
        ↓
  run_dbt_transformations

Task Types Used:
----------------
  BashOperator     : Runs a shell command (used for spark-submit, dbt, gsutil)
  PythonOperator   : Runs a Python callable (used for Kaggle download + GCS upload)

Schedule:
---------
  schedule=None  → Manual trigger only (this is correct for a batch/demo project).
  In production you'd set schedule="@daily" or schedule="0 6 * * *" (6am daily).

Deployment Note:
----------------
  This DAG is designed for Cloud Composer 2 (managed Airflow on GCP).
  All tasks assume the Composer environment has:
    - GOOGLE_APPLICATION_CREDENTIALS configured via Workload Identity
    - KAGGLE_API_TOKEN set as an Airflow variable or env var
    - The stock-market-analytics-pipeline repo cloned to the Composer DAGs bucket

  For local Airflow testing, set these environment variables before running:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
    export KAGGLE_API_TOKEN=your_kaggle_token
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GCS_BUCKET        = "dezoomcampstore"
GCS_RAW_PATH      = f"gs://{GCS_BUCKET}/security-market-raw-data"
GCS_PROCESSED     = f"gs://{GCS_BUCKET}/processed"
BQ_PROJECT        = "dezoomcamp-486216"
BQ_DATASET        = "dezoomcampds"
KAGGLE_DATASET    = "jacksoncrow/stock-market-dataset"
LOCAL_DATA_DIR    = "/tmp/stock_data"
REPO_DIR          = "/home/nextaxtion/stock-market-analytics-pipeline"  # adjust for Composer

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
#
# Concept: Default Args
# ---------------------
# These settings apply to EVERY task in the DAG unless a task overrides them.
#
#  owner:            Who is responsible for this DAG (shown in Airflow UI)
#  depends_on_past:  If True, a task won't run if the previous day's run failed.
#                    Set False for manual/batch pipelines.
#  email_on_failure: Set True in production to get alerts on task failure.
#  retries:          Automatically retry a failed task N times before marking it failed.
#  retry_delay:      Wait time between retry attempts.
#

default_args = {
    "owner": "nextaxtion",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
#
# Concept: DAG parameters
# -----------------------
#  dag_id:        Unique name shown in the Airflow UI
#  start_date:    When this DAG became "active". Airflow will not backfill
#                 past runs when schedule=None.
#  schedule:      None = manual trigger only. "@daily" / cron for automation.
#  catchup:       False = don't run missed historical runs on first deploy.
#  tags:          Labels visible in the Airflow UI for filtering.
#

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline: Kaggle → GCS → Spark → BigQuery → dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,       # Manual trigger (appropriate for project/demo)
    catchup=False,
    tags=["stock-market", "dezoomcamp", "batch"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: download_from_kaggle
    # -----------------------------------------------------------------------
    #
    # Concept: PythonOperator
    # -----------------------
    # Runs a Python function inside the Airflow worker process.
    # Good for tasks that use Python libraries (like the kaggle API).
    #
    # What this task does:
    #   - Uses the Kaggle CLI to download the stock market dataset
    #   - Unzips into LOCAL_DATA_DIR
    #
    # In Cloud Composer, KAGGLE_USERNAME and KAGGLE_KEY are set as
    # Airflow environment variables in the Composer configuration.
    #

    download_from_kaggle = BashOperator(
        task_id="download_from_kaggle",
        bash_command=f"""
            set -e
            echo "=== Task 1: Download from Kaggle ==="
            mkdir -p {LOCAL_DATA_DIR}

            # kaggle CLI reads KAGGLE_USERNAME and KAGGLE_KEY from environment
            # or from ~/.kaggle/kaggle.json
            kaggle datasets download \
                -d {KAGGLE_DATASET} \
                --unzip \
                -p {LOCAL_DATA_DIR}

            echo "Download complete. Files:"
            ls {LOCAL_DATA_DIR}/ | head -10
            echo "Total files: $(ls {LOCAL_DATA_DIR}/stocks/ 2>/dev/null | wc -l) stocks, \
                               $(ls {LOCAL_DATA_DIR}/etfs/   2>/dev/null | wc -l) etfs"
        """,
        execution_timeout=timedelta(minutes=20),
    )

    # -----------------------------------------------------------------------
    # Task 2: upload_to_gcs
    # -----------------------------------------------------------------------
    #
    # Concept: BashOperator with gsutil
    # ----------------------------------
    # gsutil is Google Cloud's command-line tool for GCS operations.
    # `gsutil -m cp -r` uploads an entire directory recursively.
    # The -m flag enables parallel multi-threaded uploads (much faster).
    #
    # What this task does:
    #   - Uploads stocks/ and etfs/ directories to GCS
    #   - Uploads symbols_valid_meta.csv to GCS
    #
    # Why upload to GCS before processing?
    #   - GCS is the "data lake" — durable, cheap storage for raw data
    #   - Spark can read directly from GCS (when running on GCP)
    #   - The DAG is re-runnable: if Spark fails, raw data is already in GCS
    #

    upload_to_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command=f"""
            set -e
            echo "=== Task 2: Upload raw data to GCS ==="

            echo "Uploading stocks/..."
            gsutil -m cp -r {LOCAL_DATA_DIR}/stocks/ {GCS_RAW_PATH}/

            echo "Uploading etfs/..."
            gsutil -m cp -r {LOCAL_DATA_DIR}/etfs/ {GCS_RAW_PATH}/

            echo "Uploading metadata CSV..."
            gsutil cp {LOCAL_DATA_DIR}/symbols_valid_meta.csv {GCS_RAW_PATH}/

            echo "Upload complete."
            gsutil ls {GCS_RAW_PATH}/
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 3: run_spark_processing
    # -----------------------------------------------------------------------
    #
    # Concept: Spark on Airflow
    # --------------------------
    # In a production environment you'd use SparkSubmitOperator or
    # DataprocSubmitJobOperator (for managed Spark on GCP Dataproc).
    # For this project, we run the local PySpark script directly.
    #
    # What this task does:
    #   - Runs the PySpark script that reads all CSVs, computes metrics,
    #     and writes enriched Parquet back to GCS
    #   - --mode gcs: reads from GCS, writes to GCS (cloud-native)
    #
    # In Cloud Composer, the Python environment must have PySpark and Java.
    # Alternatively, this task could trigger a Dataproc job.
    #

    run_spark_processing = BashOperator(
        task_id="run_spark_processing",
        bash_command=f"""
            set -e
            echo "=== Task 3: Run PySpark processing ==="

            cd {REPO_DIR}
            source .venv/bin/activate

            python spark/process_stock_data.py --mode gcs

            echo "Spark processing complete."
            gsutil ls {GCS_PROCESSED}/stock_prices.parquet/ | head -10
        """,
        execution_timeout=timedelta(hours=2),
    )

    # -----------------------------------------------------------------------
    # Task 4: load_to_bigquery
    # -----------------------------------------------------------------------
    #
    # Concept: BigQuery LoadJob via Python script
    # -------------------------------------------
    # Our load_to_bigquery.py script starts a BigQuery LoadJob that reads
    # Parquet files directly from GCS and loads them into BigQuery tables.
    # This is a cloud-to-cloud operation — BigQuery does all the work.
    #
    # What this task does:
    #   - Creates dezoomcampds.raw_daily_prices (partitioned + clustered)
    #   - Creates dezoomcampds.raw_symbols_meta (flat lookup table)
    #

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"""
            set -e
            echo "=== Task 4: Load to BigQuery ==="

            cd {REPO_DIR}
            source .venv/bin/activate

            python scripts/load_to_bigquery.py

            echo "BigQuery load complete."
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 5: run_dbt_transformations
    # -----------------------------------------------------------------------
    #
    # Concept: dbt in Airflow
    # -----------------------
    # dbt (data build tool) runs SQL transformations in BigQuery.
    # We run dbt inside Docker so there are no Python environment conflicts.
    # `dbt run` executes all models (staging → core → aggregations).
    # `dbt test` runs the data quality tests defined in schema.yml.
    #
    # In Cloud Composer, you'd typically:
    #   Option A: Run dbt Core CLI directly (if installed in Composer)
    #   Option B: Use DbtCloudRunJobOperator (if using dbt Cloud)
    #   Option C: Run via Docker (our approach)
    #
    # The profiles.yml connects dbt to BigQuery using GOOGLE_APPLICATION_CREDENTIALS.
    #

    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"""
            set -e
            echo "=== Task 5: Run dbt transformations ==="

            cd {REPO_DIR}

            # Build the dbt Docker image (if not already built)
            docker compose -f docker/docker-compose.yml build

            # Run dbt models: staging → core → aggregations
            docker compose -f docker/docker-compose.yml run --rm dbt run

            # Run data quality tests
            docker compose -f docker/docker-compose.yml run --rm dbt test

            echo "dbt transformations and tests complete."
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Define task dependencies (the DAG edges)
    # -----------------------------------------------------------------------
    #
    # The >> operator means "runs before".
    # This creates the pipeline:
    #   download → upload → spark → bigquery_load → dbt
    #
    # Each arrow is a dependency edge in the DAG graph.
    # Airflow will not start a task until all its upstream tasks are DONE.
    #

    (
        download_from_kaggle
        >> upload_to_gcs
        >> run_spark_processing
        >> load_to_bigquery
        >> run_dbt_transformations
    )

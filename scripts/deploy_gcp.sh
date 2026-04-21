#!/usr/bin/env bash
##############################################################################
# deploy_gcp.sh — Full GCP deployment for the stock market analytics pipeline
##############################################################################
#
# What this script does (in order):
#   1. Sets project + auth context
#   2. terraform apply  — provisions all GCP resources
#   3. Stores Kaggle credentials in Secret Manager
#   4. Cloud Build       — builds and pushes all 4 Docker images to Artifact Registry
#   5. Uploads the DAG file to the Cloud Composer DAGs bucket
#   6. Waits for Composer to pick up the DAG, then triggers the pipeline
#
# Prerequisites:
#   - gcloud CLI authenticated: gcloud auth login && gcloud auth application-default login
#   - Terraform >= 1.3 installed
#   - KAGGLE_USERNAME and KAGGLE_KEY exported in your shell
#
# Usage:
#   export KAGGLE_USERNAME=your_kaggle_username
#   export KAGGLE_KEY=your_kaggle_key
#   bash scripts/deploy_gcp.sh
##############################################################################

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ID="dezoomcamp-486216"
REGION="us-central1"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ""
echo "================================================================"
echo "  Stock Market Analytics Pipeline — GCP Deployment"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Repo    : ${REPO_DIR}"
echo "================================================================"
echo ""

# ── Validate prerequisites ────────────────────────────────────────────────────
if [[ -z "${KAGGLE_USERNAME:-}" || -z "${KAGGLE_KEY:-}" ]]; then
  echo "ERROR: KAGGLE_USERNAME and KAGGLE_KEY must be set."
  echo "  export KAGGLE_USERNAME=your_username"
  echo "  export KAGGLE_KEY=your_api_key"
  exit 1
fi

command -v gcloud >/dev/null 2>&1 || { echo "ERROR: gcloud CLI not found."; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "ERROR: terraform not found."; exit 1; }

# Set default project
gcloud config set project "${PROJECT_ID}"

# Enable Cloud Resource Manager API first — required by Terraform's google_project data source
# and by the service account impersonation checks. Safe to run even if already enabled.
echo "  Enabling Cloud Resource Manager API (required for Terraform)..."
gcloud services enable cloudresourcemanager.googleapis.com --project="${PROJECT_ID}" --quiet || true
echo "  Cloud Resource Manager API enabled."

# Set Application Default Credentials so Terraform can authenticate.
# Uses the service account key file (already present from earlier project phases).
GCP_KEY_FILE="${GCP_KEY_FILE:-/home/nextaxtion/deZoomcampWeek2/secrets/gcp-key.json}"
if [[ ! -f "${GCP_KEY_FILE}" ]]; then
  echo "ERROR: Service account key not found at ${GCP_KEY_FILE}"
  echo "  Set GCP_KEY_FILE=/path/to/your/key.json and re-run."
  exit 1
fi
export GOOGLE_APPLICATION_CREDENTIALS="${GCP_KEY_FILE}"
echo "  Using credentials: ${GCP_KEY_FILE}"

# ── Step 1: Terraform — provision infrastructure ──────────────────────────────
echo ""
echo "── Step 1: Terraform apply ──────────────────────────────────────"
cd "${REPO_DIR}/terraform"

# Write terraform.tfvars if it doesn't exist
if [[ ! -f terraform.tfvars ]]; then
  cat > terraform.tfvars <<EOF
project_id       = "${PROJECT_ID}"
region           = "${REGION}"
bucket_name      = "dezoomcampstore"
bq_dataset_id    = "dezoomcampds"
ar_repository    = "stock-pipeline"
composer_name    = "stock-market-composer"
credentials_file = "${GCP_KEY_FILE}"
EOF
  echo "  Created terraform.tfvars"
fi

terraform init -upgrade
terraform apply -auto-approve

# Capture outputs
AR_URL=$(terraform output -raw artifact_registry_url)
DAGS_BUCKET=$(terraform output -raw composer_dags_bucket)
COMPOSER_URI=$(terraform output -raw composer_airflow_uri)
RUNNER_SA=$(terraform output -raw pipeline_runner_sa)

echo ""
echo "  Artifact Registry : ${AR_URL}"
echo "  Composer DAGs GCS : ${DAGS_BUCKET}"
echo "  Airflow UI        : ${COMPOSER_URI}"
echo "  Pipeline Runner SA: ${RUNNER_SA}"

# Grant Composer's agent service account permission to execute Cloud Run jobs.
# The agent SA format is: service-<PROJECT_NUMBER>@cloudcomposer-accounts.iam.gserviceaccount.com
# We retrieve the project number via gcloud now that CRM API is enabled.
echo ""
echo "── Post-apply: Grant Composer agent SA Cloud Run access ────────"
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
COMPOSER_AGENT_SA="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_AGENT_SA}" \
  --role="roles/run.developer" --quiet || true
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_AGENT_SA}" \
  --role="roles/iam.serviceAccountUser" --quiet || true
echo "  Composer agent SA ${COMPOSER_AGENT_SA} granted roles/run.developer"

cd "${REPO_DIR}"

# ── Step 2: Store Kaggle credentials in Secret Manager ────────────────────────
echo ""
echo "── Step 2: Store Kaggle credentials in Secret Manager ──────────"

echo -n "${KAGGLE_USERNAME}" | gcloud secrets versions add kaggle-username \
  --data-file=- 2>/dev/null || \
echo -n "${KAGGLE_USERNAME}" | gcloud secrets versions add kaggle-username \
  --data-file=- || true

echo -n "${KAGGLE_KEY}" | gcloud secrets versions add kaggle-key \
  --data-file=- 2>/dev/null || \
echo -n "${KAGGLE_KEY}" | gcloud secrets versions add kaggle-key \
  --data-file=- || true

echo "  Kaggle credentials stored."

# ── Step 3: Configure Docker auth for Artifact Registry ───────────────────────
echo ""
echo "── Step 3: Configure Docker auth ───────────────────────────────"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ── Step 4: Cloud Build — build and push all Docker images ────────────────────
echo ""
echo "── Step 4: Cloud Build — build + push images ────────────────────"
gcloud builds submit "${REPO_DIR}" \
  --config="${REPO_DIR}/docker/cloudbuild.yaml" \
  --substitutions="_REGION=${REGION},_PROJECT=${PROJECT_ID},_REPO=stock-pipeline" \
  --timeout=3600

echo "  All images pushed to ${AR_URL}/"

# ── Step 5: Upload DAG to Composer ───────────────────────────────────────────
echo ""
echo "── Step 5: Upload DAG to Cloud Composer ─────────────────────────"
gsutil cp "${REPO_DIR}/airflow/dags/stock_market_dag.py" \
  "${DAGS_BUCKET}/dags/stock_market_dag.py"
echo "  DAG uploaded to ${DAGS_BUCKET}/dags/stock_market_dag.py"

# ── Step 6: Wait for Composer + trigger DAG ───────────────────────────────────
echo ""
echo "── Step 6: Trigger the pipeline ────────────────────────────────"
echo "  Waiting 90s for Composer to parse the DAG..."
sleep 90

gcloud composer environments run stock-market-composer \
  --location "${REGION}" \
  dags trigger -- stock_market_pipeline

echo ""
echo "================================================================"
echo "  Deployment complete!"
echo "  Monitor the pipeline at:"
echo "  ${COMPOSER_URI}"
echo "================================================================"
echo ""

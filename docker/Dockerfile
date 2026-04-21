##############################################################################
# Dockerfile — dbt Core with BigQuery adapter
##############################################################################
#
# Two usage modes:
#
#   1. Cloud Run Job (production):
#      - Runs the built-in run_and_test.sh entrypoint: dbt run && dbt test
#      - Authenticates via Cloud Run Workload Identity (Application Default Credentials)
#      - profiles.yml baked into image uses method: oauth (no key file)
#
#   2. Local development:
#      docker compose -f docker/docker-compose.yml run --rm dbt run
#      docker compose -f docker/docker-compose.yml run --rm dbt test
#
# Package versions pinned for reproducibility.
##############################################################################

FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-bigquery==1.8.*

WORKDIR /dbt

# Copy the dbt project files
COPY dbt/ /dbt/

# Bake in a Cloud Run-compatible profiles.yml that uses Application Default
# Credentials (Workload Identity). Values are injected via env vars at runtime.
# The local dev profiles.yml is NOT copied here (it contains a keyfile path).
COPY docker/dbt_profiles_cloudrun.yml /root/.dbt/profiles.yml

# Wrapper script: runs dbt run then dbt test in a single container execution.
COPY docker/dbt_run_and_test.sh /usr/local/bin/dbt_run_and_test.sh
RUN chmod +x /usr/local/bin/dbt_run_and_test.sh

# Default: full run + test (used by Cloud Run Job).
# Override CMD for local dev: docker compose run --rm dbt run
ENTRYPOINT ["/bin/bash", "/usr/local/bin/dbt_run_and_test.sh"]
CMD []

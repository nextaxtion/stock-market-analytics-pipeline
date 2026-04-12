##############################################################################
# Dockerfile — dbt Core with BigQuery adapter
##############################################################################
#
# Why Docker for dbt?
# -------------------
# dbt Core is a Python package. Without Docker, every team member and every
# CI environment would need to manually install the right Python version,
# the right dbt version, and the right BigQuery adapter version.
#
# With Docker:
#   - Zero setup: `docker compose run dbt run` works on any machine
#   - Reproducible: the same image runs locally, in CI, and in Cloud Composer
#   - Isolated: dbt's dependencies don't conflict with any other Python project
#
# Image choice: python:3.11-slim
#   - slim = minimal base image (no X11, no extra tools) → smaller image
#   - Python 3.11 = stable, compatible with current dbt-bigquery
#
# Package breakdown:
#   dbt-core      : Core dbt engine (compiles SQL, manages the DAG, runs tests)
#   dbt-bigquery  : BigQuery adapter (translates dbt's SQL to BigQuery dialect,
#                   handles auth, partition/cluster config, etc.)
#
##############################################################################

FROM python:3.11-slim

# Install system dependencies needed by the BigQuery adapter
# (google-cloud libraries use libssl for HTTPS connections)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-core and the BigQuery adapter
# Pin versions for reproducibility — these are the latest stable versions
# as of April 2026. Update when you need new dbt features.
RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-bigquery==1.8.*

# Set the working directory inside the container to /dbt
# This is where the dbt project files will be mounted
WORKDIR /dbt

# Copy the dbt project files into the image
# (docker-compose.yml also mounts ./dbt as a volume, so live edits are picked up)
COPY dbt/ /dbt/

# The entrypoint is the dbt CLI
# This means: `docker run my-image run` → executes `dbt run`
ENTRYPOINT ["dbt"]

# Default command if none is provided: run all models
CMD ["run"]

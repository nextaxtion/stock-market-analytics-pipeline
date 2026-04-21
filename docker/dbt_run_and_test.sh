#!/bin/bash
# dbt_run_and_test.sh
# Executed by the Cloud Run Job to run all dbt models then all tests.
# Exits non-zero on any failure so Cloud Run marks the job execution as failed.
set -euo pipefail

echo "=== dbt run ==="
dbt run --profiles-dir /root/.dbt --target prod

echo "=== dbt test ==="
dbt test --profiles-dir /root/.dbt --target prod

echo "=== dbt transformations complete ==="

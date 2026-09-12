#!/usr/bin/env bash
# Runs once, on an empty data dir. POSTGRES_DB (analytics) already exists;
# Airflow metadata and Metabase app state get their own databases so neither
# can collide with the warehouse.
set -euo pipefail

for db in "${AIRFLOW_DB}" "${METABASE_DB}"; do
  psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -c "CREATE DATABASE \"${db}\" OWNER \"${POSTGRES_USER}\";"
  echo "created database ${db}"
done

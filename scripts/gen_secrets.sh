#!/usr/bin/env bash
# Fill .env with random creds. Idempotent: only replaces placeholders.
set -euo pipefail

cd "$(dirname "$0")/.."

[[ -f .env ]] || { cp .env.example .env; echo "created .env from .env.example"; }

# Fernet needs urlsafe base64
rand_b64() { openssl rand -base64 "${1:-32}" | tr '+/' '-_' | tr -d '=\n'; }
rand_pw()  { openssl rand -base64 24 | tr -d '/+=\n' | cut -c1-24; }

set_var() {
  local key="$1" val="$2" current
  current=$(grep -E "^${key}=" .env | head -1 | cut -d= -f2- || true)
  if [[ -z "$current" || "$current" == "changeme" ]]; then
    sed -i "s|^${key}=.*|${key}=${val}|" .env
    echo "  set ${key}"
  else
    echo "  kept ${key} (already customised)"
  fi
}

echo "generating secrets:"
set_var POSTGRES_PASSWORD     "$(rand_pw)"
set_var MINIO_ROOT_PASSWORD   "$(rand_pw)"
set_var AIRFLOW_ADMIN_PASSWORD "$(rand_pw)"
set_var METABASE_ADMIN_PASSWORD "$(rand_pw)"
set_var AIRFLOW_FERNET_KEY    "$(rand_b64 32)"
set_var AIRFLOW_SECRET_KEY    "$(rand_b64 32)"

# Airflow writes logs as this uid.
sed -i "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" .env
echo "  set AIRFLOW_UID=$(id -u)"

echo
echo ".env ready. It is gitignored — never commit it."

#!/usr/bin/env bash
# Fill .env with random creds. Idempotent: only replaces placeholders.
set -euo pipefail

cd "$(dirname "$0")/.."

[[ -f .env ]] || { cp .env.example .env; echo "created .env from .env.example"; }

# Postgres only applies POSTGRES_PASSWORD when it initialises an empty data
# directory. If a volume from a previous run survives, new credentials will not
# be applied to it and every service fails with "password authentication
# failed" — a confusing error a long way from its cause.
if [[ -z "${SKIP_VOLUME_CHECK:-}" ]] \
   && docker volume ls -q --filter name=mini-data-platform_pgdata 2>/dev/null | grep -q .; then
  current=$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2-)
  if [[ -z "$current" || "$current" == "changeme" ]]; then
    echo "refusing to generate new credentials:" >&2
    echo "  the volume mini-data-platform_pgdata already exists and was" >&2
    echo "  initialised with different credentials, which Postgres will keep." >&2
    echo >&2
    echo "  either restore the .env those volumes were created with," >&2
    echo "  or discard the data:  make nuke && make secrets" >&2
    exit 1
  fi
fi

# Fernet needs urlsafe base64
rand_b64() { openssl rand -base64 "${1:-32}" | tr '+/' '-_' | tr -d '=\n'; }

# Alphanumeric only, because these values are embedded in a Postgres DSN where
# @ : / would break parsing. Digits are appended explicitly: Metabase rejects a
# letters-only password as failing its complexity policy.
rand_pw() {
  local letters digits
  letters=$(openssl rand -base64 48 | tr -dc 'A-Za-z' | cut -c1-20)
  digits=$(printf '%d%d%d%d' $((RANDOM % 10)) $((RANDOM % 10)) $((RANDOM % 10)) $((RANDOM % 10)))
  printf '%s%s' "$letters" "$digits"
}

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

# AIRFLOW_UID stays 50000, the image's own airflow user. Matching the host uid
# is only needed for bind-mounted logs; we use a named volume, and a foreign
# uid makes Python skip the site-packages that /home/airflow owns.

echo
echo ".env ready. It is gitignored — never commit it."

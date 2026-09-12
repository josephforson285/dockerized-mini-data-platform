#!/usr/bin/env bash
# Preflight checks.
set -uo pipefail

fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; }

check() {  # check <label> <command...>
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label"; else bad "$label"; fi
}

echo "Toolchain"
for tool in docker git make jq curl openssl; do
  check "$tool" command -v "$tool"
done
check "docker compose plugin" docker compose version
check "docker daemon reachable without sudo" docker info

echo
echo "Python"
if [[ -x .venv/bin/python ]]; then
  ok "venv present ($(env -u PYTHONPATH .venv/bin/python --version 2>&1))"
else
  bad "no .venv — run: make venv"
fi

# venvs do not shield against PYTHONPATH.
if [[ -n "${PYTHONPATH:-}" ]]; then
  warn "PYTHONPATH is set; the Makefile strips it per-call"
  warn "never invoke .venv/bin/python directly"
else
  ok "PYTHONPATH is empty"
fi

if [[ -x .venv/bin/python ]]; then
  leaked=$(.venv/bin/python -c \
    "import sys; print(':'.join(p for p in sys.path if 'ros' in p.lower() or 'MLprojs' in p))" 2>/dev/null)
  if [[ -z "$leaked" ]]; then
    ok "no foreign paths leak into the venv"
  else
    warn "unguarded venv would import from: ${leaked}"
  fi
fi

echo
echo "Config"
if [[ -f .env ]]; then
  ok ".env present"
else
  bad ".env missing — run: cp .env.example .env && make secrets"
fi

echo
echo "Ports"
# Ports are declared once, in .env. No fallbacks here — a default would be a
# second source of truth and would drift.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
  for entry in \
    "${POSTGRES_PORT}:postgres" \
    "${AIRFLOW_PORT}:airflow" \
    "${MINIO_API_PORT}:minio-api" \
    "${MINIO_CONSOLE_PORT}:minio-console" \
    "${METABASE_PORT}:metabase"
  do
    port="${entry%%:*}"; name="${entry##*:}"
    if ss -ltn "sport = :${port}" 2>/dev/null | grep -q LISTEN; then
      warn "port ${port} (${name}) in use"
    else
      ok "port ${port} (${name}) free"
    fi
  done
else
  warn "skipped — no .env"
fi

echo
echo "Disk"
avail=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
if (( avail >= 10 )); then
  ok "${avail}G free on /"
else
  bad "only ${avail}G free on / — need ~10G"
fi

echo
if (( fail )); then
  echo "doctor: FAILED"
  exit 1
fi
echo "doctor: all good"

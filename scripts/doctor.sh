#!/usr/bin/env bash
# Preflight checks.
set -uo pipefail

fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; }

echo "Toolchain"
for tool in docker git make jq curl; do
  if command -v "$tool" >/dev/null 2>&1; then ok "$tool"; else bad "$tool missing"; fi
done
docker compose version >/dev/null 2>&1 && ok "docker compose" || bad "docker compose plugin missing"
docker info >/dev/null 2>&1 && ok "docker daemon reachable without sudo" \
  || bad "cannot reach docker daemon (is the user in the docker group?)"

echo
echo "Python"
if [[ -x .venv/bin/python ]]; then
  ok "venv present ($(env -u PYTHONPATH .venv/bin/python --version 2>&1))"
else
  bad "no .venv — run: make venv"
fi

# venvs do not shield against PYTHONPATH.
if [[ -n "${PYTHONPATH:-}" ]]; then
  warn "PYTHONPATH is set: ${PYTHONPATH}"
  warn "the Makefile strips it per-call; never invoke .venv/bin/python directly"
else
  ok "PYTHONPATH is empty"
fi

if [[ -x .venv/bin/python ]]; then
  leaked=$(.venv/bin/python -c \
    "import sys; print(':'.join(p for p in sys.path if 'ros' in p.lower() or 'MLprojs' in p))" 2>/dev/null)
  [[ -z "$leaked" ]] && ok "no foreign paths leak into the venv" \
                     || warn "unguarded venv would import from: ${leaked}"
fi

echo
echo "Config"
[[ -f .env ]] && ok ".env present" || bad ".env missing — run: cp .env.example .env && make secrets"

echo
echo "Ports"
# shellcheck disable=SC1091
[[ -f .env ]] && set -a && . ./.env && set +a
for entry in \
  "${POSTGRES_PORT:-5435}:postgres" \
  "${AIRFLOW_PORT:-8082}:airflow" \
  "${MINIO_API_PORT:-9002}:minio-api" \
  "${MINIO_CONSOLE_PORT:-9003}:minio-console" \
  "${METABASE_PORT:-3001}:metabase"
do
  port="${entry%%:*}"; name="${entry##*:}"
  if ss -ltn "sport = :${port}" 2>/dev/null | grep -q LISTEN; then
    warn "port ${port} (${name}) already in use"
  else
    ok "port ${port} (${name}) free"
  fi
done

echo
echo "Disk"
avail=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
(( avail >= 10 )) && ok "${avail}G free on /" || bad "only ${avail}G free on / — need ~10G"

echo
if (( fail )); then echo "doctor: FAILED"; exit 1; fi
echo "doctor: all good"

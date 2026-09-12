"""Provision Metabase headlessly.

Creates the admin account and registers the analytics database through the
setup API, so a fresh stack comes up fully configured and CI can verify the
last hop of the pipeline. No setup wizard, no clicking.

Idempotent: on an already-provisioned instance it logs in and only adds the
database if it is missing.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import requests

from mini_platform.config import load_env

TIMEOUT = 30
DB_DISPLAY_NAME = "Analytics"


def _base_url() -> str:
    return os.environ.get("METABASE_URL") or f"http://127.0.0.1:{os.environ['METABASE_PORT']}"


def _db_payload() -> dict:
    # Metabase reaches Postgres over the compose network, not the published port.
    return {
        "engine": "postgres",
        "name": DB_DISPLAY_NAME,
        "details": {
            "host": os.environ.get("METABASE_PG_HOST", "postgres"),
            "port": int(os.environ.get("METABASE_PG_PORT", "5432")),
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "ssl": False,
        },
    }


def wait_until_healthy(base: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{base}/api/health", timeout=5)
            if r.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise TimeoutError(f"Metabase did not become healthy within {timeout}s")


def _setup_token(base: str) -> str | None:
    props = requests.get(f"{base}/api/session/properties", timeout=TIMEOUT).json()
    return props.get("setup-token")


def _first_time_setup(base: str, token: str) -> str:
    payload = {
        "token": token,
        "user": {
            "first_name": "Platform",
            "last_name": "Admin",
            "email": os.environ["METABASE_ADMIN_EMAIL"],
            "password": os.environ["METABASE_ADMIN_PASSWORD"],
            "site_name": os.environ.get("METABASE_SITE_NAME", "Mini Data Platform"),
        },
        "prefs": {
            "site_name": os.environ.get("METABASE_SITE_NAME", "Mini Data Platform"),
            "allow_tracking": False,
        },
        "database": _db_payload(),
    }
    r = requests.post(f"{base}/api/setup", json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()["id"]


def _login(base: str) -> str:
    r = requests.post(
        f"{base}/api/session",
        json={
            "username": os.environ["METABASE_ADMIN_EMAIL"],
            "password": os.environ["METABASE_ADMIN_PASSWORD"],
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["id"]


def _databases(base: str, session: str) -> list[dict]:
    r = requests.get(
        f"{base}/api/database", headers={"X-Metabase-Session": session}, timeout=TIMEOUT
    )
    r.raise_for_status()
    body = r.json()
    return body["data"] if isinstance(body, dict) else body


def ensure_database(base: str, session: str) -> int:
    existing = {db["name"]: db["id"] for db in _databases(base, session)}
    if DB_DISPLAY_NAME in existing:
        return existing[DB_DISPLAY_NAME]

    r = requests.post(
        f"{base}/api/database",
        json=_db_payload(),
        headers={"X-Metabase-Session": session},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["id"]


def sync_schema(base: str, session: str, db_id: int) -> None:
    requests.post(
        f"{base}/api/database/{db_id}/sync_schema",
        headers={"X-Metabase-Session": session},
        timeout=TIMEOUT,
    ).raise_for_status()


def provision() -> int:
    load_env()
    base = _base_url()
    wait_until_healthy(base)

    token = _setup_token(base)
    if token:
        session = _first_time_setup(base, token)
        print("metabase: admin created, analytics database registered")
    else:
        session = _login(base)
        print("metabase: already provisioned, verifying database")

    db_id = ensure_database(base, session)
    sync_schema(base, session, db_id)
    print(f"metabase: '{DB_DISPLAY_NAME}' is database id {db_id}, schema sync requested")
    return db_id


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--wait-only", action="store_true", help="only wait for /api/health")
    args = p.parse_args()

    load_env()
    if args.wait_only:
        wait_until_healthy(_base_url())
        print("metabase: healthy")
        return
    try:
        provision()
    except Exception as exc:  # surface the API body, which carries the real reason
        detail = getattr(getattr(exc, "response", None), "text", "")
        print(f"metabase provisioning failed: {exc} {detail}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

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
import yaml

from mini_platform.config import load_env
from mini_platform.settings import REPO_ROOT, get

TIMEOUT = 30
DB_DISPLAY_NAME = "Analytics"
DASHBOARD_CONFIG = REPO_ROOT / "config" / "dashboard.yml"


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


def _headers(session: str) -> dict[str, str]:
    return {"X-Metabase-Session": session}


def remove_bundled_examples(base: str, session: str) -> None:
    """Metabase ships a demo database plus an Examples collection of dashboards.
    Remove both, so the instance shows only this platform's data."""
    for db in _databases(base, session):
        if db.get("is_sample") or db["name"] == "Sample Database":
            requests.delete(
                f"{base}/api/database/{db['id']}", headers=_headers(session), timeout=TIMEOUT
            )
            print("metabase: removed the bundled Sample Database")

    collections = requests.get(
        f"{base}/api/collection", headers=_headers(session), timeout=TIMEOUT
    ).json()
    for col in collections:
        if col.get("is_sample") and not col.get("archived"):
            requests.put(
                f"{base}/api/collection/{col['id']}",
                json={"archived": True},
                headers=_headers(session),
                timeout=TIMEOUT,
            )
            print(f"metabase: archived the bundled '{col['name']}' collection")


def _dashboard_spec(cfg) -> dict:
    spec = yaml.safe_load(DASHBOARD_CONFIG.read_text())
    for card in spec["cards"]:
        card["sql"] = card["sql"].format(fact=cfg.fact_table, rejects=cfg.reject_table)
    return spec


def _existing_dashboard(base: str, session: str, name: str) -> int | None:
    for d in requests.get(
        f"{base}/api/dashboard", headers=_headers(session), timeout=TIMEOUT
    ).json():
        if d["name"] == name:
            return d["id"]
    return None


def _create_card(base: str, session: str, db_id: int, card: dict) -> int:
    payload = {
        "name": card["name"],
        "display": card["display"],
        "dataset_query": {
            "type": "native",
            "native": {"query": card["sql"]},
            "database": db_id,
        },
        "visualization_settings": {},
    }
    r = requests.post(f"{base}/api/card", json=payload, headers=_headers(session), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()["id"]


def ensure_dashboard(base: str, session: str, db_id: int, cfg) -> int:
    """Create the dashboard and its cards. Rebuilt from config if absent."""
    spec = _dashboard_spec(cfg)
    name = spec["dashboard"]["name"]

    existing = _existing_dashboard(base, session, name)
    if existing is not None:
        print(f"metabase: dashboard '{name}' already exists (id {existing})")
        return existing

    r = requests.post(
        f"{base}/api/dashboard",
        json={"name": name, "description": spec["dashboard"].get("description", "")},
        headers=_headers(session),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    dash_id = r.json()["id"]

    dashcards = []
    for i, card in enumerate(spec["cards"]):
        card_id = _create_card(base, session, db_id, card)
        dashcards.append(
            {
                "id": -(i + 1),  # negative ids mark cards new to this dashboard
                "card_id": card_id,
                "row": card["row"],
                "col": card["col"],
                "size_x": card["size_x"],
                "size_y": card["size_y"],
            }
        )

    r = requests.put(
        f"{base}/api/dashboard/{dash_id}",
        json={"dashcards": dashcards},
        headers=_headers(session),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    print(f"metabase: dashboard '{name}' created with {len(dashcards)} cards")
    return dash_id


def provision() -> int:
    load_env()
    cfg = get()
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

    remove_bundled_examples(base, session)
    ensure_dashboard(base, session, db_id, cfg)
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

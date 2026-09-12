"""Fixtures for the end-to-end test. Requires a running stack (`make up`)."""

from __future__ import annotations

import os
import time
import uuid

import psycopg
import pytest
import requests

from mini_platform.config import PostgresSettings, load_env
from mini_platform.settings import get

POLL_SECONDS = 2
RUN_TIMEOUT = 300


@pytest.fixture(scope="session", autouse=True)
def _env() -> None:
    load_env()


@pytest.fixture(scope="session")
def cfg():
    return get()


@pytest.fixture
def conn():
    with psycopg.connect(PostgresSettings.from_env().dsn) as c:
        yield c


class AirflowClient:
    def __init__(self) -> None:
        self.base = (
            os.environ.get("AIRFLOW_URL") or f"http://127.0.0.1:{os.environ['AIRFLOW_PORT']}"
        )
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._token()}"

    def _token(self) -> str:
        r = requests.post(
            f"{self.base}/auth/token",
            json={
                "username": os.environ["AIRFLOW_ADMIN_USER"],
                "password": os.environ["AIRFLOW_ADMIN_PASSWORD"],
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def unpause(self, dag_id: str) -> None:
        r = self._session.patch(
            f"{self.base}/api/v2/dags/{dag_id}", json={"is_paused": False}, timeout=30
        )
        r.raise_for_status()

    def trigger(self, dag_id: str, conf: dict) -> str:
        r = self._session.post(
            f"{self.base}/api/v2/dags/{dag_id}/dagRuns",
            json={"logical_date": None, "conf": conf},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["dag_run_id"]

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        r = self._session.get(
            f"{self.base}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances", timeout=30
        )
        r.raise_for_status()
        return {t["task_id"]: t["state"] for t in r.json()["task_instances"]}

    def run_to_completion(self, dag_id: str, conf: dict) -> str:
        """Trigger and wait. On failure the per-task states are in the message."""
        run_id = self.trigger(dag_id, conf)
        deadline = time.time() + RUN_TIMEOUT
        state = "queued"
        while time.time() < deadline:
            r = self._session.get(f"{self.base}/api/v2/dags/{dag_id}/dagRuns/{run_id}", timeout=30)
            r.raise_for_status()
            state = r.json()["state"]
            if state in ("success", "failed"):
                break
            time.sleep(POLL_SECONDS)

        if state != "success":
            raise AssertionError(
                f"dag run {run_id} ended '{state}'; tasks: {self.task_states(dag_id, run_id)}"
            )
        return run_id


class MetabaseClient:
    def __init__(self) -> None:
        self.base = (
            os.environ.get("METABASE_URL") or f"http://127.0.0.1:{os.environ['METABASE_PORT']}"
        )
        self.session = self._login()

    def _login(self) -> str:
        r = requests.post(
            f"{self.base}/api/session",
            json={
                "username": os.environ["METABASE_ADMIN_EMAIL"],
                "password": os.environ["METABASE_ADMIN_PASSWORD"],
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["id"]

    def get(self, path: str) -> dict | list:
        r = requests.get(
            f"{self.base}{path}", headers={"X-Metabase-Session": self.session}, timeout=30
        )
        r.raise_for_status()
        return r.json()

    def post(self, path: str) -> dict:
        r = requests.post(
            f"{self.base}{path}", headers={"X-Metabase-Session": self.session}, timeout=60
        )
        return r.json() if r.ok else {"status": "error", "error": r.status_code}

    def sync_and_await_table(self, db_id: int, table: str, timeout: int = 120) -> set[str]:
        """Metabase discovers tables on its own schedule; a fresh instance has
        not seen them yet. Ask for a sync and wait for the table to appear."""
        deadline = time.time() + timeout
        seen: set[str] = set()
        while time.time() < deadline:
            self.post(f"/api/database/{db_id}/sync_schema")
            meta = self.get(f"/api/database/{db_id}/metadata")
            seen = {t["name"] for t in meta.get("tables", [])}
            if table in seen:
                return seen
            time.sleep(4)
        return seen

    def databases(self) -> list[dict]:
        body = self.get("/api/database")
        return body["data"] if isinstance(body, dict) else body


@pytest.fixture(scope="session")
def airflow() -> AirflowClient:
    return AirflowClient()


@pytest.fixture(scope="session")
def metabase() -> MetabaseClient:
    return MetabaseClient()


@pytest.fixture
def batch_id() -> str:
    """Unique per test, so runs never collide and never rely on a clean stack."""
    return f"e2e_{uuid.uuid4().hex[:10]}"

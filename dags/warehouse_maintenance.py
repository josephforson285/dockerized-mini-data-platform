"""Housekeeping, kept apart from ingestion.

Maintenance runs on its own cadence and must not fail the pipeline that loads
data, so it is a separate DAG rather than a task on sales_pipeline.
"""

from __future__ import annotations

import datetime as dt
import logging

import psycopg
from airflow.sdk import dag, task

from mini_platform import warehouse
from mini_platform.config import PostgresSettings, load_env
from mini_platform.settings import get

log = logging.getLogger(__name__)


@dag(
    dag_id="warehouse_maintenance",
    schedule="@daily",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=1)},
    tags=["maintenance", "postgres"],
)
def warehouse_maintenance():
    @task(doc_md="Deletes quarantined rows older than warehouse.reject_retention_days.")
    def prune_rejects() -> dict:
        load_env()
        cfg = get()
        with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
            warehouse.ensure_schema(conn, cfg)
            removed = warehouse.prune_rejects(conn, cfg)
        log.info("pruned %d rejected rows older than %d days", removed, cfg.reject_retention_days)
        return {"removed": removed, "retention_days": cfg.reject_retention_days}

    prune_rejects()


warehouse_maintenance()

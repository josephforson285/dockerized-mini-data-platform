"""Sales ingestion: MinIO -> clean -> Postgres.

The DAG is deliberately thin. All cleaning logic lives in mini_platform, which
is unit-tested without Airflow; these tasks only move data between systems.
"""

from __future__ import annotations

import datetime as dt
import logging

import psycopg
from airflow.sdk import Param, dag, task

from mini_platform import storage, warehouse
from mini_platform.config import PostgresSettings, load_env
from mini_platform.settings import get
from mini_platform.transforms import assert_quality, clean

log = logging.getLogger(__name__)


def _batch_id(key: str) -> str:
    return key.rsplit("/", 1)[-1].removesuffix(".csv")


@dag(
    dag_id="sales_pipeline",
    schedule=None,
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # The load is idempotent, so retrying a half-finished task is safe.
    default_args={"retries": 2, "retry_delay": dt.timedelta(seconds=10)},
    tags=["sales", "minio", "postgres"],
    params={
        "batch_id": Param(
            None,
            type=["null", "string"],
            description="Process only this batch. Omit to pick up everything unloaded.",
        ),
        "reload": Param(
            False,
            type="boolean",
            description="Re-process batches already in the warehouse.",
        ),
    },
)
def sales_pipeline():
    @task
    def discover(params: dict) -> list[str]:
        """New files in MinIO, minus what the warehouse already holds.

        Discovery is based on warehouse state rather than a sensor's memory, so
        a wiped scheduler or a replayed run still does the right thing.
        """
        load_env()
        cfg = get()
        keys = [k for k in storage.list_keys(cfg.raw_prefix) if k.endswith(".csv")]

        wanted = params.get("batch_id")
        if wanted:
            keys = [k for k in keys if _batch_id(k) == wanted]
            if not keys:
                raise FileNotFoundError(f"no object in MinIO for batch {wanted}")

        if not params.get("reload"):
            with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
                warehouse.ensure_schema(conn, cfg)
                done = warehouse.loaded_batches(conn, cfg)
            keys = [k for k in keys if _batch_id(k) not in done]

        log.info("discovered %d batch(es): %s", len(keys), keys)
        return keys

    @task(max_active_tis_per_dag=2)
    def ingest(key: str) -> dict:
        load_env()
        cfg = get()
        batch_id = _batch_id(key)

        raw = storage.read_csv(key)
        good, bad = clean(raw, batch_id=batch_id, cfg=cfg)

        # Fail before writing: a mostly-bad batch is an upstream incident, and
        # loading it would publish a misleading partial dataset.
        reject_ratio = assert_quality(good, bad, cfg)

        with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
            warehouse.ensure_schema(conn, cfg)
            loaded = warehouse.load_clean(good, batch_id, conn, cfg)
            rejected = warehouse.load_rejects(bad, batch_id, conn, cfg)

        log.info("%s: %d loaded, %d rejected", batch_id, loaded, rejected)
        return {
            "batch_id": batch_id,
            "read": len(raw),
            "loaded": loaded,
            "rejected": rejected,
            "reject_ratio": round(reject_ratio, 4),
        }

    @task
    def summarise(results: list[dict]) -> dict:
        total = {
            "batches": len(results),
            "read": sum(r["read"] for r in results),
            "loaded": sum(r["loaded"] for r in results),
            "rejected": sum(r["rejected"] for r in results),
        }
        log.info("run summary: %s", total)
        return total

    summarise(ingest.expand(key=discover()))


sales_pipeline()

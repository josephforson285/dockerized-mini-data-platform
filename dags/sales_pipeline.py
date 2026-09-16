"""Sales ingestion: MinIO -> clean -> Postgres. Thin by design; logic lives in
mini_platform, which is tested without Airflow."""

from __future__ import annotations

import datetime as dt
import logging

import psycopg
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, task

from mini_platform import storage, warehouse
from mini_platform.config import PostgresSettings, load_env
from mini_platform.settings import get
from mini_platform.transforms import QualityGateFailed, assert_quality, clean

log = logging.getLogger(__name__)


def _batch_id(key: str) -> str:
    return key.rsplit("/", 1)[-1].removesuffix(".csv")


@dag(
    dag_id="sales_pipeline",
    schedule=None,
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Transient faults only; backoff spans an outage and breaks lockstep.
    default_args={
        "retries": 3,
        "retry_delay": dt.timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": dt.timedelta(minutes=5),
        "execution_timeout": dt.timedelta(minutes=20),
    },
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
    @task(doc_md="Lists CSVs in MinIO and subtracts batches already in the warehouse.")
    def discover(params: dict) -> list[str]:
        """New files in MinIO, minus every batch the ledger has attempted."""
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
                done = warehouse.attempted_batches(conn, cfg)
            keys = [k for k in keys if _batch_id(k) not in done]

        log.info("discovered %d batch(es): %s", len(keys), keys)
        return keys

    @task(
        max_active_tis_per_dag=2,
        doc_md="Reads one batch, cleans it, checks the reject ratio, then loads "
        "clean rows and quarantines the rest.",
    )
    def ingest(key: str) -> dict:
        load_env()
        cfg = get()
        batch_id = _batch_id(key)

        raw = storage.read_csv(key)
        good, bad = clean(raw, batch_id=batch_id, cfg=cfg)

        # Fail before writing: a partial load looks like success.
        try:
            reject_ratio = assert_quality(good, bad, cfg)
        except QualityGateFailed as exc:
            # Record, then fail without retrying: it fails the same way.
            with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
                warehouse.ensure_schema(conn, cfg)
                warehouse.record_run(
                    batch_id,
                    len(raw),
                    0,
                    len(bad),
                    round(len(bad) / len(raw), 4) if len(raw) else 0.0,
                    conn,
                    cfg,
                    status="quality_failed",
                    detail=str(exc),
                )
            raise AirflowFailException(str(exc)) from exc

        with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
            warehouse.ensure_schema(conn, cfg)
            loaded = warehouse.load_clean(good, batch_id, conn, cfg)
            rejected = warehouse.load_rejects(bad, batch_id, conn, cfg)
            ratio = rejected / (loaded + rejected) if loaded + rejected else 0.0
            warehouse.record_run(batch_id, len(raw), loaded, rejected, round(ratio, 4), conn, cfg)

        log.info("%s: %d loaded, %d rejected", batch_id, loaded, rejected)
        return {
            "batch_id": batch_id,
            "read": len(raw),
            "loaded": loaded,
            "rejected": rejected,
            "reject_ratio": round(reject_ratio, 4),
        }

    @task(doc_md="Re-checks what actually landed in the fact table, after the load.")
    def verify_load(results: list[dict]) -> dict:
        """Check the table, not the frame — catches a bug in the load itself."""
        load_env()
        cfg = get()
        problems = []
        with psycopg.connect(PostgresSettings.from_env().dsn) as conn:
            for r in results:
                outcome = warehouse.verify_batch(r["batch_id"], conn, cfg)
                log.info("%s: %d rows verified", r["batch_id"], outcome["rows"])
                if outcome["failures"]:
                    problems.append(f"{r['batch_id']}: {'; '.join(outcome['failures'])}")

        if problems:
            # Already written: a defect, not something a retry fixes.
            raise AirflowFailException("post-load verification failed — " + " | ".join(problems))
        return {"batches_verified": len(results)}

    @task(doc_md="Totals across every batch processed in this run.")
    def summarise(results: list[dict]) -> dict:
        total = {
            "batches": len(results),
            "read": sum(r["read"] for r in results),
            "loaded": sum(r["loaded"] for r in results),
            "rejected": sum(r["rejected"] for r in results),
        }
        log.info("run summary: %s", total)
        return total

    # Gates without relaying the payload: the mapped proxy is not serialisable.
    ingested = ingest.expand(key=discover())
    verify_load(ingested) >> summarise(ingested)


sales_pipeline()

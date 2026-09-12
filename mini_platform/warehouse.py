"""Postgres load.

Idempotent by construction: rows land in a temp staging table, the batch's
previous attempt is deleted, then the merge upserts on the natural key. Airflow
retries and manual replays therefore cannot double-count.
"""

from __future__ import annotations

import pandas as pd
import psycopg
from psycopg import sql

from mini_platform.settings import PipelineConfig, get

FACT_DDL = """
CREATE TABLE IF NOT EXISTS {fact} (
    order_id         text PRIMARY KEY,
    order_ts         timestamptz NOT NULL,
    customer_id      text NOT NULL,
    product_id       text NOT NULL,
    product_category text,
    quantity         integer NOT NULL,
    unit_price       numeric(12, 2) NOT NULL,
    currency         text NOT NULL,
    country          text,
    payment_method   text,
    revenue          numeric(14, 2) NOT NULL,
    batch_id         text NOT NULL,
    ingested_at      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS {fact_batch_ix} ON {fact} (batch_id);
CREATE INDEX IF NOT EXISTS {fact_ts_ix} ON {fact} (order_ts);
"""

REJECT_DDL = """
CREATE TABLE IF NOT EXISTS {reject} (
    reject_id      bigserial PRIMARY KEY,
    batch_id       text NOT NULL,
    reject_reason  text NOT NULL,
    payload        jsonb NOT NULL,
    rejected_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {reject_batch_ix} ON {reject} (batch_id);
"""


def _ident(name: str) -> sql.Identifier:
    return sql.Identifier(name)


def ensure_schema(conn: psycopg.Connection, cfg: PipelineConfig | None = None) -> None:
    cfg = cfg or get()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(FACT_DDL).format(
                fact=_ident(cfg.fact_table),
                fact_batch_ix=_ident(f"ix_{cfg.fact_table}_batch"),
                fact_ts_ix=_ident(f"ix_{cfg.fact_table}_ts"),
            )
        )
        cur.execute(
            sql.SQL(REJECT_DDL).format(
                reject=_ident(cfg.reject_table),
                reject_batch_ix=_ident(f"ix_{cfg.reject_table}_batch"),
            )
        )
    conn.commit()


def _rows(df: pd.DataFrame) -> list[tuple]:
    """pandas NA variants are not adaptable by psycopg; normalise to None."""
    frame = df.astype(object)
    frame = frame.where(pd.notna(frame), None)
    return list(frame.itertuples(index=False, name=None))


def load_clean(
    df: pd.DataFrame,
    batch_id: str,
    conn: psycopg.Connection,
    cfg: PipelineConfig | None = None,
) -> int:
    """Replace this batch's rows, upserting on the natural key. Returns rows written."""
    cfg = cfg or get()
    columns = list(cfg.contract.clean_columns)
    col_ids = sql.SQL(", ").join(_ident(c) for c in columns)
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=_ident(c)) for c in columns if c != "order_id"
    )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE stage_clean (LIKE {fact}) ON COMMIT DROP").format(
                fact=_ident(cfg.fact_table)
            )
        )
        copy_stmt = sql.SQL("COPY stage_clean ({cols}) FROM STDIN").format(cols=col_ids)
        with cur.copy(copy_stmt) as cp:
            for row in _rows(df.loc[:, columns]):
                cp.write_row(row)

        # Clear the prior attempt before merging, so a shrinking batch does not
        # leave orphaned rows behind.
        cur.execute(
            sql.SQL("DELETE FROM {fact} WHERE batch_id = %s").format(fact=_ident(cfg.fact_table)),
            (batch_id,),
        )
        cur.execute(
            sql.SQL(
                "INSERT INTO {fact} ({cols}) SELECT {cols} FROM stage_clean "
                "ON CONFLICT (order_id) DO UPDATE SET {updates}"
            ).format(fact=_ident(cfg.fact_table), cols=col_ids, updates=updates)
        )
        written = cur.rowcount
    conn.commit()
    return written


def load_rejects(
    df: pd.DataFrame,
    batch_id: str,
    conn: psycopg.Connection,
    cfg: PipelineConfig | None = None,
) -> int:
    """Quarantine bad rows as jsonb so a changing raw schema cannot break the load."""
    cfg = cfg or get()
    if df.empty:
        _delete_batch(conn, cfg.reject_table, batch_id)
        return 0

    payload_cols = [c for c in df.columns if c not in ("reject_reason", "batch_id")]
    records = df.loc[:, payload_cols].astype("string").astype(object)
    records = records.where(pd.notna(records), None)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {reject} WHERE batch_id = %s").format(
                reject=_ident(cfg.reject_table)
            ),
            (batch_id,),
        )
        copy_stmt = sql.SQL("COPY {reject} (batch_id, reject_reason, payload) FROM STDIN").format(
            reject=_ident(cfg.reject_table)
        )
        with cur.copy(copy_stmt) as cp:
            for reason, payload in zip(
                df["reject_reason"], records.to_dict("records"), strict=True
            ):
                cp.write_row((batch_id, reason, psycopg.types.json.Json(payload)))
    conn.commit()
    return len(df)


def _delete_batch(conn: psycopg.Connection, table: str, batch_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(t=_ident(table)), (batch_id,)
        )
    conn.commit()


def count_rows(conn: psycopg.Connection, table: str, batch_id: str | None = None) -> int:
    stmt = sql.SQL("SELECT count(*) FROM {t}").format(t=_ident(table))
    params: tuple = ()
    if batch_id is not None:
        stmt = sql.SQL("{base} WHERE batch_id = %s").format(base=stmt)
        params = (batch_id,)
    with conn.cursor() as cur:
        cur.execute(stmt, params)
        return cur.fetchone()[0]


def loaded_batches(conn: psycopg.Connection, cfg: PipelineConfig | None = None) -> set[str]:
    cfg = cfg or get()
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT DISTINCT batch_id FROM {t}").format(t=_ident(cfg.fact_table)))
        return {r[0] for r in cur.fetchall()}

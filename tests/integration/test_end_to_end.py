"""End-to-end: MinIO -> Airflow -> Postgres -> Metabase. Counts are asserted
against the generator manifest, not the transform's own output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from psycopg import sql

from data_generator.generate import generate
from mini_platform import storage, warehouse
from mini_platform.config import MinioSettings

pytestmark = pytest.mark.integration

DAG_ID = "sales_pipeline"
ROWS, SEED, CORRUPT, DUPLICATES = 600, 21, 48, 32


@pytest.fixture
def uploaded_batch(batch_id, cfg, conn, tmp_path: Path):
    """Generate a batch, upload it, and clean up both stores afterwards."""
    # A cold stack has no tables yet; teardown must not assume them.
    warehouse.ensure_schema(conn, cfg)

    df, manifest = generate(ROWS, SEED, CORRUPT, DUPLICATES, cfg)
    manifest["batch_id"] = batch_id

    csv_path = tmp_path / f"{batch_id}.csv"
    df.to_csv(csv_path, index=False)
    key = f"{cfg.raw_prefix}{batch_id}.csv"
    storage.upload(csv_path, key)

    yield manifest, key

    for table in (cfg.fact_table, cfg.reject_table, cfg.run_table):
        conn.execute(
            sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(t=sql.Identifier(table)),
            (batch_id,),
        )
    conn.commit()
    storage.client().delete_object(Bucket=MinioSettings.from_env().bucket, Key=key)


def _count(conn, table: str, batch_id: str) -> int:
    return conn.execute(
        sql.SQL("SELECT count(*) FROM {t} WHERE batch_id = %s").format(t=sql.Identifier(table)),
        (batch_id,),
    ).fetchone()[0]


def test_object_lands_in_minio(uploaded_batch, cfg):
    _, key = uploaded_batch
    assert key in storage.list_keys(cfg.raw_prefix)


def test_pipeline_loads_exactly_the_expected_rows(uploaded_batch, airflow, conn, cfg, batch_id):
    manifest, _ = uploaded_batch
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    assert _count(conn, cfg.fact_table, batch_id) == manifest["expected_clean"]
    assert _count(conn, cfg.reject_table, batch_id) == manifest["expected_rejects"]


def test_loaded_rows_satisfy_the_contract(uploaded_batch, airflow, conn, cfg, batch_id):
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    row = conn.execute(
        sql.SQL("""
            SELECT
                count(*) FILTER (WHERE customer_id IS NULL OR order_id IS NULL),
                count(*) FILTER (WHERE quantity < %s),
                count(*) FILTER (WHERE unit_price < %s),
                count(*) FILTER (WHERE currency <> ALL(%s)),
                count(*) FILTER (WHERE round(quantity * unit_price, 2) <> revenue),
                count(DISTINCT order_id),
                count(*)
            FROM {t} WHERE batch_id = %s
        """).format(t=sql.Identifier(cfg.fact_table)),
        (
            cfg.rules.min_quantity,
            cfg.rules.min_unit_price,
            list(cfg.rules.allowed_currencies),
            batch_id,
        ),
    ).fetchone()

    nulls, bad_qty, bad_price, bad_ccy, bad_revenue, distinct_ids, total = row
    assert nulls == 0
    assert bad_qty == 0
    assert bad_price == 0
    assert bad_ccy == 0
    assert bad_revenue == 0
    assert distinct_ids == total, "duplicate order_id reached the fact table"


def test_rejects_record_why_each_row_failed(uploaded_batch, airflow, conn, cfg, batch_id):
    manifest, _ = uploaded_batch
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    reasons = dict(
        conn.execute(
            sql.SQL(
                "SELECT reject_reason, count(*) FROM {t} WHERE batch_id = %s GROUP BY 1"
            ).format(t=sql.Identifier(cfg.reject_table)),
            (batch_id,),
        ).fetchall()
    )
    assert sum(reasons.values()) == manifest["expected_rejects"]
    assert reasons.get("duplicate_key") == manifest["duplicates"]
    # Every corruption kind the generator injected should be represented.
    assert len(reasons) >= 5


def test_rerunning_does_not_double_count(uploaded_batch, airflow, conn, cfg, batch_id):
    """The whole point of the staging-table merge."""
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})
    first = (_count(conn, cfg.fact_table, batch_id), _count(conn, cfg.reject_table, batch_id))

    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id, "reload": True})
    second = (_count(conn, cfg.fact_table, batch_id), _count(conn, cfg.reject_table, batch_id))

    assert first == second, f"rerun changed row counts: {first} -> {second}"


def test_metabase_serves_the_warehouse(uploaded_batch, airflow, metabase, cfg, batch_id):
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    db = next((d for d in metabase.databases() if d["name"] == "Analytics"), None)
    assert db is not None, "Analytics database is not registered in Metabase"
    assert db["engine"] == "postgres"

    tables = metabase.sync_and_await_table(db["id"], cfg.fact_table)
    assert cfg.fact_table in tables, f"Metabase cannot see {cfg.fact_table}; sees {sorted(tables)}"


def test_manifest_is_written_alongside_the_batch(tmp_path, cfg):
    """Guards the contract the other assertions depend on."""
    df, manifest = generate(100, 3, 12, 8, cfg)
    (tmp_path / "m.json").write_text(json.dumps(manifest))
    assert manifest["rows_written"] == len(df)
    assert manifest["expected_clean"] + manifest["corrupted"] == 100


def test_dashboard_exists_with_kpis_and_trends(uploaded_batch, airflow, metabase, batch_id):
    """Part 3 of the brief: a dashboard of KPIs and trends, not just a connection."""
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    dashboards = {d["name"]: d["id"] for d in metabase.get("/api/dashboard")}
    assert "Sales Overview" in dashboards, f"no Sales Overview dashboard; found {dashboards}"

    detail = metabase.get(f"/api/dashboard/{dashboards['Sales Overview']}")
    cards = detail.get("dashcards", [])
    names = {(c.get("card") or {}).get("name") for c in cards}

    assert len(cards) >= 8, f"expected a full dashboard, got {len(cards)} cards"
    # A KPI scalar and a time trend are both required by the brief.
    assert "Total revenue" in names
    assert "Revenue trend by day" in names
    displays = {(c.get("card") or {}).get("display") for c in cards}
    assert "scalar" in displays and "line" in displays


def test_every_dashboard_card_returns_data(uploaded_batch, airflow, metabase, batch_id):
    """A dashboard whose cards error is worse than no dashboard."""
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    dashboards = {d["name"]: d["id"] for d in metabase.get("/api/dashboard")}
    detail = metabase.get(f"/api/dashboard/{dashboards['Sales Overview']}")

    failures = []
    for dc in detail.get("dashcards", []):
        name = (dc.get("card") or {}).get("name", dc["card_id"])
        result = metabase.post(f"/api/card/{dc['card_id']}/query")
        if result.get("status") != "completed" or not result.get("data", {}).get("rows"):
            failures.append((name, result.get("error")))
    assert not failures, f"cards returned no data: {failures}"


def test_bundled_sample_content_is_removed(metabase):
    """The deliverable should show this platform's data, not Metabase's demo."""
    names = {d["name"] for d in metabase.databases()}
    assert "Sample Database" not in names
    assert {d["name"] for d in metabase.get("/api/dashboard")} == {"Sales Overview"}


def test_provisioning_twice_reconciles_rather_than_duplicating(metabase):
    """config/dashboard.yml must stay the source of truth after the dashboard
    exists; an earlier version skipped provisioning entirely once it did."""
    from scripts.provision_metabase import provision

    def card_names() -> list[str]:
        dash = {d["name"]: d["id"] for d in metabase.get("/api/dashboard")}
        detail = metabase.get(f"/api/dashboard/{dash['Sales Overview']}")
        return sorted((c.get("card") or {}).get("name", "") for c in detail.get("dashcards", []))

    before = card_names()
    provision()
    after = card_names()

    assert before == after, f"provisioning changed the dashboard: {before} -> {after}"
    assert len(after) == len(set(after)), f"duplicate cards on the dashboard: {after}"


def test_each_run_is_recorded_for_audit(uploaded_batch, airflow, conn, cfg, batch_id):
    """Airflow logs age out; the warehouse should still say what a run did."""
    manifest, _ = uploaded_batch
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    row = conn.execute(
        sql.SQL("SELECT rows_read, rows_loaded, rows_rejected FROM {t} WHERE batch_id = %s").format(
            t=sql.Identifier(cfg.run_table)
        ),
        (batch_id,),
    ).fetchall()

    assert len(row) == 1, f"expected exactly one audit row, got {len(row)}"
    read, loaded, rejected = row[0]
    assert loaded == manifest["expected_clean"]
    assert rejected == manifest["expected_rejects"]
    assert read == manifest["rows_written"]


def test_a_failed_batch_is_not_rediscovered(airflow, conn, cfg, tmp_path, batch_id):
    """Poison pill: a gate-failed batch must not be re-attempted forever."""
    from data_generator.generate import generate as _gen

    bad, _ = _gen(400, 5, 360, 0, cfg)  # 90% corrupt — well past max_reject_ratio
    csv = tmp_path / f"{batch_id}.csv"
    bad.to_csv(csv, index=False)
    key = f"{cfg.raw_prefix}{batch_id}.csv"
    storage.upload(csv, key)

    try:
        airflow.unpause(DAG_ID)
        with pytest.raises(AssertionError):
            airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

        ledger = conn.execute(
            sql.SQL("SELECT status FROM {t} WHERE batch_id = %s").format(
                t=sql.Identifier(cfg.run_table)
            ),
            (batch_id,),
        ).fetchone()
        assert ledger is not None, "a failed attempt was not recorded in the ledger"
        assert ledger[0] == "quality_failed"

        # A plain run must now skip it rather than failing again.
        airflow.run_to_completion(DAG_ID, {})
    finally:
        for table in (cfg.fact_table, cfg.reject_table, cfg.run_table):
            conn.execute(
                sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(t=sql.Identifier(table)),
                (batch_id,),
            )
        conn.commit()
        storage.client().delete_object(Bucket=MinioSettings.from_env().bucket, Key=key)


def test_verify_batch_catches_corrupted_rows(uploaded_batch, airflow, conn, cfg, batch_id):
    """Verification must catch data tampered with after the load."""
    airflow.unpause(DAG_ID)
    airflow.run_to_completion(DAG_ID, {"batch_id": batch_id})

    assert warehouse.verify_batch(batch_id, conn, cfg)["failures"] == []

    conn.execute(
        sql.SQL(
            "UPDATE {t} SET revenue = revenue + 999 WHERE batch_id = %s "
            "AND ctid = (SELECT ctid FROM {t} WHERE batch_id = %s LIMIT 1)"
        ).format(t=sql.Identifier(cfg.fact_table)),
        (batch_id, batch_id),
    )
    conn.commit()

    failures = warehouse.verify_batch(batch_id, conn, cfg)["failures"]
    assert any("revenue" in f for f in failures), f"corruption not caught: {failures}"


def test_attempted_batches_covers_failures_not_just_loads(conn, cfg, batch_id):
    """Discovery keys on this set, so it must include batches that never landed."""
    warehouse.ensure_schema(conn, cfg)
    warehouse.record_run(batch_id, 100, 0, 100, 1.0, conn, cfg, status="quality_failed")
    try:
        assert batch_id in warehouse.attempted_batches(conn, cfg)
    finally:
        conn.execute(
            sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(t=sql.Identifier(cfg.run_table)),
            (batch_id,),
        )
        conn.commit()


def test_prune_removes_only_rows_past_the_window(conn, cfg, batch_id):
    """Retention must delete old rejects and leave recent ones alone."""
    warehouse.ensure_schema(conn, cfg)
    ins = sql.SQL(
        "INSERT INTO {t} (batch_id, reject_reason, payload, rejected_at) "
        "VALUES (%s, %s, %s::jsonb, now() - make_interval(days => %s))"
    ).format(t=sql.Identifier(cfg.reject_table))
    conn.execute(ins, (batch_id, "old", "{}", cfg.reject_retention_days + 5))
    conn.execute(ins, (batch_id, "recent", "{}", 1))
    conn.commit()

    try:
        removed = warehouse.prune_rejects(conn, cfg)
        assert removed >= 1

        left = conn.execute(
            sql.SQL("SELECT reject_reason FROM {t} WHERE batch_id = %s").format(
                t=sql.Identifier(cfg.reject_table)
            ),
            (batch_id,),
        ).fetchall()
        assert [r[0] for r in left] == ["recent"], f"pruned the wrong rows: {left}"
    finally:
        conn.execute(
            sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(
                t=sql.Identifier(cfg.reject_table)
            ),
            (batch_id,),
        )
        conn.commit()


def test_pruning_never_touches_the_run_ledger(conn, cfg, batch_id):
    """Deleting from pipeline_runs would make old batches look unprocessed."""
    warehouse.ensure_schema(conn, cfg)
    warehouse.record_run(batch_id, 10, 10, 0, 0.0, conn, cfg)
    conn.execute(
        sql.SQL(
            "UPDATE {t} SET recorded_at = now() - make_interval(days => %s) WHERE batch_id = %s"
        ).format(t=sql.Identifier(cfg.run_table)),
        (cfg.reject_retention_days + 30, batch_id),
    )
    conn.commit()

    try:
        warehouse.prune_rejects(conn, cfg)
        assert batch_id in warehouse.attempted_batches(conn, cfg), (
            "pruning removed a ledger row; old batches would be re-attempted"
        )
    finally:
        conn.execute(
            sql.SQL("DELETE FROM {t} WHERE batch_id = %s").format(t=sql.Identifier(cfg.run_table)),
            (batch_id,),
        )
        conn.commit()

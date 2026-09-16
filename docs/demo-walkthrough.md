# Demo walkthrough

A script for showing this platform in a review. Start to finish is about 15
minutes; the reset takes ~2 of those.

---

## 0. Before the review

```bash
cd ~/Desktop/bear/ML/dockerized-mini-data-platform
make nuke                 # deletes every container and volume
make up                   # rebuild, start, wait healthy, provision Metabase
```

`make up` takes ~60s and ends with `dashboard 'Sales Overview' created with 9
cards`. Print the logins and keep them to hand:

```bash
set -a; . ./.env; set +a
echo "Airflow  http://localhost:$AIRFLOW_PORT   $AIRFLOW_ADMIN_USER / $AIRFLOW_ADMIN_PASSWORD"
echo "MinIO    http://localhost:$MINIO_CONSOLE_PORT   $MINIO_ROOT_USER / $MINIO_ROOT_PASSWORD"
echo "Metabase http://localhost:$METABASE_PORT   $METABASE_ADMIN_EMAIL / $METABASE_ADMIN_PASSWORD"
```

Open four tabs: Airflow, MinIO, Metabase, and the GitHub repo.

**The point to make about the reset:** everything just came from nothing — three
databases, a bucket, an Airflow admin, a Metabase admin, a registered database
and a nine-card dashboard. No wizard, no clicking. That is what lets CI verify
the whole chain.

---

## 1. Ingestion — MinIO

```bash
.venv/bin/python -m data_generator.generate \
  --batch-id demo --rows 2000 --seed 42 --corrupt 60 --duplicates 40 --upload
```

Output states the expectation up front:

```
rows_written   2040
expected_clean 1940
expected_rejects 100
```

**Show:** MinIO console → Object Browser → `raw` → `sales/` → `demo.csv`.

**Say:** the generator corrupts rows *deliberately* and records how many should
survive. Every number downstream is checked against that manifest, so the
pipeline is never marking its own homework.

---

## 2. Processing — Airflow

**Show:** http://localhost:8082 → `sales_pipeline` → Graph.

```
discover  →  ingest [n]  →  verify_load  →  summarise
```

Unpause, then **Trigger** — with no config.

**Say:** nothing tells it which file to process. `discover` lists MinIO and
subtracts every batch in the run ledger, so it finds `demo` on its own. The
stacked box on `ingest` means it is dynamically mapped: one task instance per
file found, ten files would run ten in parallel.

Hover each box — every task carries its own description.

---

## 3. Storage — Postgres

```bash
docker compose exec -T postgres psql -U platform -d analytics -c "
SELECT 'fact_sales' AS t, count(*) FROM fact_sales
UNION ALL SELECT 'rejected_sales', count(*) FROM rejected_sales;"
```

**Expect 1940 and 100** — exactly the manifest.

Then show *why* rows were rejected:

```bash
docker compose exec -T postgres psql -U platform -d analytics -c "
SELECT reject_reason, count(*) FROM rejected_sales GROUP BY 1 ORDER BY 2 DESC;"
```

**Say:** bad rows are quarantined with a reason, never dropped. If an upstream
system starts sending bad timestamps you see it as a number here, not as
missing revenue later.

---

## 4. Visualisation — Metabase

**Show:** http://localhost:3001 → Our analytics → **Sales Overview**.

**Say:** Orders reads 1940 and Rows quarantined reads 100 — the same numbers as
Postgres and the manifest. The chain agrees end to end. Data quality is on the
business dashboard on purpose, not buried in a log.

Click a card title to show the SQL. It lives in `config/dashboard.yml`, not in
Metabase — the dashboard is rebuilt from the repo on every `make up`.

---

## 5. The two that separate it from a demo

### Idempotency

Trigger the DAG again with config:

```json
{"batch_id": "demo", "reload": true}
```

Re-check the counts. **Still 1940 / 100.**

**Say:** Airflow retries tasks automatically. Without this, one retry mid-load
silently doubles a batch and nobody notices until the revenue looks wrong. Rows
go to a staging table, the batch's previous attempt is deleted, then rows are
upserted on the natural key — all in one transaction.

### The quality gate

```bash
.venv/bin/python -m data_generator.generate \
  --batch-id broken --rows 2000 --seed 9 --corrupt 1800 --duplicates 0 --upload
```

Trigger with `{"batch_id": "broken"}` and watch `ingest` go **red**.

```bash
docker compose exec -T postgres psql -U platform -d analytics -c "
SELECT batch_id, rows_loaded, rows_rejected, status, detail FROM pipeline_runs;"
```

**Expect:** `broken | 0 | 1800 | quality_failed | 1800/2000 rows rejected (90.0%); limit is 50.0%`

**Say three things:**

1. Nothing was written — `fact_sales` is untouched. Loading the 200 good rows
   and reporting success would publish a dataset that looks fine and is badly
   wrong.
2. It failed **once**, not three times. Retries are for transient faults; a bad
   file fails identically every time, so it raises a non-retryable error.
3. The failure is recorded. Ask "what happened to `broken`?" in SQL and the
   ledger answers, long after the Airflow logs have aged out.

Then trigger a plain run with no config: it goes **green**, skipping `broken`
because the ledger has seen it. That is the poison-pill fix — discovery keys on
attempts, not on what landed.

---

## 6. CI/CD — GitHub

**Show:** repo → Actions → newest run.

Six jobs: `lint`, `unit tests`, `dag integrity`, `end-to-end`, `publish image`,
`deploy to test environment`.

**Say:**

- Fast tier (lint, unit, dags) runs in parallel with no services — under 90s.
- `end-to-end` builds the whole platform and runs the integration suite.
- `publish` pushes the image to GHCR tagged `sha-<commit>`, never `latest`.
- `deploy to test environment` **pulls that exact image back** and runs the
  stack with `--no-build`, so what is verified is the artifact that ships.

Then **Insights → Network** for the branch graph, and any merged PR to show the
checks that gated it.

---

## What this lab was asked for, and where it is

| Brief clause | Where |
| :-- | :-- |
| Four services in one compose file | `docker-compose.yml` |
| Persistent volumes | `pgdata`, `miniodata`, `airflow-logs`, `airflow-auth` |
| Network for Airflow → MinIO and Postgres | explicit `platform` bridge |
| Sample data generator | `data_generator/generate.py` |
| DAG detects new files in MinIO | `discover` task |
| Cleaning and transformation | `mini_platform/transforms.py` |
| Loads into PostgreSQL | `mini_platform/warehouse.py` |
| Metabase connected to Postgres | `scripts/provision_metabase.py` |
| Dashboard of KPIs and trends | `config/dashboard.yml`, 9 cards |
| Build and lint images per commit | `lint` + `dags` jobs |
| Deploy to a test environment | `publish` → `deploy-test` |
| Validate MinIO → Airflow → Postgres → Metabase | `tests/integration/` |

Full mapping with the test that proves each: [brief-compliance.md](brief-compliance.md).

---

## Questions to expect

**"Why is business logic not in the DAG?"** `mini_platform` imports no Airflow,
so the cleaning contract is unit-tested in under a second with no containers.
The DAG only moves data between systems.

**"How do you know the tests are any good?"** De-duplication was deliberately
disabled to check: 5 of 7 end-to-end tests failed. A suite that stays green
through sabotage proves nothing.

**"Why is `ingest` one task and not six?"** Tasks split at persistence
boundaries. Everything before the write shares one in-memory DataFrame, and
passing that between tasks means materialising it each time. `verify_load` is
separate because it reads from Postgres and needs nothing passed to it.

**"What is not done?"** Promotion to a long-lived production host. `deploy-test`
deploys the published image to an ephemeral environment and verifies it;
promoting the same digest to a real server is one further gated job. There is no
server behind this lab, and a fake deploy would be worse than an honest gap.

---

## If something misbehaves

```bash
make doctor          # toolchain, venv, ports, disk
make ps              # service status
make logs            # tail everything
make nuke && make up # start over
```

Known traps are in the README's *Gotchas worth knowing* table.

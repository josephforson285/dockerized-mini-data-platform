# Brief compliance

Every clause of [the brief](brief.md), what implements it, and how it is proven.
Kept current; a clause with no evidence column is not done.

## Part 1 — Infrastructure

| Clause | Implementation | Evidence |
| :-- | :-- | :-- |
| Four services in one `docker-compose.yml` | `docker-compose.yml` | `make validate` |
| Persistent storage via Docker volumes | `pgdata`, `miniodata`, `airflow-logs`, `airflow-auth` | `docker volume ls` after `make up` |
| Network allows Airflow → MinIO and Postgres | explicit `platform` bridge network | `tests/integration` runs through it |

## Part 2 — Pipeline

| Clause | Implementation | Evidence |
| :-- | :-- | :-- |
| Sample data generator | `data_generator/generate.py` | `tests/unit/test_generator.py` |
| DAG detects new files in MinIO | `discover` task in `dags/sales_pipeline.py` | `test_pipeline_loads_exactly_the_expected_rows` |
| Cleaning and transformation | `mini_platform/transforms.py` | 26 unit tests |
| Loads into PostgreSQL | `mini_platform/warehouse.py` | `test_loaded_rows_satisfy_the_contract` |

## Part 3 — Visualisation

| Clause | Implementation | Evidence |
| :-- | :-- | :-- |
| Connect Metabase to PostgreSQL | `scripts/provision_metabase.py` | `test_metabase_serves_the_warehouse` |
| Dashboard of KPIs and trends | `config/dashboard.yml`, provisioned via API | `test_dashboard_exists_with_kpis_and_trends` |

## CI/CD

| Clause | Implementation | Evidence |
| :-- | :-- | :-- |
| Build and lint images each commit | `lint` + `dags` jobs | hadolint on the only Dockerfile; the other three services run pinned upstream images, which have no Dockerfile to build |
| Deploy updated containers to a test environment | `publish` → GHCR `sha-<commit>`, then `deploy-test` pulls that image into an ephemeral environment | `deploy-test` job smoke-tests the published artifact |
| Validate data flow MinIO → Airflow → Postgres → Metabase | `tests/integration/test_end_to_end.py` | `integration` job |
| Manage notifications | GitHub's native failure email, plus a run summary of per-service state and uploaded compose logs | `Report failure in the run summary` step |

## Repository structure

Required by the brief and present: `dags/`, `data_generator/`, `docker-compose.yml`,
`config/`, `.github/workflows/`, `.gitignore`, `README.md`.

## Known limits

Promotion to a long-lived production host is not wired up: there is no server
for this lab. `deploy-test` deploys the published image to an ephemeral
environment and verifies it; promoting the same digest to a real host would be
one further job gated behind a GitHub Environment.

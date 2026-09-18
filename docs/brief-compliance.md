# Brief Compliance

This document maps the project brief to its implementation and verification.

## Infrastructure

| Requirement                 | Implementation            | Verification             |
| :-------------------------- | :------------------------ | :----------------------- |
| Four containerized services | `docker-compose.yml`      | `make validate`          |
| Persistent storage          | Docker volumes            | Verified after `make up` |
| Service networking          | `platform` bridge network | Integration tests        |

## Data Pipeline

| Requirement                 | Implementation                    | Verification       |
| :-------------------------- | :-------------------------------- | :----------------- |
| Synthetic data generator    | `data_generator/generate.py`      | Unit tests         |
| Detect new MinIO files      | `discover` task                   | Integration tests  |
| Clean and transform data    | `mini_platform/transforms.py`     | Unit tests         |
| Load PostgreSQL             | `mini_platform/warehouse.py`      | Integration tests  |
| Verify loaded data          | `verify_load`                     | Verification tests |
| Reject poor-quality batches | Quality gate + `max_reject_ratio` | Failure-path tests |
| Audit and retention         | `pipeline_runs`, maintenance DAG  | Retention tests    |

## Visualisation

| Requirement                      | Implementation          | Verification     |
| :------------------------------- | :---------------------- | :--------------- |
| Metabase connected to PostgreSQL | `provision_metabase.py` | Integration test |
| KPI dashboard                    | `config/dashboard.yml`  | Dashboard test   |

## CI/CD

| Requirement                      | Implementation                           | Verification           |
| :------------------------------- | :--------------------------------------- | :--------------------- |
| Automated linting and validation | `lint`, `dags` jobs                      | GitHub Actions         |
| Deploy to test environment       | GHCR image + `deploy-test`               | Smoke test             |
| Validate end-to-end flow         | Integration suite                        | `end-to-end` job       |
| Failure reporting                | GitHub notifications, summaries and logs | Workflow failure steps |

## Repository Structure

Required project components are present:

`dags/`, `data_generator/`, `config/`, `.github/workflows/`, `docker-compose.yml`, `.gitignore`, and `README.md`.

 
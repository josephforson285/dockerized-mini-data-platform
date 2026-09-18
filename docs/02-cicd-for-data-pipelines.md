# CI/CD for Data Pipelines

CI/CD for data systems must validate more than code. It also has to protect data quality, pipeline state, and reproducibility.

## What Makes Data Pipelines Different

**Correct code can still produce bad data.**
Tests must validate both transformation logic and the **data contract** — schema, required fields, ranges, and expected values.

**Pipelines are stateful.**
Retries, replays, and backfills may process the same batch more than once, so loads must be **idempotent** and safe to repeat.

**Test environments need representative data.**
Testing requires realistic data structures and failure cases without exposing production records.

## Testing Layers

| Layer                 | Purpose                               | In this project             |
| :-------------------- | :------------------------------------ | :-------------------------- |
| Unit tests            | Validate transformation logic         | Pure Python, no Docker      |
| Contract/config tests | Detect schema or configuration errors | Config validated at load    |
| DAG tests             | Ensure Airflow DAGs import correctly  | Tested inside Airflow image |
| Integration tests     | Validate real service interactions    | Full platform stack         |
| Data-quality checks   | Detect invalid incoming data          | Rejects stored with reasons |

Business logic is kept outside Airflow in `mini_platform`, allowing transformations to be tested independently and quickly.

## Reliability Patterns

**Idempotent loading**
Rows are staged, previous attempts for the batch are replaced, and records are upserted by `order_id`. Re-running a batch therefore does not duplicate data.

**Independent expected results**
Expected row counts come from the generator manifest rather than the transformation output itself, preventing tests from validating a function against its own result.

**Cold-start testing**
End-to-end tests start from clean services and volumes, exposing hidden dependencies on existing state.

**Centralized rules**
Validation rules such as allowed currencies live in `config/pipeline.yml`, providing a single source of truth.

**Visible failures**
Invalid records are written to `rejected_sales` with rejection reasons instead of being silently discarded.

## DataOps

DataOps extends CI/CD principles across the full data lifecycle:

* version code, configuration, and dashboard definitions;
* automate testing and deployment;
* use reproducible infrastructure and immutable artifacts;
* monitor data quality as well as job status.

In this platform, Airflow, PostgreSQL, Metabase connections, and dashboard configuration are provisioned from version-controlled files rather than manual setup. This makes the environment reproducible, reviewable, and testable.

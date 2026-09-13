# CI/CD for data pipelines

Applying CI/CD to data work is not just running the same pipeline against
different code. Three properties of data systems change what the pipeline has
to prove.

## What makes data different

**Correct code can still produce wrong data.** An application test asks "does
this function return what I expect?". A pipeline must also ask "is the data that
came out of it usable?" — a transform can be flawless and still load a batch
where every `customer_id` is null because upstream changed. Tests therefore have
to cover the *data contract*, not only the code.

**Pipelines are stateful.** Deploying a web service replaces a stateless
process. Deploying a pipeline changes something that writes to a warehouse other
people query. Retries, backfills and replays all re-execute the same work, so
"run it again" must be safe by construction.

**Environments need data, not just code.** A test environment with no data
proves nothing. It needs data that is realistic in shape and volume, without
copying production records into a place with weaker access controls.

## The testing layers

Tests are cheapest and most specific at the bottom, and each layer catches what
the one below cannot.

| Layer | Catches | Cost | Here |
| :-- | :-- | :-- | :-- |
| Unit — pure transforms | logic errors in cleaning rules | ~0.5s | 36 tests, no Docker |
| Contract — schema and config | a changed contract, an invalid config | instant | config validated at load |
| DAG parse | a DAG that no longer imports | seconds | 4 tests in the Airflow image |
| Integration — real services | wiring, permissions, ordering | minutes | 11 tests, full stack |
| In-pipeline data quality | bad rows in real data | per run | rejects table with reasons |

The structural decision that makes this possible is keeping business logic out
of the orchestrator. `mini_platform` imports no Airflow. That is why the cleaning
contract is testable in half a second instead of needing a scheduler, and why
the fast tier of CI finishes in under a minute.

## How this improves reliability — with evidence

Each of these is something that actually happened while building this platform,
not a hypothetical.

**Idempotency turns retries from a risk into a non-event.** The load stages rows
in a temp table, deletes the batch's previous attempt, then upserts on the
natural key. Airflow retries tasks by default; without this, one retry silently
doubles a batch. The end-to-end suite runs the DAG twice and asserts the row
count is unchanged.

**Tests must be able to fail.** Expected row counts come from the generator's
manifest — computed from deliberate corruption — rather than from the
transform's own output. Comparing a transform to itself always passes. This was
verified by sabotage: disabling de-duplication failed 5 of 7 end-to-end tests,
and the 2 that passed were the ones that do not exercise the transform.

**CI must run what production runs.** A healthcheck used
`airflow jobs check` with a 5-second timeout. Measured on an idle 12-core
machine, that command takes **5.014 seconds**. It passed locally by luck and
failed on a shared CI runner every time. Local success on a fast machine is not
evidence.

**Test from a cold start.** The suite passed repeatedly against a stack that had
been running for hours, then failed the first time it ran against empty volumes:
Metabase had not yet discovered a table, and a fixture assumed tables another
test had created. Warm-state dependence is invisible until CI, which is always
cold.

**One definition of every rule.** The list of valid currencies was declared in
both the schema module and the generator. Adding a currency to one would make
the generator emit rows its own validator rejected — and the failure would
surface as a confusing count mismatch far from the cause. Rules now live in
`config/pipeline.yml`, with a regression test pinning it.

**Bad rows are visible, not discarded.** Cleaning returns `(clean, rejects)`;
rejected rows are written to a quarantine table with the rule that failed and
are shown on the dashboard. A pipeline that silently drops rows reports success
while losing data.

## What DataOps adds

DataOps is this discipline applied to the data lifecycle as a whole: version
everything (code, configuration, dashboards), test continuously, deploy
immutable artifacts, and monitor the data itself rather than only the job status.

The practical marker is that nothing is configured by hand. In this platform the
Airflow credentials, the Metabase admin, the warehouse connection and all nine
dashboard cards are provisioned through APIs from files in the repository, and
the dashboard reconciles with its config on every run. If a dashboard exists
only because someone clicked it into being, it cannot be reviewed, cannot be
rebuilt, and cannot be verified by CI.

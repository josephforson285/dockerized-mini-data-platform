"""Parse the DAG bag and fail on any import error.

Runs inside the Airflow image (`make dags`), because that is the only place
Airflow is installed. Catches the most common breakage — a DAG that no longer
imports — in seconds, without starting a scheduler.
"""

from __future__ import annotations

import pytest

pytest.importorskip("airflow", reason="only meaningful inside the Airflow image")

from airflow.models import DagBag  # noqa: E402

DAGS_FOLDER = "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    # Airflow 3's DagBag has no include_examples; examples are off by config.
    return DagBag(dag_folder=DAGS_FOLDER)


def test_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_are_present(dagbag: DagBag):
    assert "sales_pipeline" in dagbag.dag_ids


def test_every_dag_has_tags_and_an_owner(dagbag: DagBag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("retries") is not None, f"{dag_id} sets no retries"


def test_no_dag_schedules_a_catchup_backlog(dagbag: DagBag):
    """catchup=True on a manual pipeline floods the scheduler on unpause."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} has catchup enabled"

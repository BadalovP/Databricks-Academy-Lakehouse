import json
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from lab09 import jobs


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg() -> dict:
    return {"job": {"name": "lab09_taxi_reconciliation_job", "task_key": "reconcile_counts"}}


def test_find_job_by_name_returns_exact_match():
    client = _autospec_client()
    match = MagicMock()
    match.settings.name = "lab09_taxi_reconciliation_job"
    match.job_id = 42
    client.jobs.list.return_value = [match]

    found = jobs.find_job_by_name(client, "lab09_taxi_reconciliation_job")
    assert found.job_id == 42


def test_find_job_by_name_returns_none_when_missing():
    client = _autospec_client()
    client.jobs.list.return_value = []
    assert jobs.find_job_by_name(client, "lab09_taxi_reconciliation_job") is None


def test_ensure_job_reuses_existing_job_without_creating_a_duplicate():
    client = _autospec_client()
    existing = MagicMock()
    existing.settings.name = "lab09_taxi_reconciliation_job"
    existing.job_id = 42
    client.jobs.list.return_value = [existing]

    job_id = jobs.ensure_job(
        client,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        cluster_id="cluster-1",
    )

    assert job_id == 42
    client.jobs.create.assert_not_called()


def test_ensure_job_creates_when_missing():
    client = _autospec_client()
    client.jobs.list.return_value = []
    created = MagicMock()
    created.job_id = 99
    client.jobs.create.return_value = created

    job_id = jobs.ensure_job(
        client,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        cluster_id="cluster-1",
    )

    assert job_id == 99
    client.jobs.create.assert_called_once()


def test_reset_job_cluster_points_task_at_the_given_cluster_id():
    client = _autospec_client()

    jobs.reset_job_cluster(
        client,
        42,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        cluster_id="cluster-new",
    )

    client.jobs.reset.assert_called_once()
    _, kwargs = client.jobs.reset.call_args
    assert kwargs["job_id"] == 42
    task = kwargs["new_settings"].tasks[0]
    assert task.existing_cluster_id == "cluster-new"
    assert task.task_key == "reconcile_counts"


def test_run_job_now_returns_run_id_without_waiting():
    client = _autospec_client()
    waiter = MagicMock()
    waiter.run_id = 7
    client.jobs.run_now.return_value = waiter

    run_id = jobs.run_job_now(client, 42)

    assert run_id == 7
    waiter.result.assert_not_called()


def test_get_run_output_json_locates_task_and_parses_notebook_result():
    client = _autospec_client()

    run = MagicMock()
    task_run = MagicMock()
    task_run.task_key = "reconcile_counts"
    task_run.run_id = 555
    other_task_run = MagicMock()
    other_task_run.task_key = "some_other_task"
    other_task_run.run_id = 111
    run.tasks = [other_task_run, task_run]
    client.jobs.get_run.return_value = run

    output = MagicMock()
    output.notebook_output.result = json.dumps({"bronze_rows": 10, "reconciliation_passed": True})
    client.jobs.get_run_output.return_value = output

    result = jobs.get_run_output_json(client, run_id=1, task_key="reconcile_counts")

    assert result == {"bronze_rows": 10, "reconciliation_passed": True}
    client.jobs.get_run_output.assert_called_once_with(run_id=555)


def test_get_run_output_json_raises_when_task_key_not_found():
    client = _autospec_client()
    run = MagicMock()
    run.tasks = []
    client.jobs.get_run.return_value = run

    with pytest.raises(RuntimeError):
        jobs.get_run_output_json(client, run_id=1, task_key="reconcile_counts")


def test_get_run_output_json_raises_when_notebook_output_missing():
    client = _autospec_client()
    run = MagicMock()
    task_run = MagicMock()
    task_run.task_key = "reconcile_counts"
    task_run.run_id = 555
    run.tasks = [task_run]
    client.jobs.get_run.return_value = run

    output = MagicMock()
    output.notebook_output = None
    output.error = "task failed"
    client.jobs.get_run_output.return_value = output

    with pytest.raises(RuntimeError):
        jobs.get_run_output_json(client, run_id=1, task_key="reconcile_counts")

import json
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from lab09 import jobs


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg() -> dict:
    return {
        "catalog": "dbr_dev",
        "pipeline": {"target_schema": "lab09"},
        "tables": {
            "bronze": "lab09_taxi_bronze",
            "silver": "lab09_taxi_silver",
            "quarantine": "lab09_taxi_quarantine",
            "gold": "lab09_taxi_daily_summary",
        },
        "job": {"name": "lab09_taxi_reconciliation_job", "task_key": "reconcile_counts"},
    }


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


# --- notebook base_parameters (dedicated output schema) ---------------------


def test_task_settings_passes_catalog_and_output_schema_as_base_parameters():
    task = jobs._task_settings(
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        "cluster-1",
        None,
    )

    assert task.notebook_task.base_parameters["catalog"] == "dbr_dev"
    # Must be the pipeline's OUTPUT schema (config/dev.yml's
    # pipeline.target_schema), never the landing/input schema -- the
    # reconciliation notebook's own hardcoded widget default
    # ("parvinbadalov") must be overridden, not relied upon.
    assert task.notebook_task.base_parameters["schema"] == "lab09"


def test_task_settings_passes_all_four_table_names_as_base_parameters():
    task = jobs._task_settings(
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        "cluster-1",
        None,
    )

    params = task.notebook_task.base_parameters
    assert params["bronze_table"] == "lab09_taxi_bronze"
    assert params["silver_table"] == "lab09_taxi_silver"
    assert params["quarantine_table"] == "lab09_taxi_quarantine"
    assert params["gold_table"] == "lab09_taxi_daily_summary"


def test_task_settings_base_parameters_present_in_every_compute_mode():
    """base_parameters must not depend on which of the three mutually
    exclusive compute modes is selected.
    """
    serverless_task = jobs._task_settings(
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        None,
        None,
        serverless=True,
    )
    new_cluster_task = jobs._task_settings(
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        None,
        {"spark_version": "18.x-scala2.13", "node_type_id": "m4.large", "num_workers": 0},
    )

    for task in (serverless_task, new_cluster_task):
        assert task.notebook_task.base_parameters["schema"] == "lab09"
        assert task.notebook_task.base_parameters["catalog"] == "dbr_dev"


# --- three mutually exclusive compute modes ---------------------------------


def test_task_settings_serverless_has_no_cluster_reference_fields():
    task = jobs._task_settings(
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        None,
        None,
        serverless=True,
    )

    task_dict = task.as_dict()
    assert "existing_cluster_id" not in task_dict
    assert "new_cluster" not in task_dict
    assert "job_cluster_key" not in task_dict
    # environment_key must also never be set on a notebook task -- the
    # Jobs API rejects it there; it only applies to script/wheel/dbt tasks.
    assert "environment_key" not in task_dict
    assert (
        task.notebook_task.notebook_path == "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts"
    )


def test_task_settings_rejects_zero_modes_selected():
    with pytest.raises(ValueError, match="Exactly one"):
        jobs._task_settings(
            _cfg(),
            "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
            None,
            None,
            serverless=False,
        )


def test_task_settings_rejects_multiple_modes_selected():
    with pytest.raises(ValueError, match="Exactly one"):
        jobs._task_settings(
            _cfg(),
            "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
            "cluster-1",
            None,
            serverless=True,
        )


def test_ensure_job_reuses_existing_job_in_serverless_mode_without_duplicating():
    client = _autospec_client()
    existing = MagicMock()
    existing.settings.name = "lab09_taxi_reconciliation_job"
    existing.job_id = 42
    client.jobs.list.return_value = [existing]

    job_id = jobs.ensure_job(
        client,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        serverless=True,
    )

    assert job_id == 42
    client.jobs.create.assert_not_called()


def test_ensure_job_creates_in_serverless_mode_when_missing():
    client = _autospec_client()
    client.jobs.list.return_value = []
    created = MagicMock()
    created.job_id = 99
    client.jobs.create.return_value = created

    job_id = jobs.ensure_job(
        client,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        serverless=True,
    )

    assert job_id == 99
    _, kwargs = client.jobs.create.call_args
    task = kwargs["tasks"][0]
    assert task.existing_cluster_id is None
    assert task.new_cluster is None
    assert task.job_cluster_key is None


def test_reset_job_cluster_serverless_mode_has_no_cluster_reference():
    client = _autospec_client()

    jobs.reset_job_cluster(
        client,
        42,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        serverless=True,
    )

    client.jobs.reset.assert_called_once()
    _, kwargs = client.jobs.reset.call_args
    task = kwargs["new_settings"].tasks[0]
    assert task.existing_cluster_id is None
    assert task.new_cluster is None
    assert task.job_cluster_key is None


def test_reset_job_cluster_job_cluster_mode_still_intact():
    """Regression guard: mode B (a job-managed new_cluster) must keep working
    exactly as before now that a third mode exists.
    """
    client = _autospec_client()
    new_cluster_dict = {
        "spark_version": "18.x-photon-scala2.13",
        "node_type_id": "m4.large",
        "num_workers": 0,
        "autotermination_minutes": 20,
        "data_security_mode": "SINGLE_USER",
    }

    jobs.reset_job_cluster(
        client,
        42,
        _cfg(),
        "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
        new_cluster=new_cluster_dict,
    )

    _, kwargs = client.jobs.reset.call_args
    task = kwargs["new_settings"].tasks[0]
    assert task.existing_cluster_id is None
    assert task.new_cluster is not None
    assert task.new_cluster.node_type_id == "m4.large"


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

"""Tests for scripts/run_azure_job.py -- trigger, monitor, and verify one run
of the deployed Azure three-task Job. Every external Databricks call is
mocked; this file never touches live infrastructure.
"""

from unittest.mock import MagicMock

import run_azure_job as raj

from lab09 import monitoring


def _cfg() -> dict:
    return {
        "job": {
            "name": "lab09_taxi_reconciliation_job",
            "ingestion_task_key": "ingestion",
            "pipeline_task_key": "lakeflow_pipeline",
            "reconciliation_task_key": "reconciliation",
        }
    }


def _task_run(task_key: str, cluster_id: str | None):
    t = MagicMock()
    t.task_key = task_key
    if cluster_id is not None:
        t.cluster_instance.cluster_id = cluster_id
    else:
        t.cluster_instance = None
    return t


def _kwargs(**overrides) -> dict:
    base = dict(
        job_timeout_seconds=10,
        job_poll_interval_seconds=0,
        termination_timeout_seconds=10,
        termination_poll_interval_seconds=0,
    )
    base.update(overrides)
    return base


def test_job_not_found_reports_error_and_never_triggers_anything():
    client = MagicMock()
    client.jobs.list.return_value = []

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.job_id is None
    assert report.run_id is None
    assert "not found" in report.error
    client.jobs.run_now.assert_not_called()


def _existing_job(client, job_id=777):
    job = MagicMock()
    job.job_id = job_id
    job.settings.name = "lab09_taxi_reconciliation_job"
    client.jobs.list.return_value = [job]
    return job


def test_successful_run_collects_output_and_confirms_cluster_terminated(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    client.jobs.run_now.return_value.run_id = 555

    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="SUCCESS"
        ),
    )
    client.jobs.get_run.return_value.tasks = [
        _task_run("ingestion", "cluster-shared-1"),
        _task_run("lakeflow_pipeline", None),
        _task_run("reconciliation", "cluster-shared-1"),
    ]
    monkeypatch.setattr(
        raj.jobs,
        "get_run_output_json",
        lambda client, run_id, task_key: {"task": task_key, "run_id": run_id},
    )
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="TERMINATED", confirmed=True
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.job_id == 777
    assert report.run_id == 555
    assert report.result_state == "SUCCESS"
    assert report.ingestion_output == {"task": "ingestion", "run_id": 555}
    assert report.reconciliation_output == {"task": "reconciliation", "run_id": 555}
    assert report.job_cluster_id == "cluster-shared-1"
    assert report.job_cluster_terminated_confirmed is True
    assert report.succeeded is True


def test_job_cluster_not_terminated_fails_overall_despite_successful_run(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    client.jobs.run_now.return_value.run_id = 555
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="SUCCESS"
        ),
    )
    client.jobs.get_run.return_value.tasks = [_task_run("ingestion", "cluster-shared-1")]
    monkeypatch.setattr(raj.jobs, "get_run_output_json", lambda *a, **k: {})
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="PENDING", confirmed=False
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.result_state == "SUCCESS"
    assert report.job_cluster_terminated_confirmed is False
    assert report.succeeded is False


def test_job_failure_does_not_attempt_output_retrieval(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    client.jobs.run_now.return_value.run_id = 555
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="FAILED"
        ),
    )
    client.jobs.get_run.return_value.tasks = []
    output_mock = MagicMock()
    monkeypatch.setattr(raj.jobs, "get_run_output_json", output_mock)

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.succeeded is False
    output_mock.assert_not_called()
    assert report.ingestion_output is None
    assert report.reconciliation_output is None


def test_no_cluster_instance_found_does_not_count_as_a_termination_failure(monkeypatch):
    """None (no job-cluster instance ever observed, e.g. the run failed
    before any task started) must never be conflated with an explicit
    False -- only the run's own result_state should determine success here.
    """
    client = MagicMock()
    _existing_job(client)
    client.jobs.run_now.return_value.run_id = 555
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="FAILED"
        ),
    )
    client.jobs.get_run.return_value.tasks = []

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.job_cluster_id is None
    assert report.job_cluster_terminated_confirmed is None
    assert report.succeeded is False  # still fails, but because of result_state, not cleanup

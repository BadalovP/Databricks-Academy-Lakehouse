"""Tests for scripts/run_azure_job.py -- trigger, monitor, and verify one run
of the deployed Azure three-task Job. Every external Databricks call is
mocked; this file never touches live infrastructure.
"""

from unittest.mock import MagicMock

import pytest
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


def _task_run(
    task_key: str, life_cycle_state: str, result_state: str | None, cluster_id: str | None = None
):
    t = MagicMock()
    t.task_key = task_key
    t.state.life_cycle_state = MagicMock(value=life_cycle_state)
    t.state.result_state = MagicMock(value=result_state) if result_state else None
    if cluster_id is not None:
        t.cluster_instance.cluster_id = cluster_id
    else:
        t.cluster_instance = None
    return t


def _successful_task_runs(cluster_id: str = "cluster-shared-1"):
    return [
        _task_run("ingestion", "TERMINATED", "SUCCESS", cluster_id),
        _task_run("lakeflow_pipeline", "TERMINATED", "SUCCESS"),
        _task_run("reconciliation", "TERMINATED", "SUCCESS", cluster_id),
    ]


def _kwargs(**overrides) -> dict:
    base = dict(
        job_timeout_seconds=10,
        job_poll_interval_seconds=0,
        termination_timeout_seconds=10,
        termination_poll_interval_seconds=0,
    )
    base.update(overrides)
    return base


def _existing_job(client, job_id=777):
    job = MagicMock()
    job.job_id = job_id
    job.settings.name = "lab09_taxi_reconciliation_job"
    client.jobs.list.return_value = [job]
    return job


def _patch_job_run_success(monkeypatch, run_id=555):
    client_holder = {}

    def _run_now(client, job_id):
        return run_id

    monkeypatch.setattr(raj.jobs, "run_job_now", _run_now)
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=run_id, life_cycle_state="TERMINATED", result_state="SUCCESS"
        ),
    )
    return client_holder


def test_job_not_found_reports_error_and_never_triggers_anything():
    client = MagicMock()
    client.jobs.list.return_value = []

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.job_id is None
    assert report.run_id is None
    assert "not found" in report.error
    assert report.succeeded is False
    client.jobs.run_now.assert_not_called()


def test_fully_successful_run_passes_every_check(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = _successful_task_runs()
    monkeypatch.setattr(
        raj.jobs,
        "get_run_output_json",
        lambda client, run_id, task_key: (
            {"month": "2024-01", "file_bytes": 123}
            if task_key == "ingestion"
            else {
                "bronze_rows": 10,
                "silver_valid_rows": 9,
                "rejected_rows": 1,
                "reconciliation_passed": True,
            }
        ),
    )
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="TERMINATED", confirmed=True
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.succeeded is True
    assert report.reconciliation_passed is True
    assert report.job_cluster_terminated_confirmed is True
    assert report.warnings == []


def test_reconciliation_output_present_but_reconciliation_passed_false_fails(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = _successful_task_runs()
    monkeypatch.setattr(
        raj.jobs,
        "get_run_output_json",
        lambda client, run_id, task_key: (
            {"month": "2024-01"}
            if task_key == "ingestion"
            else {"bronze_rows": 10, "silver_valid_rows": 8, "reconciliation_passed": False}
        ),
    )
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="TERMINATED", confirmed=True
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.reconciliation_passed is False
    assert report.succeeded is False


def test_missing_ingestion_output_fails_even_if_job_reports_success(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = _successful_task_runs()

    def _get_output(client, run_id, task_key):
        if task_key == "ingestion":
            raise RuntimeError("no notebook_output.result")
        return {"reconciliation_passed": True}

    monkeypatch.setattr(raj.jobs, "get_run_output_json", _get_output)
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="TERMINATED", confirmed=True
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.ingestion_output is None
    assert "no notebook_output" in report.ingestion_output_error
    assert report.succeeded is False


def test_pipeline_task_skipped_fails_even_if_overall_result_state_is_success(monkeypatch):
    """Regression test: the overall job result_state alone is not enough --
    each task's own result must be checked explicitly.
    """
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = [
        _task_run("ingestion", "TERMINATED", "SUCCESS", "cluster-shared-1"),
        _task_run("lakeflow_pipeline", "SKIPPED", "UPSTREAM_FAILED"),
        _task_run("reconciliation", "SKIPPED", "UPSTREAM_FAILED"),
    ]
    monkeypatch.setattr(raj.jobs, "get_run_output_json", lambda *a, **k: {"result": "x"})
    monkeypatch.setattr(
        raj.monitoring,
        "poll_cluster_termination",
        lambda *a, **k: monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-shared-1", state="TERMINATED", confirmed=True
        ),
    )

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.pipeline_task_result_state == "UPSTREAM_FAILED"
    assert report.succeeded is False
    # reconciliation output must never even be attempted for a skipped task
    assert report.reconciliation_output is None


def test_success_with_no_cluster_id_is_treated_as_a_monitoring_anomaly_not_a_pass(monkeypatch):
    """Regression test: this Job always uses a shared job cluster for
    ingestion/reconciliation -- an absent cluster id on an otherwise
    successful run must never be silently treated as "nothing to clean up".
    """
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = [
        _task_run("ingestion", "TERMINATED", "SUCCESS"),  # no cluster_instance
        _task_run("lakeflow_pipeline", "TERMINATED", "SUCCESS"),
        _task_run("reconciliation", "TERMINATED", "SUCCESS"),  # no cluster_instance
    ]
    monkeypatch.setattr(
        raj.jobs,
        "get_run_output_json",
        lambda client, run_id, task_key: {"reconciliation_passed": True},
    )
    terminate_mock = MagicMock()
    monkeypatch.setattr(raj.monitoring, "poll_cluster_termination", terminate_mock)

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.job_cluster_id is None
    assert report.succeeded is False
    assert any("monitoring anomaly" in w for w in report.warnings)
    terminate_mock.assert_not_called()


def test_job_cluster_not_terminated_fails_overall_despite_successful_run(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    _patch_job_run_success(monkeypatch)
    client.jobs.get_run.return_value.tasks = _successful_task_runs()
    monkeypatch.setattr(
        raj.jobs,
        "get_run_output_json",
        lambda client, run_id, task_key: {"reconciliation_passed": True},
    )
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
    assert any("not confirmed" in w for w in report.warnings)


def test_job_failure_does_not_attempt_output_retrieval(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    monkeypatch.setattr(raj.jobs, "run_job_now", lambda *a, **k: 555)
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="FAILED"
        ),
    )
    client.jobs.get_run.return_value.tasks = [
        _task_run("ingestion", "TERMINATED", "FAILED"),
        _task_run("lakeflow_pipeline", "SKIPPED", "UPSTREAM_FAILED"),
        _task_run("reconciliation", "SKIPPED", "UPSTREAM_FAILED"),
    ]
    output_mock = MagicMock()
    monkeypatch.setattr(raj.jobs, "get_run_output_json", output_mock)

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.succeeded is False
    output_mock.assert_not_called()
    assert report.ingestion_output is None
    assert report.reconciliation_output is None


# --- timeout escalation ------------------------------------------------------


def test_timeout_performs_a_live_status_check_and_warns_without_assuming_stopped(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    monkeypatch.setattr(raj.jobs, "run_job_now", lambda *a, **k: 555)
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="RUNNING", result_state=None, timed_out=True
        ),
    )
    live_run = MagicMock()
    live_run.state.life_cycle_state = MagicMock(value="RUNNING")
    live_run.tasks = []
    client.jobs.get_run.return_value = live_run

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert report.timed_out is True
    assert report.post_timeout_live_state == "RUNNING"
    assert any("may still be active and billing" in w for w in report.warnings)
    assert report.succeeded is False


def test_timeout_live_check_failure_is_recorded_not_raised(monkeypatch):
    client = MagicMock()
    _existing_job(client)
    monkeypatch.setattr(raj.jobs, "run_job_now", lambda *a, **k: 555)
    monkeypatch.setattr(
        raj.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="RUNNING", result_state=None, timed_out=True
        ),
    )
    client.jobs.get_run.side_effect = RuntimeError("network error")

    report = raj.run_and_verify(client, _cfg(), **_kwargs())

    assert "network error" in report.post_timeout_live_state
    assert report.succeeded is False


# --- --confirm-host safety check --------------------------------------------


def test_confirm_host_mismatch_aborts_before_triggering_anything(monkeypatch, tmp_path):
    client = MagicMock()
    client.config.host = "https://actual-host.azuredatabricks.net"
    monkeypatch.setattr(raj, "get_workspace_client", lambda profile: client)
    run_mock = MagicMock(side_effect=AssertionError("must not run when the host does not match"))
    monkeypatch.setattr(raj, "run_and_verify", run_mock)

    with pytest.raises(SystemExit):
        raj.main(
            [
                "--confirm-host",
                "different-host.azuredatabricks.net",
                "--report-path",
                str(tmp_path / "report.json"),
            ]
        )

    run_mock.assert_not_called()


def _fully_successful_report() -> "raj.RunReport":
    return raj.RunReport(
        result_state="SUCCESS",
        timed_out=False,
        ingestion_task_result_state="SUCCESS",
        pipeline_task_result_state="SUCCESS",
        reconciliation_task_result_state="SUCCESS",
        ingestion_output={"month": "2024-01"},
        reconciliation_output={"reconciliation_passed": True},
        reconciliation_passed=True,
        job_cluster_id="cluster-shared-1",
        job_cluster_terminated_confirmed=True,
    )


def test_confirm_host_match_allows_the_run_to_proceed(monkeypatch, tmp_path):
    client = MagicMock()
    client.config.host = "https://actual-host.azuredatabricks.net"
    monkeypatch.setattr(raj, "get_workspace_client", lambda profile: client)
    monkeypatch.setattr(raj, "load_config", lambda path: _cfg())
    run_mock = MagicMock(return_value=_fully_successful_report())
    monkeypatch.setattr(raj, "run_and_verify", run_mock)

    exit_code = raj.main(
        [
            "--confirm-host",
            "actual-host.azuredatabricks.net",
            "--report-path",
            str(tmp_path / "report.json"),
        ]
    )

    assert exit_code == 0
    run_mock.assert_called_once()


def test_confirm_host_omitted_skips_the_check(monkeypatch, tmp_path):
    """--confirm-host is optional -- omitting it must not block a local,
    profile-based invocation that has no independent expected-host value.
    """
    client = MagicMock()
    client.config.host = "https://whatever-host.azuredatabricks.net"
    monkeypatch.setattr(raj, "get_workspace_client", lambda profile: client)
    monkeypatch.setattr(raj, "load_config", lambda path: _cfg())
    run_mock = MagicMock(return_value=_fully_successful_report())
    monkeypatch.setattr(raj, "run_and_verify", run_mock)

    exit_code = raj.main(["--report-path", str(tmp_path / "report.json")])

    assert exit_code == 0
    run_mock.assert_called_once()

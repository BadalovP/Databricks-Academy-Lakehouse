"""Tests for scripts/validate_classic_e2e.py -- the one-shot, uninterrupted
classic-compute demonstration (create -> RUNNING -> Job -> verify ->
terminate -> cleanup). Every external Databricks call is mocked; this file
never touches live infrastructure.
"""

import uuid
from unittest.mock import MagicMock

import pytest
import validate_classic_e2e as vce

from lab09 import monitoring


def _client() -> MagicMock:
    client = MagicMock()
    client.config.host = "https://adb-example.1.azuredatabricks.net"
    client.current_user.me.return_value.user_name = "test.user@example.com"
    return client


def _kwargs(**overrides) -> dict:
    base = dict(
        node_type_hint="Standard_D4ds_v5",
        cluster_policy_name="Personal Compute",
        autotermination_minutes=20,
        readiness_timeout_seconds=10,
        readiness_poll_interval_seconds=0,
        job_timeout_seconds=10,
        job_poll_interval_seconds=0,
        termination_timeout_seconds=10,
        termination_poll_interval_seconds=0,
    )
    base.update(overrides)
    return base


def _patch_resolution(monkeypatch, client, cluster_id="cluster-1"):
    monkeypatch.setattr(vce.compute, "resolve_policy_id", lambda *a, **k: "policy-123")
    monkeypatch.setattr(vce.compute, "resolve_node_type", lambda *a, **k: "Standard_D4ds_v5")
    monkeypatch.setattr(
        vce.compute, "resolve_lts_spark_version", lambda *a, **k: "15.4.x-scala2.12"
    )
    monkeypatch.setattr(vce.compute, "start_cluster_create", lambda *a, **k: cluster_id)
    client.jobs.create.return_value = MagicMock(job_id=99)


def _patch_running(monkeypatch, cluster_id="cluster-1"):
    monkeypatch.setattr(
        vce.monitoring,
        "poll_cluster_state",
        lambda *a, **k: monitoring.ClusterOutcome(cluster_id=cluster_id, state="RUNNING"),
    )


def _patch_job_success(monkeypatch, run_id=555, result="OK:42"):
    monkeypatch.setattr(vce.jobs, "run_job_now", lambda *a, **k: run_id)
    monkeypatch.setattr(
        vce.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=run_id, life_cycle_state="TERMINATED", result_state="SUCCESS"
        ),
    )
    monkeypatch.setattr(vce.jobs, "get_run_output_json", lambda *a, **k: {"result": result})


def _patch_termination_confirmed(monkeypatch, cluster_id="cluster-1", confirmed=True):
    monkeypatch.setattr(
        vce.compute,
        "terminate_and_verify_cluster",
        MagicMock(
            return_value=monitoring.ClusterTerminationOutcome(
                cluster_id=cluster_id,
                state="TERMINATED" if confirmed else "PENDING",
                confirmed=confirmed,
            )
        ),
    )


# --- successful end-to-end orchestration ------------------------------------


def test_successful_end_to_end_validation(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    _patch_job_success(monkeypatch)
    terminate_mock = MagicMock(
        return_value=monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-1", state="TERMINATED", confirmed=True
        )
    )
    monkeypatch.setattr(vce.compute, "terminate_and_verify_cluster", terminate_mock)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "SUCCESS"
    assert report.succeeded is True
    assert report.cluster_id == "cluster-1"
    assert report.output_matches_expected is True
    assert report.notebook_output == "OK:42"
    assert report.job_deleted is True
    assert report.notebook_deleted is True
    assert report.cluster_terminated_confirmed is True
    assert report.error is None

    client.jobs.delete.assert_called_once_with(job_id=99)
    client.workspace.delete.assert_called_once()
    terminate_mock.assert_called_once_with(
        client, "cluster-1", timeout_seconds=10, poll_interval_seconds=0
    )


# --- delayed cluster provisioning (real poll_cluster_state, not mocked) ----


def test_delayed_cluster_provisioning_still_succeeds(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    pending = MagicMock(state=MagicMock(value="PENDING"))
    running = MagicMock(state=MagicMock(value="RUNNING"))
    client.clusters.get.side_effect = [pending, pending, running]
    _patch_job_success(monkeypatch)
    _patch_termination_confirmed(monkeypatch)

    report = vce.run_validation(client, **_kwargs(readiness_timeout_seconds=100))

    assert report.status == "SUCCESS"
    assert report.cluster_reached_running is True
    assert client.clusters.get.call_count == 3


# --- readiness timeout (real poll_cluster_state, timeout_seconds=0) -------


def test_readiness_timeout_fails_and_still_cleans_up(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    pending = MagicMock(state=MagicMock(value="PENDING"))
    client.clusters.get.return_value = pending
    terminate_mock = MagicMock(
        return_value=monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-1", state="TERMINATED", confirmed=True
        )
    )
    monkeypatch.setattr(vce.compute, "terminate_and_verify_cluster", terminate_mock)

    report = vce.run_validation(
        client, **_kwargs(readiness_timeout_seconds=0, readiness_poll_interval_seconds=100)
    )

    assert report.status == "FAILED"
    assert report.cluster_reached_running is False
    assert "did not reach RUNNING" in report.error
    client.jobs.create.assert_not_called()
    terminate_mock.assert_called_once_with(
        client, "cluster-1", timeout_seconds=10, poll_interval_seconds=0
    )


# --- cluster creation failure -----------------------------------------------


def test_cluster_creation_failure_with_no_matching_tag_reports_failure(monkeypatch):
    client = _client()
    monkeypatch.setattr(vce.compute, "resolve_policy_id", lambda *a, **k: None)
    monkeypatch.setattr(vce.compute, "resolve_node_type", lambda *a, **k: "Standard_D4ds_v5")
    monkeypatch.setattr(
        vce.compute, "resolve_lts_spark_version", lambda *a, **k: "15.4.x-scala2.12"
    )

    def _boom(*a, **k):
        raise TimeoutError("Timed out after 0:05:00")

    monkeypatch.setattr(vce.compute, "start_cluster_create", _boom)
    client.clusters.list.return_value = []
    terminate_mock = MagicMock()
    monkeypatch.setattr(vce.compute, "terminate_and_verify_cluster", terminate_mock)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "FAILED"
    assert report.cluster_create_required_tag_search is True
    assert report.cluster_id is None
    assert "no cluster carrying this run's own tag was found" in report.error
    terminate_mock.assert_not_called()
    client.jobs.create.assert_not_called()


def test_cluster_creation_failure_but_tag_search_finds_and_cleans_up_the_cluster(monkeypatch):
    client = _client()
    monkeypatch.setattr(vce.compute, "resolve_policy_id", lambda *a, **k: None)
    monkeypatch.setattr(vce.compute, "resolve_node_type", lambda *a, **k: "Standard_D4ds_v5")
    monkeypatch.setattr(
        vce.compute, "resolve_lts_spark_version", lambda *a, **k: "15.4.x-scala2.12"
    )
    monkeypatch.setattr(vce.uuid, "uuid4", lambda: uuid.UUID(int=1))
    expected_tag = uuid.UUID(int=1).hex[:12]

    def _boom(*a, **k):
        raise TimeoutError("Timed out after 0:05:00")

    monkeypatch.setattr(vce.compute, "start_cluster_create", _boom)
    orphan = MagicMock(cluster_id="cluster-orphan", custom_tags={"lab09_run_id": expected_tag})
    client.clusters.list.return_value = [orphan]
    client.clusters.get.return_value = MagicMock(state=MagicMock(value="PENDING"))
    terminate_mock = MagicMock(
        return_value=monitoring.ClusterTerminationOutcome(
            cluster_id="cluster-orphan", state="TERMINATED", confirmed=True
        )
    )
    monkeypatch.setattr(vce.compute, "terminate_and_verify_cluster", terminate_mock)

    report = vce.run_validation(
        client, **_kwargs(readiness_timeout_seconds=0, readiness_poll_interval_seconds=100)
    )

    assert report.cluster_create_required_tag_search is True
    assert report.cluster_id == "cluster-orphan"
    terminate_mock.assert_called_once_with(
        client, "cluster-orphan", timeout_seconds=10, poll_interval_seconds=0
    )


def test_find_cluster_by_unique_tag_matches_only_the_exact_tag():
    client = MagicMock()
    client.clusters.list.return_value = [
        MagicMock(cluster_id="c1", custom_tags={"lab09_run_id": "abc"}),
        MagicMock(cluster_id="c2", custom_tags={"lab09_run_id": "xyz"}),
        MagicMock(cluster_id="c3", custom_tags=None),
    ]

    assert vce.find_cluster_by_unique_tag(client, "xyz") == "c2"
    assert vce.find_cluster_by_unique_tag(client, "does-not-exist") is None


# --- job failure / timeout / bad output -------------------------------------


def test_job_failure_marks_failed_and_still_cleans_up(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    monkeypatch.setattr(vce.jobs, "run_job_now", lambda *a, **k: 555)
    monkeypatch.setattr(
        vce.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="TERMINATED", result_state="FAILED"
        ),
    )
    _patch_termination_confirmed(monkeypatch)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "FAILED"
    assert "did not succeed" in report.error
    assert report.job_deleted is True
    assert report.notebook_deleted is True
    assert report.cluster_terminated_confirmed is True


def test_job_timeout_marks_failed(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    monkeypatch.setattr(vce.jobs, "run_job_now", lambda *a, **k: 555)
    monkeypatch.setattr(
        vce.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=555, life_cycle_state="RUNNING", result_state=None, timed_out=True
        ),
    )
    _patch_termination_confirmed(monkeypatch)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "FAILED"
    assert report.job_timed_out is True


def test_incorrect_notebook_output_marks_failed(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    _patch_job_success(monkeypatch, result="something-else")
    _patch_termination_confirmed(monkeypatch)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "FAILED"
    assert report.output_matches_expected is False
    assert "OK:42" in report.error
    assert report.job_deleted is True
    assert report.cluster_terminated_confirmed is True


# --- cleanup edge cases ------------------------------------------------------


def test_cluster_termination_unconfirmed_preserves_status_but_fails_overall(monkeypatch):
    """Regression test for a real defect: a successful notebook run must not
    be reported as overall success when required cleanup failed.
    `status` still preserves the actual execution result (the notebook DID
    run and DID return the right output) -- `succeeded` is the separate,
    combined signal that main()'s exit code is based on, and must be False
    here even though `status` stays "SUCCESS".
    """
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    _patch_job_success(monkeypatch)
    _patch_termination_confirmed(monkeypatch, confirmed=False)

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "SUCCESS"
    assert report.cluster_terminated_confirmed is False
    assert report.cleanup_confirmed is False
    assert report.succeeded is False


def test_job_and_notebook_deletion_failures_are_recorded_and_fail_overall(monkeypatch):
    client = _client()
    _patch_resolution(monkeypatch, client)
    _patch_running(monkeypatch)
    _patch_job_success(monkeypatch)
    _patch_termination_confirmed(monkeypatch)
    client.jobs.delete.side_effect = RuntimeError("permission denied")
    client.workspace.delete.side_effect = RuntimeError("not found")

    report = vce.run_validation(client, **_kwargs())

    assert report.status == "SUCCESS"
    assert report.job_deleted is False
    assert report.notebook_deleted is False
    assert report.succeeded is False


def test_fully_clean_run_succeeds_overall():
    report = vce.ValidationReport(
        status="SUCCESS",
        job_deleted=True,
        notebook_deleted=True,
        cluster_terminated_confirmed=True,
    )
    assert report.cleanup_confirmed is True
    assert report.succeeded is True


def test_cleanup_fields_never_attempted_do_not_count_as_a_cleanup_failure():
    """None (never attempted, e.g. a run that failed before creating that
    resource) must never be conflated with an explicit False (attempted and
    failed) -- only the latter should ever fail `cleanup_confirmed`.
    """
    report = vce.ValidationReport(status="FAILED")
    assert report.job_deleted is None
    assert report.notebook_deleted is None
    assert report.cluster_terminated_confirmed is None
    assert report.cleanup_confirmed is True
    assert report.succeeded is False  # status alone still fails it here


# --- CLI wiring --------------------------------------------------------------


def test_confirm_billable_flag_is_required():
    with pytest.raises(SystemExit):
        vce.main(["--profile", "lab09-azure-prod-oauth", "--confirm-host", "example.com"])


def test_confirm_host_mismatch_aborts_before_creating_anything(monkeypatch):
    client = _client()
    client.config.host = "https://actual-host.azuredatabricks.net"
    monkeypatch.setattr(vce, "get_workspace_client", lambda profile: client)
    monkeypatch.setattr(
        vce,
        "run_validation",
        MagicMock(side_effect=AssertionError("must not run when the host does not match")),
    )

    with pytest.raises(SystemExit):
        vce.main(
            [
                "--profile",
                "lab09-azure-prod-oauth",
                "--confirm-host",
                "different-host.azuredatabricks.net",
                "--confirm-billable",
            ]
        )


def test_confirm_host_match_allows_the_run_to_proceed(monkeypatch, tmp_path):
    client = _client()
    client.config.host = "https://actual-host.azuredatabricks.net"
    monkeypatch.setattr(vce, "get_workspace_client", lambda profile: client)
    run_mock = MagicMock(return_value=vce.ValidationReport(status="SUCCESS"))
    monkeypatch.setattr(vce, "run_validation", run_mock)

    exit_code = vce.main(
        [
            "--profile",
            "lab09-azure-prod-oauth",
            "--confirm-host",
            "actual-host.azuredatabricks.net",
            "--confirm-billable",
            "--report-path",
            str(tmp_path / "report.json"),
        ]
    )

    assert exit_code == 0
    run_mock.assert_called_once()
    assert (tmp_path / "report.json").exists()

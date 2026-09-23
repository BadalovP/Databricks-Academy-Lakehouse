"""Integration-style tests for cli.cmd_run_all's compute-mode wiring.

These exist specifically to prove the fix for the "documented fallback is
not actually usable" defect: compute.try_start_cluster_create() existed,
and jobs.py already accepted a new_cluster alternative to
existing_cluster_id, but cmd_run_all() always called
compute.start_cluster_create() unconditionally and never used either. Every
external call cmd_run_all makes is mocked here (this is not a live test).
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from databricks.sdk.errors import InvalidParameterValue

from lab09 import cli, compute, monitoring
from lab09.landing import LandingResult


def _cfg(tmp_path) -> dict:
    return {
        "catalog": "dbr_dev",
        "schema": "parvinbadalov",
        "volume": "lab09_landing",
        "months": ["2024-01"],
        "pipeline": {"name": "lab09_taxi_pipeline", "target_schema": "parvinbadalov"},
        "job": {"name": "lab09_taxi_reconciliation_job", "task_key": "reconcile_counts"},
        "monitoring": {
            "pipeline_timeout_seconds": 10,
            "pipeline_poll_interval_seconds": 1,
            "cluster_timeout_seconds": 10,
            "cluster_poll_interval_seconds": 1,
            "job_timeout_seconds": 10,
            "job_poll_interval_seconds": 1,
        },
        "report": {"output_path": str(tmp_path / "report.json")},
    }


def _patch_common_success_path(monkeypatch):
    """Patch every step of cmd_run_all up through pipeline-update success,
    leaving compute.try_start_cluster_create for each test to control.
    """
    monkeypatch.setattr(
        cli.preflight, "run_preflight", lambda *a, **k: SimpleNamespace(passed=True, checks=[])
    )
    monkeypatch.setattr(cli.volumes, "ensure_volume", MagicMock())
    monkeypatch.setattr(
        cli.landing,
        "land_next_month",
        lambda *a, **k: LandingResult(
            status="SUCCESS",
            month="2024-01",
            month_landed=True,
            file_bytes=123,
            volume_path="/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips/yellow_tripdata_2024-01.parquet",
        ),
    )
    monkeypatch.setattr(cli.landing, "ensure_reference_csv", MagicMock())
    monkeypatch.setattr(cli.workspace, "upload_pipeline_sources", MagicMock())
    monkeypatch.setattr(
        cli.workspace,
        "upload_notebook",
        lambda *a, **k: "/Workspace/Users/x/lab09/notebooks/01_reconcile_counts",
    )
    monkeypatch.setattr(
        cli.workspace, "pipeline_source_dir", lambda *a, **k: "/Workspace/Users/x/lab09/pipeline"
    )
    monkeypatch.setattr(cli.pipelines, "ensure_pipeline", lambda *a, **k: ("pipeline-1", True))
    monkeypatch.setattr(cli.pipelines, "start_update", lambda *a, **k: "update-1")
    monkeypatch.setattr(
        cli.compute,
        "build_cluster_spec",
        lambda *a, **k: compute.ClusterSpec(
            spark_version="15.4.x-scala2.12", node_type_id="small", autotermination_minutes=20
        ),
    )
    monkeypatch.setattr(
        cli.monitoring,
        "poll_pipeline_update",
        lambda *a, **k: monitoring.PipelineOutcome(
            pipeline_id="pipeline-1", update_id="update-1", state="COMPLETED"
        ),
    )


def _patch_job_success_path(monkeypatch, ensure_job_mock, reset_job_cluster_mock):
    monkeypatch.setattr(cli.jobs, "ensure_job", ensure_job_mock)
    monkeypatch.setattr(cli.jobs, "reset_job_cluster", reset_job_cluster_mock)
    monkeypatch.setattr(cli.jobs, "run_job_now", lambda *a, **k: 7)
    monkeypatch.setattr(
        cli.monitoring,
        "poll_job_run",
        lambda *a, **k: monitoring.JobRunOutcome(
            run_id=7, life_cycle_state="TERMINATED", result_state="SUCCESS"
        ),
    )
    monkeypatch.setattr(
        cli.jobs,
        "get_run_output_json",
        lambda *a, **k: {
            "bronze_rows": 10,
            "silver_valid_rows": 9,
            "rejected_rows": 1,
            "gold_rows": 3,
            "failed_rules": {"INVALID_FARE": 1},
            "reconciliation_passed": True,
        },
    )


def test_explicit_cluster_path_used_when_creation_succeeds(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _patch_common_success_path(monkeypatch)
    monkeypatch.setattr(
        cli.compute, "try_start_cluster_create", lambda *a, **k: ("cluster-1", None)
    )
    monkeypatch.setattr(
        cli.monitoring,
        "poll_cluster_state",
        lambda *a, **k: monitoring.ClusterOutcome(cluster_id="cluster-1", state="RUNNING"),
    )
    ensure_job_mock = MagicMock(return_value=42)
    reset_job_cluster_mock = MagicMock()
    _patch_job_success_path(monkeypatch, ensure_job_mock, reset_job_cluster_mock)

    terminate_mock = MagicMock()
    monkeypatch.setattr(cli.compute, "terminate_cluster", terminate_mock)
    monkeypatch.setattr(cli.compute, "cluster_exists_and_active", lambda *a, **k: True)

    client = MagicMock()
    exit_code = cli.cmd_run_all(client, cfg, SimpleNamespace())

    assert exit_code == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["compute_mode"] == "explicit_cluster"
    assert report["cluster_id"] == "cluster-1"
    assert report["cluster_cleaned_up"] is True

    # jobs.ensure_job / reset_job_cluster were called with cluster_id set and
    # new_cluster left None -- the explicit-cluster wiring, not the fallback.
    _, kwargs = ensure_job_mock.call_args
    assert kwargs["cluster_id"] == "cluster-1"
    assert kwargs["new_cluster"] is None
    _, kwargs = reset_job_cluster_mock.call_args
    assert kwargs["cluster_id"] == "cluster-1"
    assert kwargs["new_cluster"] is None

    terminate_mock.assert_called_once_with(client, "cluster-1")


def test_permitted_fallback_to_new_cluster_when_creation_is_rejected(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _patch_common_success_path(monkeypatch)
    rejection = InvalidParameterValue("cluster policy forbids this configuration")
    monkeypatch.setattr(cli.compute, "try_start_cluster_create", lambda *a, **k: (None, rejection))
    # poll_cluster_state must NOT be called in job_cluster mode -- fail the
    # test loudly if cmd_run_all calls it anyway.
    monkeypatch.setattr(
        cli.monitoring,
        "poll_cluster_state",
        MagicMock(
            side_effect=AssertionError("poll_cluster_state must not run in job_cluster mode")
        ),
    )
    ensure_job_mock = MagicMock(return_value=42)
    reset_job_cluster_mock = MagicMock()
    _patch_job_success_path(monkeypatch, ensure_job_mock, reset_job_cluster_mock)

    terminate_mock = MagicMock()
    monkeypatch.setattr(cli.compute, "terminate_cluster", terminate_mock)
    monkeypatch.setattr(
        cli.compute,
        "cluster_exists_and_active",
        MagicMock(side_effect=AssertionError("nothing to clean up in job_cluster mode")),
    )

    client = MagicMock()
    exit_code = cli.cmd_run_all(client, cfg, SimpleNamespace())

    assert exit_code == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["compute_mode"] == "job_cluster"
    assert report["cluster_id"] is None
    # No standalone cluster cleanup is needed/attempted in this mode.
    assert report["cluster_cleaned_up"] is True
    terminate_mock.assert_not_called()

    # jobs.ensure_job / reset_job_cluster were called with a new_cluster
    # dict and no cluster_id -- the fallback wiring, not the explicit path.
    _, kwargs = ensure_job_mock.call_args
    assert kwargs["cluster_id"] is None
    assert isinstance(kwargs["new_cluster"], dict)
    assert kwargs["new_cluster"]["node_type_id"] == "small"
    _, kwargs = reset_job_cluster_mock.call_args
    assert kwargs["cluster_id"] is None
    assert isinstance(kwargs["new_cluster"], dict)

    # Exactly one stable job -- never a duplicate -- regardless of mode.
    ensure_job_mock.assert_called_once()
    reset_job_cluster_mock.assert_called_once()


def test_unrelated_cluster_error_fails_the_run_instead_of_falling_back(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _patch_common_success_path(monkeypatch)

    def _boom(*args, **kwargs):
        raise RuntimeError("network error talking to the clusters API")

    monkeypatch.setattr(cli.compute, "try_start_cluster_create", _boom)

    ensure_job_mock = MagicMock()
    reset_job_cluster_mock = MagicMock()
    monkeypatch.setattr(cli.jobs, "ensure_job", ensure_job_mock)
    monkeypatch.setattr(cli.jobs, "reset_job_cluster", reset_job_cluster_mock)

    client = MagicMock()
    exit_code = cli.cmd_run_all(client, cfg, SimpleNamespace())

    assert exit_code != 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert "network error" in report["error"]
    assert report["compute_mode"] is None

    # The run must fail outright, not silently proceed in either compute mode.
    ensure_job_mock.assert_not_called()
    reset_job_cluster_mock.assert_not_called()

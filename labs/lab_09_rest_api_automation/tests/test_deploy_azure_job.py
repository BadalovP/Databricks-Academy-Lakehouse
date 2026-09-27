"""Tests for scripts/deploy_azure_job.py -- the idempotent Azure three-task
Job deployment (ingestion -> Lakeflow pipeline -> reconciliation). Every
external Databricks call is mocked; this file never touches live
infrastructure and never triggers a run.
"""

from unittest.mock import MagicMock

import deploy_azure_job as daj


def _cfg() -> dict:
    return {
        "catalog": "dbr_dev",
        "schema": "parvinbadalov_lab09_prod",
        "volume": "lab09_landing",
        "pipeline": {"name": "lab09_taxi_pipeline_v2", "target_schema": "parvinbadalov_lab09_prod"},
        "tables": {
            "bronze": "lab09_taxi_bronze",
            "silver": "lab09_taxi_silver",
            "quarantine": "lab09_taxi_quarantine",
            "gold": "lab09_taxi_daily_summary",
        },
        "job_compute": {
            "policy_name": "Job Compute",
            "node_type_id": "Standard_F4",
            "num_workers": 1,
        },
        "job": {
            "name": "lab09_taxi_reconciliation_job",
            "ingestion_task_key": "ingestion",
            "pipeline_task_key": "lakeflow_pipeline",
            "reconciliation_task_key": "reconciliation",
            "shared_job_cluster_key": "lab09_shared_compute",
        },
    }


def _spark_version(key: str, name: str) -> MagicMock:
    v = MagicMock()
    v.key = key
    v.name = name
    return v


def _node_type(node_type_id: str) -> MagicMock:
    nt = MagicMock()
    nt.node_type_id = node_type_id
    nt.num_cores = 4
    nt.memory_mb = 16384
    nt.is_deprecated = False
    nt.is_graviton = False
    return nt


def _autospec_client() -> MagicMock:
    return MagicMock()


def test_build_shared_job_cluster_excludes_cluster_name_and_autotermination():
    """The shared job cluster must use the corrected as_new_cluster_dict()
    shape -- omitting cluster_name and autotermination_minutes, which this
    workspace's Job Compute policy rejects outright (see compute.py's
    ClusterSpec.as_new_cluster_dict() regression test for the live-confirmed
    reason).
    """
    client = _autospec_client()
    client.clusters.spark_versions.return_value.versions = [
        _spark_version("15.4.x-scala2.12", "15.4 LTS")
    ]
    policy = MagicMock()
    policy.name = "Job Compute"
    policy.policy_id = "job-compute-policy-id"
    client.cluster_policies.list.return_value = [policy]

    job_cluster = daj.build_shared_job_cluster(client, _cfg())

    assert job_cluster.job_cluster_key == "lab09_shared_compute"
    assert job_cluster.new_cluster.cluster_name is None
    assert job_cluster.new_cluster.autotermination_minutes is None
    assert job_cluster.new_cluster.node_type_id == "Standard_F4"
    assert job_cluster.new_cluster.num_workers == 1
    assert job_cluster.new_cluster.policy_id == "job-compute-policy-id"


def test_build_task_graph_has_correct_dependencies_and_shared_compute():
    cfg = _cfg()
    shared_cluster = MagicMock()
    shared_cluster.job_cluster_key = "lab09_shared_compute"

    job_clusters, tasks = daj.build_task_graph(
        cfg,
        pipeline_id="pipeline-123",
        ingestion_notebook_path="/Workspace/x/lab09/notebooks/02_ingest_data",
        reconciliation_notebook_path="/Workspace/x/lab09/notebooks/01_reconcile_counts",
        shared_cluster=shared_cluster,
    )

    assert job_clusters == [shared_cluster]
    by_key = {t.task_key: t for t in tasks}
    assert set(by_key) == {"ingestion", "lakeflow_pipeline", "reconciliation"}

    ingestion = by_key["ingestion"]
    assert ingestion.job_cluster_key == "lab09_shared_compute"
    assert ingestion.notebook_task.notebook_path == "/Workspace/x/lab09/notebooks/02_ingest_data"
    assert not ingestion.depends_on

    pipeline_task = by_key["lakeflow_pipeline"]
    assert pipeline_task.pipeline_task.pipeline_id == "pipeline-123"
    assert [d.task_key for d in pipeline_task.depends_on] == ["ingestion"]
    # The pipeline task must use its own pipeline-managed compute, never the
    # ingestion/reconciliation tasks' shared job cluster.
    assert pipeline_task.job_cluster_key is None

    reconciliation = by_key["reconciliation"]
    assert reconciliation.job_cluster_key == "lab09_shared_compute"
    assert [d.task_key for d in reconciliation.depends_on] == ["lakeflow_pipeline"]
    assert reconciliation.notebook_task.base_parameters["schema"] == "parvinbadalov_lab09_prod"
    assert reconciliation.notebook_task.base_parameters["bronze_table"] == "lab09_taxi_bronze"


def test_ensure_three_task_job_creates_when_missing():
    client = _autospec_client()
    client.jobs.list.return_value = []
    client.jobs.create.return_value = MagicMock(job_id=555)

    job_id, created = daj.ensure_three_task_job(client, _cfg(), job_clusters=[], tasks=[])

    assert job_id == 555
    assert created is True
    client.jobs.create.assert_called_once()
    client.jobs.reset.assert_not_called()


def test_ensure_three_task_job_resets_existing_job_instead_of_duplicating():
    client = _autospec_client()
    existing = MagicMock()
    existing.job_id = 42
    existing.settings.name = "lab09_taxi_reconciliation_job"
    client.jobs.list.return_value = [existing]

    job_id, created = daj.ensure_three_task_job(client, _cfg(), job_clusters=[], tasks=[])

    assert job_id == 42
    assert created is False
    client.jobs.create.assert_not_called()
    client.jobs.reset.assert_called_once()
    _, kwargs = client.jobs.reset.call_args
    assert kwargs["job_id"] == 42


def test_deploy_never_triggers_a_run(monkeypatch):
    """deploy() must only ensure resources exist -- it must never call
    run_now() or otherwise start a Job run or pipeline update by itself.
    """
    client = _autospec_client()
    client.current_user.me.return_value.user_name = "test.user@example.com"
    client.config.host = "https://adb-example.1.azuredatabricks.net"

    monkeypatch.setattr(daj.volumes, "ensure_output_schema", MagicMock())
    monkeypatch.setattr(daj.volumes, "ensure_volume", MagicMock())
    monkeypatch.setattr(daj.workspace, "upload_pipeline_sources", MagicMock())
    monkeypatch.setattr(
        daj.workspace, "upload_notebook", MagicMock(side_effect=["/reconcile", "/ingest"])
    )
    monkeypatch.setattr(
        daj.workspace, "pipeline_source_dir", MagicMock(return_value="/Workspace/x/lab09/pipeline")
    )
    monkeypatch.setattr(
        daj.pipelines, "ensure_pipeline", MagicMock(return_value=("pipeline-1", True))
    )
    monkeypatch.setattr(daj, "upload_project_source", MagicMock(return_value=("/root/src", [])))
    monkeypatch.setattr(daj, "upload_azure_config", MagicMock())
    monkeypatch.setattr(daj, "build_shared_job_cluster", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(daj, "ensure_three_task_job", MagicMock(return_value=(99, True)))
    run_now_mock = MagicMock()
    client.jobs.run_now = run_now_mock

    report = daj.deploy(client, _cfg(), config_path=daj.DEFAULT_CONFIG_PATH)

    assert report.job_id == 99
    assert report.job_created is True
    assert report.pipeline_id == "pipeline-1"
    run_now_mock.assert_not_called()
    client.pipelines.start_update.assert_not_called()

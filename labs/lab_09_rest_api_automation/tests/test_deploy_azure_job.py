"""Tests for scripts/deploy_azure_job.py -- the idempotent Azure three-task
Job deployment (ingestion -> Lakeflow pipeline -> reconciliation). Every
external Databricks call is mocked; this file never touches live
infrastructure and never triggers a run.
"""

from unittest.mock import MagicMock

import deploy_azure_job as daj
import pytest


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

    job_id, created = daj.ensure_three_task_job(
        client, _cfg(), job_clusters=[], tasks=[], identity="me@example.com"
    )

    assert job_id == 555
    assert created is True
    client.jobs.create.assert_called_once()
    client.jobs.reset.assert_not_called()


def test_ensure_three_task_job_resets_existing_job_instead_of_duplicating():
    client = _autospec_client()
    existing = MagicMock()
    existing.job_id = 42
    existing.settings.name = "lab09_taxi_reconciliation_job"
    existing.creator_user_name = "me@example.com"
    client.jobs.list.return_value = [existing]

    job_id, created = daj.ensure_three_task_job(
        client, _cfg(), job_clusters=[], tasks=[], identity="me@example.com"
    )

    assert job_id == 42
    assert created is False
    client.jobs.create.assert_not_called()
    client.jobs.reset.assert_called_once()
    _, kwargs = client.jobs.reset.call_args
    assert kwargs["job_id"] == 42


def test_ensure_three_task_job_refuses_to_reset_a_job_owned_by_someone_else():
    """Regression test: this Azure workspace is shared by 100+ other
    students -- a same-named Job found by name must never be reset unless
    its own creator_user_name positively matches the current identity.
    """
    client = _autospec_client()
    existing = MagicMock()
    existing.job_id = 42
    existing.settings.name = "lab09_taxi_reconciliation_job"
    existing.creator_user_name = "someone.else@example.com"
    client.jobs.list.return_value = [existing]

    with pytest.raises(daj.ResourceOwnershipError):
        daj.ensure_three_task_job(
            client, _cfg(), job_clusters=[], tasks=[], identity="me@example.com"
        )

    client.jobs.reset.assert_not_called()
    client.jobs.create.assert_not_called()


def test_verify_owned_by_current_identity_accepts_exact_match():
    resource = MagicMock(creator_user_name="me@example.com")
    daj.verify_owned_by_current_identity(resource, "me@example.com", "Job 'x'")  # no raise


def test_verify_owned_by_current_identity_rejects_missing_creator_field():
    """A resource object with no creator_user_name at all (e.g. a mock or a
    genuinely malformed response) must never be treated as owned by
    default -- getattr's fallback (None) must never equal a real identity.
    """
    resource = object()
    with pytest.raises(daj.ResourceOwnershipError):
        daj.verify_owned_by_current_identity(resource, "me@example.com", "Job 'x'")


# --- explicit, ID-based identity-migration allowlist ------------------------


def test_migration_allowlist_permits_the_two_known_approved_resources():
    """The two resources actually migrated from github-lab08-travelops to
    the dedicated Lab 9 identity must be allowed through -- by exact id AND
    exact recorded prior creator, never by id alone.
    """
    old_creator = "3ec7e8df-66a2-4102-ab57-e4448b4e0e01"
    job = MagicMock(creator_user_name=old_creator)
    daj.verify_owned_by_current_identity(
        job, "new-identity", "Job 'lab09_taxi_reconciliation_job'", resource_id="374991019372414"
    )  # no raise

    pipeline = MagicMock(creator_user_name=old_creator)
    daj.verify_owned_by_current_identity(
        pipeline,
        "new-identity",
        "Pipeline 'lab09_taxi_pipeline_v2'",
        resource_id="93a49a14-366e-4224-b2b3-587ef0b7a028",
    )  # no raise


def test_migration_allowlist_rejects_a_listed_id_with_the_wrong_actual_creator():
    """Listing an id is not enough by itself -- the resource's actual
    creator must also exactly match that entry's own recorded prior
    creator. A different creator at the same id must still be refused.
    """
    with pytest.raises(daj.ResourceOwnershipError):
        daj.verify_owned_by_current_identity(
            MagicMock(creator_user_name="someone.else@example.com"),
            "new-identity",
            "Job 'lab09_taxi_reconciliation_job'",
            resource_id="374991019372414",
        )


def test_migration_allowlist_never_applies_to_an_unlisted_id():
    """A resource id that is not on the explicit allowlist must always
    require an exact creator match, even if its creator happens to match
    some other allowlisted entry's prior creator by coincidence.
    """
    old_creator = "3ec7e8df-66a2-4102-ab57-e4448b4e0e01"
    with pytest.raises(daj.ResourceOwnershipError):
        daj.verify_owned_by_current_identity(
            MagicMock(creator_user_name=old_creator),
            "new-identity",
            "Job 'some-other-job'",
            resource_id="9999999999999",
        )


def test_migration_allowlist_never_applies_when_resource_id_is_not_provided():
    """Omitting resource_id entirely (as every pre-migration call site did)
    must fall back to requiring an exact creator match -- the allowlist is
    opt-in per call site, never a default bypass.
    """
    old_creator = "3ec7e8df-66a2-4102-ab57-e4448b4e0e01"
    with pytest.raises(daj.ResourceOwnershipError):
        daj.verify_owned_by_current_identity(
            MagicMock(creator_user_name=old_creator), "new-identity", "Job 'x'"
        )


def test_ensure_three_task_job_allows_reset_of_the_approved_migrated_job():
    client = _autospec_client()
    existing = MagicMock()
    existing.job_id = 374991019372414
    existing.settings.name = "lab09_taxi_reconciliation_job"
    existing.creator_user_name = "3ec7e8df-66a2-4102-ab57-e4448b4e0e01"
    client.jobs.list.return_value = [existing]

    job_id, created = daj.ensure_three_task_job(
        client, _cfg(), job_clusters=[], tasks=[], identity="new-identity"
    )

    assert job_id == 374991019372414
    assert created is False
    client.jobs.reset.assert_called_once()


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
    monkeypatch.setattr(daj.pipelines, "find_pipeline_by_name", MagicMock(return_value=None))
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


def test_deploy_refuses_to_update_a_pipeline_owned_by_someone_else(monkeypatch):
    client = _autospec_client()
    client.current_user.me.return_value.user_name = "me@example.com"
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
    existing_pipeline = MagicMock(creator_user_name="someone.else@example.com")
    monkeypatch.setattr(
        daj.pipelines, "find_pipeline_by_name", MagicMock(return_value=existing_pipeline)
    )
    ensure_pipeline_mock = MagicMock()
    monkeypatch.setattr(daj.pipelines, "ensure_pipeline", ensure_pipeline_mock)
    ensure_job_mock = MagicMock()
    monkeypatch.setattr(daj, "ensure_three_task_job", ensure_job_mock)

    with pytest.raises(daj.ResourceOwnershipError):
        daj.deploy(client, _cfg(), config_path=daj.DEFAULT_CONFIG_PATH)

    ensure_pipeline_mock.assert_not_called()
    ensure_job_mock.assert_not_called()

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from urbanflow.automation import (
    APPROVED_RUN_CLUSTER_IDS,
    PROTECTED_SHARED_CLUSTER_IDS,
    WorkspaceSafetyError,
    require_running_cluster,
    resolve_protected_cluster_ids,
    terminate_and_verify_cluster,
    upload_source_notebook,
)
from urbanflow.config import load_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GP1_ID = "0702-132442-toro5spu"
GP2_ID = "0702-171207-xo9bbc0y"
CONFIG_FILES = ("config/dev.yml", "config/azure.yml")
MUTATING_CLUSTER_CALLS = (
    "start",
    "restart",
    "edit",
    "resize",
    "delete",
    "permanent_delete",
    "create",
)


def _assert_no_cluster_mutation(client: Mock) -> None:
    """Read-only means read-only: no state change and no library installed."""
    for call_name in MUTATING_CLUSTER_CALLS:
        assert getattr(client.clusters, call_name).call_count == 0, call_name
    assert client.libraries.install.call_count == 0


def test_notebook_job_uses_existing_cluster_and_defaults_to_dry_run() -> None:
    resource = yaml.safe_load(
        (PROJECT_ROOT / "resources/retired/component_jobs.yml").read_text(encoding="utf-8")
    )
    job = resource["resources"]["jobs"]["urbanflow_bounded_stream_test"]
    task = job["tasks"][0]

    assert task["existing_cluster_id"] == "${var.compute_cluster_id}"
    assert "new_cluster" not in task
    assert job["parameters"][0] == {"name": "run_stream", "default": "false"}
    assert task["notebook_task"]["notebook_path"].endswith("03_eventhubs_to_bronze.py")
    assert "schedule" not in job


def test_lakeflow_uses_managed_serverless_compute_without_gp_cluster() -> None:
    path = PROJECT_ROOT / "resources/pipelines.yml"
    text = path.read_text(encoding="utf-8")
    pipeline = yaml.safe_load(text)["resources"]["pipelines"]["urbanflow_pipeline"]

    assert pipeline["serverless"] is True
    assert pipeline["continuous"] is False
    assert "existing_cluster_id" not in text
    assert "0702-132442-toro5spu" not in text
    assert "0702-171207-xo9bbc0y" not in text
    assert (
        not (PROJECT_ROOT / "pipeline/bronze.py")
        .read_text(encoding="utf-8")
        .startswith("# Databricks notebook source")
    )


def test_storage_bootstrap_is_isolated_and_non_destructive() -> None:
    text = (PROJECT_ROOT / "sql/00_prepare_urbanflow_storage.sql").read_text(encoding="utf-8")

    assert "dbr_dev.parvinbadalov_urbanflow" in text
    assert "urbanflow_landing" in text
    assert "IF NOT EXISTS" in text
    assert "DROP " not in text.upper()
    assert "REPLACE " not in text.upper()


def test_never_terminate_and_approved_to_run_are_two_separate_lists() -> None:
    # Same members today, different meanings: they must stay independent names so that
    # either one can change without silently changing the other.
    assert PROTECTED_SHARED_CLUSTER_IDS is not APPROVED_RUN_CLUSTER_IDS
    assert isinstance(PROTECTED_SHARED_CLUSTER_IDS, frozenset)
    assert isinstance(APPROVED_RUN_CLUSTER_IDS, frozenset)
    assert PROTECTED_SHARED_CLUSTER_IDS == {GP1_ID, GP2_ID}
    assert APPROVED_RUN_CLUSTER_IDS == {GP1_ID, GP2_ID}


def test_approved_run_list_is_documented_as_distinct_from_the_denylist() -> None:
    text = (PROJECT_ROOT / "src/urbanflow/automation.py").read_text(encoding="utf-8")

    assert "# DENYLIST" in text
    assert "# ALLOWLIST" in text


def test_running_shared_cluster_passes_the_preflight_without_mutating_anything() -> None:
    client = Mock()
    client.clusters.get.return_value = SimpleNamespace(state="RUNNING")

    assert require_running_cluster(client, GP1_ID) == "RUNNING"

    client.clusters.get.assert_called_once_with(cluster_id=GP1_ID)
    _assert_no_cluster_mutation(client)


@pytest.mark.parametrize("state", ["TERMINATED", "PENDING", "TERMINATING", "RESTARTING"])
def test_preflight_refuses_any_non_running_state_and_never_starts_the_cluster(state: str) -> None:
    client = Mock()
    client.clusters.get.return_value = SimpleNamespace(state=state)

    with pytest.raises(WorkspaceSafetyError, match=f"is {state}, not RUNNING"):
        require_running_cluster(client, GP2_ID)

    _assert_no_cluster_mutation(client)


def test_preflight_tells_the_operator_to_wait_instead_of_starting_it() -> None:
    client = Mock()
    client.clusters.get.return_value = SimpleNamespace(state="TERMINATED")

    with pytest.raises(WorkspaceSafetyError, match="wait until its owner starts it"):
        require_running_cluster(client, GP2_ID)


def test_hardcoded_protection_survives_a_config_that_omits_gp2(tmp_path: Path) -> None:
    replacement_id = "0929-101010-extra01"
    source = (PROJECT_ROOT / "config/dev.yml").read_text(encoding="utf-8")
    thinner = tmp_path / "omits-gp2.yml"
    thinner.write_text(source.replace(GP2_ID, replacement_id), encoding="utf-8")
    compute = load_config(thinner).compute

    assert GP2_ID not in compute.declared_protected_cluster_ids
    # The frozenset in automation.py is the floor, so YAML can never unprotect GP2.
    assert GP2_ID in resolve_protected_cluster_ids(compute)

    client = Mock()
    with pytest.raises(WorkspaceSafetyError, match="protected shared cluster"):
        terminate_and_verify_cluster(client, GP2_ID, compute=compute)
    _assert_no_cluster_mutation(client)


def test_config_can_add_protection_for_a_third_cluster(tmp_path: Path) -> None:
    extra_id = "0929-202020-extra02"
    source = (PROJECT_ROOT / "config/dev.yml").read_text(encoding="utf-8")
    wider = tmp_path / "adds-gp3.yml"
    wider.write_text(
        source.replace(
            "  clusters:\n",
            "  clusters:\n"
            "    gp3:\n"
            f"      cluster_id: {extra_id}\n"
            "      expected_name: GP3\n"
            "      expected_spark_version: 17.3.x-scala2.13\n"
            "      expected_data_security_mode: USER_ISOLATION\n"
            "      verified_state: TERMINATED\n"
            "      verified_at: 2026-09-29\n"
            "      unity_catalog_compatible: true\n"
            "      kafka_compatible: true\n",
        ),
        encoding="utf-8",
    )
    compute = load_config(wider).compute

    assert resolve_protected_cluster_ids(compute) >= {GP1_ID, GP2_ID, extra_id}

    client = Mock()
    with pytest.raises(WorkspaceSafetyError, match="protected shared cluster"):
        terminate_and_verify_cluster(client, extra_id, compute=compute)
    _assert_no_cluster_mutation(client)


def test_notebook_upload_is_allowed_inside_the_users_own_folder(tmp_path: Path) -> None:
    local = tmp_path / "03_eventhubs_to_bronze.py"
    local.write_text("# Databricks notebook source\n", encoding="utf-8")
    client = Mock()

    upload_source_notebook(
        client,
        local,
        "/Workspace/Users/student@example.com/urbanflow/03_eventhubs_to_bronze.py",
        expected_user_name="student@example.com",
    )

    assert client.workspace.import_.call_count == 1
    assert client.workspace.import_.call_args.kwargs["overwrite"] is True


@pytest.mark.parametrize(
    "workspace_path",
    [
        "/Workspace/Users/other@example.com/urbanflow/03_eventhubs_to_bronze.py",
        "/Workspace/Shared/urbanflow/03_eventhubs_to_bronze.py",
        "/Workspace/Users/student@example.com.attacker/nb.py",
        "/Workspace/Users/student@example.com/../other@example.com/nb.py",
        "/Workspace/Users/student@example.com",
    ],
)
def test_notebook_upload_refuses_any_path_outside_that_folder(
    tmp_path: Path, workspace_path: str
) -> None:
    local = tmp_path / "03_eventhubs_to_bronze.py"
    local.write_text("# Databricks notebook source\n", encoding="utf-8")
    client = Mock()

    with pytest.raises(WorkspaceSafetyError, match="Refusing to upload"):
        upload_source_notebook(
            client,
            local,
            workspace_path,
            expected_user_name="student@example.com",
        )

    client.workspace.import_.assert_not_called()


@pytest.mark.parametrize("config_file", CONFIG_FILES)
def test_config_records_the_latest_2026_09_29_read_only_cluster_check(config_file: str) -> None:
    clusters = load_config(PROJECT_ROOT / config_file).compute.clusters

    assert clusters["gp1"].verified_state == "RUNNING"
    assert clusters["gp2"].verified_state == "TERMINATED"
    assert clusters["gp1"].verified_at == "2026-09-29"
    assert clusters["gp2"].verified_at == "2026-09-29"


@pytest.mark.parametrize("config_file", CONFIG_FILES)
def test_config_keeps_every_shared_compute_safety_flag_closed(config_file: str) -> None:
    compute = load_config(PROJECT_ROOT / config_file).compute

    assert compute.allow_start is False
    assert compute.allow_restart is False
    assert compute.allow_resize is False
    assert compute.allow_terminate is False
    assert compute.educational_cluster_creation_enabled is False
    assert compute.protected_from_termination is True
    assert compute.required_state == "RUNNING"

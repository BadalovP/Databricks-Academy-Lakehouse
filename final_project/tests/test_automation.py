from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow.automation import (
    PROTECTED_SHARED_CLUSTER_IDS,
    WorkspaceSafetyError,
    assess_existing_cluster,
    create_demonstration_cluster,
    select_ready_existing_cluster,
    terminate_and_verify_cluster,
    verify_workspace,
)
from urbanflow.config import load_config
from urbanflow.monitoring import poll_state

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _mock_attach_permission(client: Mock) -> None:
    client.current_user.me.return_value = SimpleNamespace(
        user_name="student@example.com",
        groups=[SimpleNamespace(display="users")],
    )
    client.permissions.get.return_value = SimpleNamespace(
        access_control_list=[
            SimpleNamespace(
                user_name=None,
                group_name="users",
                service_principal_name=None,
                all_permissions=[SimpleNamespace(permission_level="CAN_RESTART")],
            )
        ]
    )


def test_poll_state_times_out_explicitly() -> None:
    times = iter([0.0, 0.0, 1.0, 2.0])
    result = poll_state(
        lambda: "RUNNING",
        terminal_states={"TERMINATED"},
        timeout_seconds=1,
        poll_interval_seconds=0,
        clock=lambda: next(times),
        sleep=lambda _: None,
    )
    assert result.timed_out is True
    assert result.state == "RUNNING"


def test_workspace_host_mismatch_fails_before_identity_call() -> None:
    client = Mock()
    client.config.host = "https://unexpected.azuredatabricks.net"
    with pytest.raises(WorkspaceSafetyError, match="does not match"):
        verify_workspace(client, "https://expected.azuredatabricks.net")
    client.current_user.me.assert_not_called()


def test_workspace_identity_is_returned_for_expected_host() -> None:
    client = Mock()
    client.config.host = "https://adb-1.azuredatabricks.net/"
    client.current_user.me.return_value = SimpleNamespace(user_name="student@example.com")
    identity = verify_workspace(client, "adb-1.azuredatabricks.net")
    assert identity.user_name == "student@example.com"


def test_cluster_cleanup_requires_exact_terminated_state() -> None:
    client = Mock()
    client.clusters.get.return_value = SimpleNamespace(state="TERMINATED")
    result = terminate_and_verify_cluster(
        client, "cluster-1", timeout_seconds=0, poll_interval_seconds=0
    )
    client.clusters.delete.assert_called_once_with(cluster_id="cluster-1")
    assert result.terminal is True
    assert result.state == "TERMINATED"


def test_protected_shared_cluster_can_never_be_terminated() -> None:
    client = Mock()
    gp1_id = next(iter(PROTECTED_SHARED_CLUSTER_IDS))

    with pytest.raises(WorkspaceSafetyError, match="protected shared cluster"):
        terminate_and_verify_cluster(client, gp1_id)

    client.clusters.delete.assert_not_called()


def test_educational_cluster_creation_is_disabled_by_default() -> None:
    client = Mock()

    with pytest.raises(WorkspaceSafetyError, match="creation is disabled"):
        create_demonstration_cluster(
            client,
            cluster_name="urbanflow-educational-only",
            spark_version="17.3.x-scala2.13",
            node_type_id="Standard_F4",
            policy_id="policy",
        )

    client.clusters.create.assert_not_called()


def test_cluster_assessment_is_compatible_but_not_ready_when_terminated() -> None:
    config = load_config(PROJECT_ROOT / "config/dev.yml")
    client = Mock()
    client.clusters.get.return_value = SimpleNamespace(
        cluster_name="GP1",
        state="TERMINATED",
        spark_version="17.3.x-scala2.13",
        data_security_mode="USER_ISOLATION",
    )
    _mock_attach_permission(client)

    result = assess_existing_cluster(client, config.compute.preferred_cluster)

    assert result.compatible is True
    assert result.ready is False
    assert result.reasons == ("state is TERMINATED, required RUNNING",)


def test_cluster_selection_falls_back_to_running_gp2() -> None:
    config = load_config(PROJECT_ROOT / "config/dev.yml")
    client = Mock()
    client.clusters.get.side_effect = [
        SimpleNamespace(
            cluster_name="GP1",
            state="TERMINATED",
            spark_version="17.3.x-scala2.13",
            data_security_mode="USER_ISOLATION",
        ),
        SimpleNamespace(
            cluster_name="GP2",
            state="RUNNING",
            spark_version="17.3.x-scala2.13",
            data_security_mode="USER_ISOLATION",
        ),
    ]
    _mock_attach_permission(client)

    selected, assessments = select_ready_existing_cluster(client, config.compute)

    assert len(assessments) == 2
    assert selected is not None
    assert selected.alias == "gp2"

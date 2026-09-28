from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow.automation import (
    WorkspaceSafetyError,
    terminate_and_verify_cluster,
    verify_workspace,
)
from urbanflow.monitoring import poll_state


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

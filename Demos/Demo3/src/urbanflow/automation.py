"""Safe Databricks SDK primitives for the later UrbanFlow automation stage."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from urbanflow.config import normalize_host
from urbanflow.monitoring import PollResult, poll_state, state_name


class WorkspaceSafetyError(RuntimeError):
    """Raised before an SDK call can target an unexpected workspace."""


@dataclass(frozen=True)
class WorkspaceIdentity:
    host: str
    user_name: str


@dataclass(frozen=True)
class ClusterExecution:
    cluster_id: str
    start: PollResult
    cleanup: PollResult | None = None


def workspace_client(profile: str, expected_host: str) -> Any:
    """Construct a profile-based client and reject a host mismatch immediately."""
    if not profile:
        raise WorkspaceSafetyError("An explicit Databricks profile is required.")
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    actual_host = normalize_host(str(client.config.host))
    wanted_host = normalize_host(expected_host)
    if actual_host != wanted_host:
        raise WorkspaceSafetyError(
            f"Profile {profile!r} resolved to {actual_host!r}, expected {wanted_host!r}."
        )
    return client


def verify_workspace(client: Any, expected_host: str) -> WorkspaceIdentity:
    actual_host = normalize_host(str(client.config.host))
    wanted_host = normalize_host(expected_host)
    if actual_host != wanted_host:
        raise WorkspaceSafetyError(
            f"Resolved Databricks host {actual_host!r} does not match {wanted_host!r}."
        )
    current_user = client.current_user.me()
    return WorkspaceIdentity(host=actual_host, user_name=str(current_user.user_name))


def upload_source_notebook(client: Any, local_path: str | Path, workspace_path: str) -> None:
    """Upload one Databricks source notebook with overwrite made explicit."""
    from databricks.sdk.service.workspace import ImportFormat, Language

    content = base64.b64encode(Path(local_path).read_bytes()).decode("ascii")
    client.workspace.import_(
        path=workspace_path,
        content=content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )


def poll_job_run(
    client: Any,
    run_id: int,
    *,
    timeout_seconds: float = 1800,
    poll_interval_seconds: float = 10,
) -> PollResult:
    def fetch() -> str:
        run = client.jobs.get_run(run_id=run_id)
        return state_name(run.state.life_cycle_state)

    return poll_state(
        fetch,
        terminal_states={"TERMINATED", "SKIPPED", "INTERNAL_ERROR"},
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def poll_pipeline_update(
    client: Any,
    pipeline_id: str,
    update_id: str,
    *,
    timeout_seconds: float = 1800,
    poll_interval_seconds: float = 10,
) -> PollResult:
    def fetch() -> str:
        response = client.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        update = getattr(response, "update", response)
        return state_name(update.state)

    return poll_state(
        fetch,
        terminal_states={"COMPLETED", "FAILED", "CANCELED"},
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def terminate_and_verify_cluster(
    client: Any,
    cluster_id: str,
    *,
    timeout_seconds: float = 900,
    poll_interval_seconds: float = 10,
) -> PollResult:
    """Request deletion and accept success only after an exact TERMINATED state."""
    client.clusters.delete(cluster_id=cluster_id)

    def fetch() -> str:
        return state_name(client.clusters.get(cluster_id=cluster_id).state)

    return poll_state(
        fetch,
        terminal_states={"TERMINATED"},
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def create_demonstration_cluster(
    client: Any,
    *,
    cluster_name: str,
    spark_version: str,
    node_type_id: str,
    policy_id: str,
    timeout_seconds: float = 1200,
) -> ClusterExecution:
    """Create one tagged single-node cluster; caller must always invoke cleanup."""
    response = client.clusters.create(
        cluster_name=cluster_name,
        spark_version=spark_version,
        node_type_id=node_type_id,
        num_workers=0,
        policy_id=policy_id,
        autotermination_minutes=10,
        custom_tags={"project": "urbanflow", "purpose": "api-automation-demo"},
    )
    cluster_id = str(response.cluster_id)

    def fetch() -> str:
        return state_name(client.clusters.get(cluster_id=cluster_id).state)

    start = poll_state(
        fetch,
        terminal_states={"RUNNING", "TERMINATED", "ERROR"},
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=10,
    )
    return ClusterExecution(cluster_id=cluster_id, start=start)

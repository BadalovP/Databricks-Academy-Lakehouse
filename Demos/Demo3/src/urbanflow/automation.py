"""Safe Databricks SDK primitives for the later UrbanFlow automation stage."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from urbanflow.config import ComputeSettings, ExistingCluster, normalize_host
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


@dataclass(frozen=True)
class ExistingClusterAssessment:
    alias: str
    cluster_id: str
    name: str
    state: str
    spark_version: str
    data_security_mode: str
    permission_levels: tuple[str, ...]
    compatible: bool
    ready: bool
    reasons: tuple[str, ...]


# DENYLIST - "never terminate these". Only terminate_and_verify_cluster reads this set.
# Adding an id here takes a destructive power away; it never grants permission to run.
PROTECTED_SHARED_CLUSTER_IDS = frozenset(
    {
        "0702-132442-toro5spu",  # GP1
        "0702-171207-xo9bbc0y",  # GP2
    }
)

# ALLOWLIST - "a live, billable UrbanFlow run may attach here". The bounded-stream
# notebook reads this set. It is deliberately a SECOND set with the same members today:
# protecting a newly found cluster from deletion must not authorize billable runs on it,
# and allowing a cluster to be terminated must not block legitimate runs on it.
# The two lists mean different things, so never merge them back into one.
APPROVED_RUN_CLUSTER_IDS = frozenset(
    {
        "0702-132442-toro5spu",  # GP1
        "0702-171207-xo9bbc0y",  # GP2
    }
)


def resolve_protected_cluster_ids(compute: ComputeSettings | None = None) -> frozenset[str]:
    """Return every never-terminate id: the hardcoded floor plus anything the YAML declares.

    Single source of truth for termination protection. Configuration can only ADD ids,
    so editing a YAML file can never remove protection from GP1 or GP2.
    """
    if compute is None:
        return PROTECTED_SHARED_CLUSTER_IDS
    return PROTECTED_SHARED_CLUSTER_IDS | compute.declared_protected_cluster_ids


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


def assess_existing_cluster(
    client: Any,
    configured: ExistingCluster,
    *,
    required_state: str = "RUNNING",
) -> ExistingClusterAssessment:
    """Inspect one shared cluster without starting, restarting, editing, or terminating it."""
    actual = client.clusters.get(cluster_id=configured.cluster_id)
    identity = client.current_user.me()
    principals = {str(identity.user_name)}
    principals.update(str(group.display) for group in (getattr(identity, "groups", None) or []))
    permission_response = client.permissions.get("clusters", configured.cluster_id)
    effective_permissions: set[str] = set()
    for entry in permission_response.access_control_list or []:
        entry_principals = {
            str(value)
            for value in (
                getattr(entry, "user_name", None),
                getattr(entry, "group_name", None),
                getattr(entry, "service_principal_name", None),
            )
            if value
        }
        if principals.isdisjoint(entry_principals):
            continue
        effective_permissions.update(
            state_name(permission.permission_level) for permission in (entry.all_permissions or [])
        )
    if "CAN_MANAGE" in effective_permissions:
        effective_permissions.update({"CAN_RESTART", "CAN_ATTACH_TO"})
    elif "CAN_RESTART" in effective_permissions:
        effective_permissions.add("CAN_ATTACH_TO")
    permission_levels = tuple(sorted(effective_permissions))
    name = str(actual.cluster_name)
    state = state_name(actual.state)
    spark_version = str(actual.spark_version)
    security_mode = state_name(actual.data_security_mode)

    reasons: list[str] = []
    if name != configured.expected_name:
        reasons.append(f"name is {name!r}, expected {configured.expected_name!r}")
    if spark_version != configured.expected_spark_version:
        reasons.append(
            f"runtime is {spark_version!r}, expected {configured.expected_spark_version!r}"
        )
    if security_mode != configured.expected_data_security_mode:
        reasons.append(
            f"access mode is {security_mode!r}, "
            f"expected {configured.expected_data_security_mode!r}"
        )
    if not configured.unity_catalog_compatible:
        reasons.append("configuration does not mark the cluster Unity Catalog compatible")
    if not configured.kafka_compatible:
        reasons.append("configuration does not mark the cluster Kafka compatible")
    if "CAN_ATTACH_TO" not in permission_levels:
        reasons.append("identity does not have CAN_ATTACH_TO")

    compatible = not reasons
    ready = compatible and state == required_state.upper()
    if compatible and not ready:
        reasons.append(f"state is {state}, required {required_state.upper()}")
    return ExistingClusterAssessment(
        alias=configured.alias,
        cluster_id=configured.cluster_id,
        name=name,
        state=state,
        spark_version=spark_version,
        data_security_mode=security_mode,
        permission_levels=permission_levels,
        compatible=compatible,
        ready=ready,
        reasons=tuple(reasons),
    )


def select_ready_existing_cluster(
    client: Any, compute: ComputeSettings
) -> tuple[ExistingClusterAssessment | None, tuple[ExistingClusterAssessment, ...]]:
    """Prefer GP1, fall back to GP2, and return none unless one is already running."""
    assessments = tuple(
        assess_existing_cluster(
            client,
            compute.clusters[alias],
            required_state=compute.required_state,
        )
        for alias in (compute.preferred, compute.fallback)
    )
    selected = next((assessment for assessment in assessments if assessment.ready), None)
    return selected, assessments


def require_running_cluster(client: Any, cluster_id: str) -> str:
    """Read-only preflight: refuse to proceed unless the cluster is already RUNNING.

    A Databricks job task pointed at a terminated all-purpose cluster makes Databricks
    START that cluster, so a dispatcher must call this before submitting any run against
    shared academy compute. This function only reads (clusters.get); it never starts,
    restarts, edits, resizes, or deletes a cluster, and installs no libraries.
    """
    state = state_name(client.clusters.get(cluster_id=cluster_id).state)
    if state != "RUNNING":
        raise WorkspaceSafetyError(
            f"Cluster {cluster_id} is {state}, not RUNNING. UrbanFlow never starts a shared "
            "academy cluster: wait until its owner starts it, then run this again."
        )
    return state


def upload_source_notebook(
    client: Any,
    local_path: str | Path,
    workspace_path: str,
    *,
    expected_user_name: str,
) -> None:
    """Upload one source notebook, but only inside the expected user's own Workspace folder.

    The academy workspace is shared and ``overwrite=True`` cannot be undone, so a mistyped
    destination would silently replace another student's notebook.
    """
    if not expected_user_name:
        raise WorkspaceSafetyError("An expected workspace user name is required before upload.")
    home_folder = f"/Workspace/Users/{expected_user_name}"
    if not workspace_path.startswith(f"{home_folder}/") or ".." in workspace_path.split("/"):
        raise WorkspaceSafetyError(
            f"Refusing to upload to {workspace_path!r}: the destination must be a path "
            f"inside {home_folder}/."
        )
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
    compute: ComputeSettings | None = None,
    timeout_seconds: float = 900,
    poll_interval_seconds: float = 10,
) -> PollResult:
    """Request deletion and accept success only after an exact TERMINATED state."""
    if cluster_id in resolve_protected_cluster_ids(compute):
        raise WorkspaceSafetyError(f"Refusing to terminate protected shared cluster {cluster_id}.")
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
    enabled: bool = False,
) -> ClusterExecution:
    """Optional Lab 9 example; disabled unless a caller makes an explicit code-level choice."""
    if not enabled:
        raise WorkspaceSafetyError(
            "Educational cluster creation is disabled. UrbanFlow uses GP1 or GP2 only."
        )
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

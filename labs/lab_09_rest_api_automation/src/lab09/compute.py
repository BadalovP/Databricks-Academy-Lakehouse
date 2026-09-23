"""Temporary compute provisioning for LAB 09.

Cluster-create is deliberately kept separate from pipeline compute (see
pipelines.py): a Lakeflow pipeline update runs on its own pipeline-managed
compute (serverless by default, with a classic fallback), while this module
provisions a short-lived, single-node, autoterminating cluster used only for
the persistent reconciliation Job (Phase 11 steps 11-17).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute as compute_svc

logger = logging.getLogger(__name__)


@dataclass
class ClusterSpec:
    spark_version: str
    node_type_id: str
    autotermination_minutes: int
    num_workers: int = 0
    policy_id: str | None = None
    single_node: bool = True
    custom_tags: dict[str, str] = field(default_factory=dict)
    data_security_mode: str = "SINGLE_USER"

    def as_create_kwargs(self, cluster_name: str) -> dict[str, Any]:
        tags = {"lab": "lab09", **self.custom_tags}
        kwargs: dict[str, Any] = {
            "cluster_name": cluster_name,
            "spark_version": self.spark_version,
            "node_type_id": self.node_type_id,
            "autotermination_minutes": self.autotermination_minutes,
            "num_workers": self.num_workers,
            "data_security_mode": compute_svc.DataSecurityMode(self.data_security_mode),
            "custom_tags": tags,
        }
        if self.single_node:
            kwargs["spark_conf"] = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
            tags["ResourceClass"] = "SingleNode"
        if self.policy_id:
            kwargs["policy_id"] = self.policy_id
        return kwargs

    def as_new_cluster_dict(self, cluster_name: str) -> dict[str, Any]:
        """Shape suitable for jobs.submit(new_cluster=...) / job task new_cluster.

        Used both for the persistent job's fallback task definition and for
        documenting the "explicit cluster creation forbidden" fallback path.
        """
        payload = self.as_create_kwargs(cluster_name)
        payload["data_security_mode"] = self.data_security_mode
        return payload


def resolve_lts_spark_version(client: WorkspaceClient) -> str:
    """Pick a current, non-deprecated LTS runtime rather than hardcoding one."""
    versions = client.clusters.spark_versions().versions or []
    candidates = [
        v
        for v in versions
        if "LTS" in (v.name or "") and "ML" not in (v.name or "") and "GPU" not in (v.name or "")
    ]
    if not candidates:
        candidates = list(versions)
    if not candidates:
        raise RuntimeError("Workspace reported no available Spark runtimes.")

    def sort_key(v: Any) -> tuple[int, ...]:
        head = (v.key or "").split("-")[0]
        parts = []
        for piece in head.split("."):
            try:
                parts.append(int(piece))
            except ValueError:
                parts.append(0)
        return tuple(parts)

    best = sorted(candidates, key=sort_key, reverse=True)[0]
    return best.key


def resolve_node_type(client: WorkspaceClient, node_type_hint: str | None = None) -> str:
    """Pick the smallest available, non-deprecated node type unless hinted."""
    node_types = client.clusters.list_node_types().node_types or []
    if node_type_hint:
        for nt in node_types:
            if nt.node_type_id == node_type_hint:
                return nt.node_type_id
        logger.warning(
            "Configured node_type_hint %s not found in this workspace; picking dynamically.",
            node_type_hint,
        )

    eligible = [nt for nt in node_types if not getattr(nt, "is_deprecated", False)]
    if not eligible:
        eligible = list(node_types)
    if not eligible:
        raise RuntimeError("Workspace reported no available node types.")
    eligible.sort(key=lambda nt: (nt.num_cores or 0, nt.memory_mb or 0))
    return eligible[0].node_type_id


def resolve_policy_id(client: WorkspaceClient, policy_name: str | None) -> str | None:
    """Look up a cluster policy id by name. Returns None if none is configured."""
    if not policy_name:
        return None
    for policy in client.cluster_policies.list():
        if policy.name == policy_name:
            return policy.policy_id
    logger.warning(
        "Configured cluster_policy_name %s not found among available policies; "
        "proceeding without a policy.",
        policy_name,
    )
    return None


def build_cluster_spec(client: WorkspaceClient, cfg: dict[str, Any]) -> ClusterSpec:
    compute_cfg = cfg.get("compute", {})
    spark_version = resolve_lts_spark_version(client)
    node_type_id = resolve_node_type(client, compute_cfg.get("node_type_hint"))
    policy_id = resolve_policy_id(client, compute_cfg.get("cluster_policy_name"))
    single_node = bool(compute_cfg.get("single_node", True))
    return ClusterSpec(
        spark_version=spark_version,
        node_type_id=node_type_id,
        autotermination_minutes=int(compute_cfg.get("autotermination_minutes", 20)),
        num_workers=0 if single_node else 1,
        policy_id=policy_id,
        single_node=single_node,
        custom_tags={"purpose": "lab09-temporary"},
    )


def start_cluster_create(client: WorkspaceClient, spec: ClusterSpec, cluster_name: str) -> str:
    """Start cluster creation and return immediately with the cluster_id.

    Deliberately does NOT call .result() -- monitoring.poll_cluster_state()
    performs the explicit, loggable polling LAB 09 requires instead.
    """
    kwargs = spec.as_create_kwargs(cluster_name)
    waiter = client.clusters.create(**kwargs)
    return waiter.cluster_id


def terminate_cluster(client: WorkspaceClient, cluster_id: str) -> None:
    client.clusters.delete(cluster_id=cluster_id)


def cluster_exists_and_active(client: WorkspaceClient, cluster_id: str) -> bool:
    """Best-effort check used by the outer safety net before a final terminate call."""
    try:
        details = client.clusters.get(cluster_id=cluster_id)
    except Exception:  # noqa: BLE001 - a lookup failure means nothing to clean up
        return False
    state = getattr(details.state, "value", details.state)
    return state not in ("TERMINATED", "ERROR", "UNKNOWN", None)

"""Lakeflow pipeline find-or-create/update and update triggering for LAB 09.

Pipeline compute is provisioned and managed by the pipeline itself (either
serverless, or the classic fallback cluster definition below) -- it is a
separate concern from compute.py's temporary notebook-job cluster. Never
assume they are the same compute.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import pipelines as pipelines_svc

from . import compute

logger = logging.getLogger(__name__)


def find_pipeline_by_name(client: WorkspaceClient, name: str):
    """Return the existing Lab 9 pipeline with this exact name, or None."""
    for p in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p
    return None


def _library_specs(pipeline_source_dir: str) -> list[pipelines_svc.PipelineLibrary]:
    return [
        pipelines_svc.PipelineLibrary(
            file=pipelines_svc.FileLibrary(path=f"{pipeline_source_dir}/bronze.py")
        ),
        pipelines_svc.PipelineLibrary(
            file=pipelines_svc.FileLibrary(path=f"{pipeline_source_dir}/silver.py")
        ),
        pipelines_svc.PipelineLibrary(
            file=pipelines_svc.FileLibrary(path=f"{pipeline_source_dir}/gold.py")
        ),
    ]


def _classic_cluster_spec(client: WorkspaceClient, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a minimal classic pipeline cluster definition for the serverless-rejected fallback.

    Pipeline clusters are configured differently from job/notebook clusters
    (no autotermination_minutes -- Lakeflow manages pipeline cluster
    lifecycle itself), so this intentionally does not reuse compute.py's
    ClusterSpec.as_create_kwargs() shape.
    """
    pipeline_cfg = cfg.get("pipeline", {})
    node_type_id = compute.resolve_node_type(client, pipeline_cfg.get("classic_node_type_hint"))
    num_workers = int(pipeline_cfg.get("classic_num_workers", 1))
    return [
        {
            "label": "default",
            "node_type_id": node_type_id,
            "num_workers": num_workers,
            "custom_tags": {"lab": "lab09", "purpose": "lab09-pipeline-classic-fallback"},
        }
    ]


def ensure_pipeline(
    client: WorkspaceClient, cfg: dict[str, Any], pipeline_source_dir: str
) -> tuple[str, bool]:
    """Find the Lab 9 pipeline by name; create it only if missing.

    Returns (pipeline_id, used_serverless). Tries serverless first (per
    config); if the workspace rejects serverless pipeline creation, falls
    back to a classic cluster definition and documents which path was used
    in the returned tuple / log output.
    """
    pipeline_cfg = cfg["pipeline"]
    name = pipeline_cfg["name"]
    libraries = _library_specs(pipeline_source_dir)
    catalog = cfg["catalog"]
    target_schema = pipeline_cfg["target_schema"]

    existing = find_pipeline_by_name(client, name)
    if existing is not None:
        logger.info(
            "Pipeline %s already exists (id=%s); updating configuration.",
            name,
            existing.pipeline_id,
        )
        client.pipelines.update(
            pipeline_id=existing.pipeline_id,
            name=name,
            catalog=catalog,
            target=target_schema,
            libraries=libraries,
            serverless=bool(pipeline_cfg.get("prefer_serverless", True)),
            continuous=False,
        )
        used_serverless = bool(pipeline_cfg.get("prefer_serverless", True))
        return existing.pipeline_id, used_serverless

    prefer_serverless = bool(pipeline_cfg.get("prefer_serverless", True))
    if prefer_serverless:
        try:
            created = client.pipelines.create(
                name=name,
                catalog=catalog,
                target=target_schema,
                libraries=libraries,
                serverless=True,
                continuous=False,
            )
            logger.info("Created serverless pipeline %s (id=%s).", name, created.pipeline_id)
            return created.pipeline_id, True
        except DatabricksError as exc:
            logger.warning(
                "Serverless pipeline creation was rejected (%s); falling back to classic "
                "pipeline compute.",
                exc,
            )

    clusters = _classic_cluster_spec(client, cfg)
    created = client.pipelines.create(
        name=name,
        catalog=catalog,
        target=target_schema,
        libraries=libraries,
        serverless=False,
        clusters=clusters,
        continuous=False,
    )
    logger.info("Created classic-compute pipeline %s (id=%s).", name, created.pipeline_id)
    return created.pipeline_id, False


def start_update(client: WorkspaceClient, pipeline_id: str, full_refresh: bool = False) -> str:
    """Start a pipeline update and return its update_id immediately (no waiting)."""
    response = client.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
    return response.update_id

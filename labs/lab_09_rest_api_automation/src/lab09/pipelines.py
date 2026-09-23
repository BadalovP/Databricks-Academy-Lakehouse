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
from databricks.sdk.errors import InvalidParameterValue, PermissionDenied
from databricks.sdk.service import pipelines as pipelines_svc

from . import compute

logger = logging.getLogger(__name__)

# Same narrow rejection signal compute.py uses for its cluster-create
# fallback: a genuine "this configuration/permission is not allowed"
# response. Catching bare DatabricksError here would also mask unrelated
# bugs (a typo'd catalog name, a bad library path) as a false "serverless
# unsupported" fallback, silently creating/updating a classic pipeline
# instead of surfacing the real problem.
SERVERLESS_REJECTION_ERRORS = (PermissionDenied, InvalidParameterValue)


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


def _classic_clusters(
    client: WorkspaceClient, cfg: dict[str, Any]
) -> list[pipelines_svc.PipelineCluster]:
    """Typed classic pipeline cluster definition for the serverless-rejected fallback.

    Pipeline clusters are configured differently from job/notebook clusters
    (no autotermination_minutes -- Lakeflow manages pipeline cluster
    lifecycle itself), so this intentionally does not reuse compute.py's
    ClusterSpec.
    """
    pipeline_cfg = cfg.get("pipeline", {})
    node_type_id = compute.resolve_node_type(client, pipeline_cfg.get("classic_node_type_hint"))
    num_workers = int(pipeline_cfg.get("classic_num_workers", 1))
    return [
        pipelines_svc.PipelineCluster(
            label="default",
            node_type_id=node_type_id,
            num_workers=num_workers,
            custom_tags={"lab": "lab09", "purpose": "lab09-pipeline-classic-fallback"},
        )
    ]


def _create_pipeline(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    name: str,
    catalog: str,
    target_schema: str,
    libraries: list[pipelines_svc.PipelineLibrary],
) -> tuple[str, bool]:
    """Create a new pipeline. Tries serverless first (per config); falls back to
    classic compute only when serverless is genuinely rejected. Returns
    (pipeline_id, used_serverless) reflecting what actually happened.
    """
    prefer_serverless = bool(cfg["pipeline"].get("prefer_serverless", True))

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
        except SERVERLESS_REJECTION_ERRORS as exc:
            logger.warning(
                "Serverless pipeline creation was rejected (%s: %s); falling back to "
                "classic pipeline compute.",
                type(exc).__name__,
                exc,
            )

    clusters = _classic_clusters(client, cfg)
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


def _update_pipeline(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    pipeline_id: str,
    name: str,
    catalog: str,
    target_schema: str,
    libraries: list[pipelines_svc.PipelineLibrary],
) -> bool:
    """Update an existing pipeline in place, with the same serverless->classic
    fallback creation uses. Returns used_serverless reflecting what actually
    happened -- never assumed from configuration preference alone.
    """
    prefer_serverless = bool(cfg["pipeline"].get("prefer_serverless", True))

    if prefer_serverless:
        try:
            client.pipelines.update(
                pipeline_id=pipeline_id,
                name=name,
                catalog=catalog,
                target=target_schema,
                libraries=libraries,
                serverless=True,
                continuous=False,
            )
            logger.info("Updated pipeline %s (id=%s) to serverless compute.", name, pipeline_id)
            return True
        except SERVERLESS_REJECTION_ERRORS as exc:
            logger.warning(
                "Serverless pipeline update was rejected (%s: %s); falling back to "
                "classic pipeline compute.",
                type(exc).__name__,
                exc,
            )

    clusters = _classic_clusters(client, cfg)
    client.pipelines.update(
        pipeline_id=pipeline_id,
        name=name,
        catalog=catalog,
        target=target_schema,
        libraries=libraries,
        serverless=False,
        clusters=clusters,
        continuous=False,
    )
    logger.info("Updated pipeline %s (id=%s) to classic-compute.", name, pipeline_id)
    return False


def ensure_pipeline(
    client: WorkspaceClient, cfg: dict[str, Any], pipeline_source_dir: str
) -> tuple[str, bool]:
    """Find the Lab 9 pipeline by name; create it only if missing, else update it in place.

    Returns (pipeline_id, used_serverless) reflecting what the create/update
    call that actually succeeded did -- never just the configured
    preference -- for both the creation and the update code path.
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
        used_serverless = _update_pipeline(
            client, cfg, existing.pipeline_id, name, catalog, target_schema, libraries
        )
        return existing.pipeline_id, used_serverless

    return _create_pipeline(client, cfg, name, catalog, target_schema, libraries)


def start_update(client: WorkspaceClient, pipeline_id: str, full_refresh: bool = False) -> str:
    """Start a pipeline update and return its update_id immediately (no waiting)."""
    response = client.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
    return response.update_id

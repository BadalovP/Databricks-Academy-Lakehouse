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

# Confirmed live (2026-09-24, personal-yahoo profile): updating an EXISTING
# pipeline's target schema raises InvalidParameterValue with a message that
# has NOTHING to do with serverless-vs-classic compute support --
# "Changing target schema is not allowed. Reason: DLT does not yet support
# changing target schema of a pipeline that uses an Default Storage
# catalog. Please create a new pipeline if you need to change the target
# schema." Blindly treating every InvalidParameterValue as "serverless was
# rejected, retry as classic" made the code retry that exact same update
# call as classic compute, which failed with the identical error (a
# platform restriction on changing target schema at all, independent of
# compute type) -- a wasted, pointless second API call that also delayed
# surfacing the real, actionable error.
#
# A denylist that excludes only that one observed message would still be
# wrong: the Pipelines/Jobs API can raise InvalidParameterValue for many
# other completely unrelated reasons (a bad catalog name, an invalid
# library path, a malformed cluster policy reference, ...), and none of
# those should be silently reinterpreted as "try classic compute instead"
# either. This is therefore a POSITIVE allow-list: an InvalidParameterValue
# is only ever treated as a genuine serverless-capability rejection if its
# message unambiguously says serverless itself is unavailable/unsupported.
# Every other InvalidParameterValue -- the target-schema-change rejection
# included, but not limited to it -- propagates immediately instead of
# triggering a second, likely-doomed API call.
_SERVERLESS_REJECTION_MESSAGE_PATTERNS = (
    "serverless is not enabled",
    "serverless is not supported",
    "serverless is not available",
    "serverless pipelines are not enabled",
    "serverless compute is not enabled",
    "serverless compute is not supported",
    "does not support serverless",
)


def _is_serverless_capability_rejection(exc: Exception) -> bool:
    """True only for an error that unambiguously says serverless compute
    itself is unavailable/unsupported -- see module-level comment above for
    why this is a positive allow-list, not a denylist.
    """
    if isinstance(exc, PermissionDenied):
        return True
    if isinstance(exc, InvalidParameterValue):
        message = str(exc).lower()
        return any(pattern in message for pattern in _SERVERLESS_REJECTION_MESSAGE_PATTERNS)
    return False


def find_pipeline_by_name(client: WorkspaceClient, name: str):
    """Return the existing Lab 9 pipeline with this exact name, or None."""
    for p in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p
    return None


def _library_specs(pipeline_source_dir: str) -> list[pipelines_svc.PipelineLibrary]:
    """One typed glob-include library covering the whole pipeline source directory.

    The installed SDK's `PipelineLibrary` supports a `glob: PathPattern`
    field (`PathPattern(include=...)`), the modern equivalent of listing
    bronze.py/silver.py/gold.py individually via `FileLibrary`. All three
    files are still uploaded as genuine workspace FILE objects by
    workspace.py -- this glob is what registers that whole directory as
    pipeline source with the pipeline, so a future fourth pipeline source
    file dropped into the same directory would not require touching this
    code, matching how Lab 8's DAB `root_path` config already works.
    """
    return [
        pipelines_svc.PipelineLibrary(
            glob=pipelines_svc.PathPattern(include=f"{pipeline_source_dir}/**")
        )
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
            if not _is_serverless_capability_rejection(exc):
                raise
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
            if not _is_serverless_capability_rejection(exc):
                raise
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

    Note: this does NOT (and, per Databricks' own platform restriction,
    cannot) migrate an existing pipeline to a different target schema --
    see _is_serverless_capability_rejection()'s docstring. cfg["pipeline"]["name"]
    must be a name that either doesn't exist yet, or already targets
    cfg["pipeline"]["target_schema"]; config/dev.yml's comments explain why
    this project renamed to "lab09_taxi_pipeline_v2" rather than reusing the
    original "lab09_taxi_pipeline" (pipeline_id 9fcf88d2-...), which is left
    untouched.
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

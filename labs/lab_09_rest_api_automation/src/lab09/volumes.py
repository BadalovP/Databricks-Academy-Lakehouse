"""Unity Catalog volume + pipeline-output-schema creation/discovery for LAB 09."""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

logger = logging.getLogger(__name__)


def ensure_volume(client: WorkspaceClient, cfg: dict[str, Any]):
    """Find the configured Lab 9 managed volume, creating it only if missing.

    Only a genuine NotFound/ResourceDoesNotExist (the SDK's typed
    exception, not a string match against error_code -- a real volume's
    "not found" error_code is RESOURCE_DOES_NOT_EXIST, which a naive
    "NOT_FOUND" substring check never matches) is treated as "missing";
    any other error (permission, auth, service) propagates.
    """
    full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
    try:
        existing = client.volumes.read(full_name)
        logger.info("Volume %s already exists.", full_name)
        return existing
    except NotFound:
        pass

    logger.info("Volume %s not found; creating it.", full_name)
    return client.volumes.create(
        catalog_name=cfg["catalog"],
        schema_name=cfg["schema"],
        name=cfg["volume"],
        volume_type=VolumeType.MANAGED,
        comment="LAB 09 REST API automation landing volume (trips/ and reference/).",
    )


def ensure_output_schema(client: WorkspaceClient, cfg: dict[str, Any]):
    """Find the Lab 9 pipeline OUTPUT schema (cfg["pipeline"]["target_schema"]),
    creating it only if missing.

    Deliberately separate from cfg["schema"] (the landing/input schema that
    holds the Files API volume, via ensure_volume() above): confirmed live
    that dbr_dev.parvinbadalov -- the landing schema, shared with every other
    lab/demo in this Personal workspace -- has hit Unity Catalog's
    per-schema table-count quota (QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED,
    ~100+ existing tables against a limit of 100), which is why Lab 9's
    pipeline now writes bronze/silver/quarantine/gold into their own schema
    instead. See config/dev.yml's pipeline.target_schema comment and
    README.md "Known limitations" for the full evidence.

    Same NotFound-only-is-missing discipline as ensure_volume(): any other
    error (permission, auth, service) propagates instead of being treated
    as "schema is missing".
    """
    catalog = cfg["catalog"]
    schema_name = cfg["pipeline"]["target_schema"]
    full_name = f"{catalog}.{schema_name}"
    try:
        existing = client.schemas.get(full_name)
        logger.info("Output schema %s already exists.", full_name)
        return existing
    except NotFound:
        pass

    logger.info("Output schema %s not found; creating it.", full_name)
    return client.schemas.create(
        name=schema_name,
        catalog_name=catalog,
        comment=(
            "LAB 09 REST API automation pipeline OUTPUT schema "
            "(bronze/silver/quarantine/gold tables) -- separate from the "
            "landing schema to avoid dbr_dev.parvinbadalov's UC table-count quota."
        ),
    )

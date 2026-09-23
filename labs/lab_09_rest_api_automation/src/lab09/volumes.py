"""Unity Catalog volume creation / discovery for LAB 09."""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import VolumeType

logger = logging.getLogger(__name__)


def ensure_volume(client: WorkspaceClient, cfg: dict[str, Any]):
    """Find the configured Lab 9 managed volume, creating it only if missing."""
    full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
    try:
        existing = client.volumes.read(full_name)
        logger.info("Volume %s already exists.", full_name)
        return existing
    except DatabricksError as exc:
        if "NOT_FOUND" not in str(getattr(exc, "error_code", "")) and "NOT_FOUND" not in str(exc):
            raise

    logger.info("Volume %s not found; creating it.", full_name)
    return client.volumes.create(
        catalog_name=cfg["catalog"],
        schema_name=cfg["schema"],
        name=cfg["volume"],
        volume_type=VolumeType.MANAGED,
        comment="LAB 09 REST API automation landing volume (trips/ and reference/).",
    )

"""Workspace file upload helpers for LAB 09 (pipeline sources + notebook).

Pipeline compute and notebook-job compute are separate concerns (see
compute.py and pipelines.py); this module only handles getting the Python
source files into the workspace filesystem so both can reference them.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

logger = logging.getLogger(__name__)


def resolve_current_user_home(client: WorkspaceClient) -> str:
    user_name = client.current_user.me().user_name
    return f"/Workspace/Users/{user_name}"


def lab09_root_path(client: WorkspaceClient, cfg: dict[str, Any]) -> str:
    home = resolve_current_user_home(client)
    subpath = cfg.get("workspace", {}).get("root_subpath", "lab09")
    return f"{home}/{subpath}"


def pipeline_source_dir(client: WorkspaceClient, cfg: dict[str, Any]) -> str:
    root = lab09_root_path(client, cfg)
    subpath = cfg.get("workspace", {}).get("pipeline_subpath", "pipeline")
    return f"{root}/{subpath}"


def notebook_dir(client: WorkspaceClient, cfg: dict[str, Any]) -> str:
    root = lab09_root_path(client, cfg)
    subpath = cfg.get("workspace", {}).get("notebook_subpath", "notebooks")
    return f"{root}/{subpath}"


def _upload_python_file(client: WorkspaceClient, local_path: Path, workspace_path: str) -> None:
    content = local_path.read_bytes()
    client.workspace.mkdirs(str(Path(workspace_path).parent).replace("\\", "/"))
    client.workspace.upload(
        workspace_path,
        content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info("Uploaded %s -> %s", local_path, workspace_path)


def upload_pipeline_sources(
    client: WorkspaceClient, local_pipeline_dir: Path, cfg: dict[str, Any]
) -> list[str]:
    """Upload bronze.py, silver.py, gold.py to the workspace. Never skip any of them."""
    target_dir = pipeline_source_dir(client, cfg)
    client.workspace.mkdirs(target_dir)
    uploaded = []
    for filename in ("bronze.py", "silver.py", "gold.py"):
        local_path = local_pipeline_dir / filename
        if not local_path.exists():
            raise FileNotFoundError(f"Expected pipeline source file not found: {local_path}")
        workspace_path = f"{target_dir}/{filename}"
        _upload_python_file(client, local_path, workspace_path)
        uploaded.append(workspace_path)
    return uploaded


def upload_notebook(client: WorkspaceClient, local_notebook_path: Path, cfg: dict[str, Any]) -> str:
    target_dir = notebook_dir(client, cfg)
    client.workspace.mkdirs(target_dir)
    workspace_path = f"{target_dir}/{local_notebook_path.name}"
    _upload_python_file(client, local_notebook_path, workspace_path)
    return workspace_path

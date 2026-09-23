"""Workspace file upload helpers for LAB 09 (pipeline sources + notebook).

Pipeline compute and notebook-job compute are separate concerns (see
compute.py and pipelines.py); this module only handles getting the Python
source files into the workspace filesystem so both can reference them.

Two genuinely different Workspace object types are involved here, and they
require different `import_()` parameters (verified against this repo's
installed `databricks-sdk` -- there is no `WorkspaceClient.workspace.upload`
method; the real method is `import_`, and its `content` parameter must be
base64-encoded text, not raw bytes):

- notebooks/01_reconcile_counts.py has a `# Databricks notebook source`
  header and is imported with `format=SOURCE, language=PYTHON`, which
  produces an `ObjectType.NOTEBOOK` -- required for `NotebookTask`.
- pipeline/bronze.py, silver.py, gold.py have NO notebook header and must
  become plain `ObjectType.FILE` objects, not notebooks, to match what
  Lakeflow's `PipelineLibrary(file=...)` / glob `PathPattern` expect. Per
  the SDK's own `import_()` docstring, the `language` field "is set only
  if the object type is NOTEBOOK", and Databricks' own docs state that
  importing a single file as `SOURCE` requires (and therefore produces) a
  notebook -- so `format=RAW` with no `language` set is used instead,
  which imports the bytes as-is without notebook-header inference.

This RAW-vs-SOURCE distinction has been verified against the SDK's method
signature and public documentation, but NOT against a live workspace --
see README.md "Known limitations" for what live Lakeflow verification
still needs to confirm (that a pipeline glob/file library actually
resolves these RAW-imported files as valid pipeline source).
"""

from __future__ import annotations

import base64
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


def _encode(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")


def _mkparent(client: WorkspaceClient, workspace_path: str) -> None:
    parent = str(Path(workspace_path).parent).replace("\\", "/")
    client.workspace.mkdirs(parent)


def _upload_workspace_file(client: WorkspaceClient, local_path: Path, workspace_path: str) -> None:
    """Upload a plain, non-notebook Python module as a genuine ObjectType.FILE."""
    _mkparent(client, workspace_path)
    client.workspace.import_(
        workspace_path,
        content=_encode(local_path.read_bytes()),
        format=ImportFormat.RAW,
        overwrite=True,
    )
    logger.info("Uploaded workspace FILE %s -> %s", local_path, workspace_path)


def _upload_notebook_source(client: WorkspaceClient, local_path: Path, workspace_path: str) -> None:
    """Upload a Databricks SOURCE-format notebook (used by NotebookTask)."""
    _mkparent(client, workspace_path)
    client.workspace.import_(
        workspace_path,
        content=_encode(local_path.read_bytes()),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info("Uploaded notebook %s -> %s", local_path, workspace_path)


def upload_pipeline_sources(
    client: WorkspaceClient, local_pipeline_dir: Path, cfg: dict[str, Any]
) -> list[str]:
    """Upload bronze.py, silver.py, gold.py as workspace FILEs. Never skip any of them."""
    target_dir = pipeline_source_dir(client, cfg)
    client.workspace.mkdirs(target_dir)
    uploaded = []
    for filename in ("bronze.py", "silver.py", "gold.py"):
        local_path = local_pipeline_dir / filename
        if not local_path.exists():
            raise FileNotFoundError(f"Expected pipeline source file not found: {local_path}")
        workspace_path = f"{target_dir}/{filename}"
        _upload_workspace_file(client, local_path, workspace_path)
        uploaded.append(workspace_path)
    return uploaded


def upload_notebook(client: WorkspaceClient, local_notebook_path: Path, cfg: dict[str, Any]) -> str:
    target_dir = notebook_dir(client, cfg)
    client.workspace.mkdirs(target_dir)
    workspace_path = f"{target_dir}/{local_notebook_path.name}"
    _upload_notebook_source(client, local_notebook_path, workspace_path)
    return workspace_path

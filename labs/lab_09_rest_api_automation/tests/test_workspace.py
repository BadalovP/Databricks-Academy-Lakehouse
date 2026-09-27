"""Tests proving pipeline sources upload as workspace FILEs, not notebooks.

The defect this guards against: workspace.py previously called a
non-existent `client.workspace.upload(...)` method and used
`format=SOURCE, language=PYTHON` for both the reconciliation notebook AND
the plain bronze.py/silver.py/gold.py pipeline source files -- which would
have made all four into ObjectType.NOTEBOOK, not the ObjectType.FILE a
Lakeflow pipeline's file/glob library expects for the latter three.
"""

import base64
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from lab09 import workspace

LAB_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = LAB_ROOT / "pipeline"
NOTEBOOK_PATH = LAB_ROOT / "notebooks" / "01_reconcile_counts.py"


def _autospec_client() -> MagicMock:
    client = create_autospec(WorkspaceClient, instance=True)
    client.current_user.me.return_value.user_name = "someone@example.com"
    return client


def _cfg() -> dict:
    return {
        "workspace": {
            "root_subpath": "lab09",
            "pipeline_subpath": "pipeline",
            "notebook_subpath": "notebooks",
        }
    }


# --- pipeline sources: genuine workspace FILE semantics ---------------------


def test_upload_pipeline_sources_uses_raw_file_format_never_language():
    client = _autospec_client()
    workspace.upload_pipeline_sources(client, PIPELINE_DIR, _cfg())

    assert client.workspace.import_.call_count == 3
    for call in client.workspace.import_.call_args_list:
        assert call.kwargs["format"] == ImportFormat.RAW
        # `language` must never be set for these -- setting it is what
        # turns an imported object into a NOTEBOOK instead of a FILE.
        assert "language" not in call.kwargs


def test_upload_pipeline_sources_registers_all_three_files_at_expected_paths():
    client = _autospec_client()
    uploaded = workspace.upload_pipeline_sources(client, PIPELINE_DIR, _cfg())

    expected = [
        "/Workspace/Users/someone@example.com/lab09/pipeline/bronze.py",
        "/Workspace/Users/someone@example.com/lab09/pipeline/silver.py",
        "/Workspace/Users/someone@example.com/lab09/pipeline/gold.py",
    ]
    assert uploaded == expected
    called_paths = [call.args[0] for call in client.workspace.import_.call_args_list]
    assert called_paths == expected


# --- lab09_root_path: run_as override ---------------------------------------


def test_lab09_root_path_uses_deploying_identity_by_default():
    client = _autospec_client()
    assert workspace.lab09_root_path(client, _cfg()) == "/Workspace/Users/someone@example.com/lab09"


def test_lab09_root_path_uses_run_as_identity_when_configured():
    """Regression test for a real defect found live (2026-09-27): content
    uploaded under the deploying identity's home directory was not
    importable by a job task running under a DIFFERENT run_as identity
    (ModuleNotFoundError), even though that run_as identity is a workspace
    admin with full API-level read access. When cfg declares
    run_as_user_name, the root must resolve under THAT identity's home
    directory instead of whichever identity is currently deploying.
    """
    client = _autospec_client()
    cfg = _cfg()
    cfg["run_as_user_name"] = "parvinbadalov@softserve.academy"

    root = workspace.lab09_root_path(client, cfg)

    assert root == "/Workspace/Users/parvinbadalov@softserve.academy/lab09"
    # Must not even need to resolve the deploying identity in this case.
    client.current_user.me.assert_not_called()


def test_upload_pipeline_sources_base64_encodes_content_faithfully():
    client = _autospec_client()
    workspace.upload_pipeline_sources(client, PIPELINE_DIR, _cfg())

    bronze_call = next(
        c for c in client.workspace.import_.call_args_list if c.args[0].endswith("bronze.py")
    )
    decoded = base64.b64decode(bronze_call.kwargs["content"])
    assert decoded == (PIPELINE_DIR / "bronze.py").read_bytes()


def test_upload_pipeline_sources_raises_if_a_file_is_missing(tmp_path):
    client = _autospec_client()
    (tmp_path / "bronze.py").write_text("# bronze")
    (tmp_path / "silver.py").write_text("# silver")
    # gold.py deliberately absent.

    with pytest.raises(FileNotFoundError):
        workspace.upload_pipeline_sources(client, tmp_path, _cfg())


# --- reconciliation notebook: genuine SOURCE notebook semantics -------------


def test_upload_notebook_uses_source_format_and_python_language():
    client = _autospec_client()
    notebook_path = workspace.upload_notebook(client, NOTEBOOK_PATH, _cfg())

    assert (
        notebook_path
        == "/Workspace/Users/someone@example.com/lab09/notebooks/01_reconcile_counts.py"
    )
    client.workspace.import_.assert_called_once()
    _, kwargs = client.workspace.import_.call_args
    assert kwargs["format"] == ImportFormat.SOURCE
    assert kwargs["language"] == Language.PYTHON


def test_upload_notebook_content_round_trips_through_base64():
    client = _autospec_client()
    workspace.upload_notebook(client, NOTEBOOK_PATH, _cfg())

    _, kwargs = client.workspace.import_.call_args
    decoded = base64.b64decode(kwargs["content"])
    assert decoded == NOTEBOOK_PATH.read_bytes()

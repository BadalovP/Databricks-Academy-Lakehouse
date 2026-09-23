from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from lab09 import pipelines


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg(prefer_serverless: bool = True) -> dict:
    return {
        "catalog": "dbr_dev",
        "pipeline": {
            "name": "lab09_taxi_pipeline",
            "target_schema": "parvinbadalov",
            "prefer_serverless": prefer_serverless,
            "classic_node_type_hint": None,
            "classic_num_workers": 1,
        },
    }


def test_find_pipeline_by_name_returns_exact_match():
    client = _autospec_client()
    other = MagicMock()
    other.name = "some_other_pipeline"
    other.pipeline_id = "1"
    match = MagicMock()
    match.name = "lab09_taxi_pipeline"
    match.pipeline_id = "2"
    client.pipelines.list_pipelines.return_value = [other, match]

    found = pipelines.find_pipeline_by_name(client, "lab09_taxi_pipeline")
    assert found.pipeline_id == "2"


def test_find_pipeline_by_name_returns_none_when_missing():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    assert pipelines.find_pipeline_by_name(client, "lab09_taxi_pipeline") is None


def test_ensure_pipeline_reuses_existing_pipeline_without_creating_duplicate():
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "existing-id"
    assert used_serverless is True
    client.pipelines.create.assert_not_called()
    client.pipelines.update.assert_called_once()
    _, kwargs = client.pipelines.update.call_args
    assert kwargs["name"] == "lab09_taxi_pipeline"


def test_ensure_pipeline_creates_serverless_when_missing_and_preferred():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    created = MagicMock()
    created.pipeline_id = "new-id"
    client.pipelines.create.return_value = created

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "new-id"
    assert used_serverless is True
    client.pipelines.create.assert_called_once()
    _, kwargs = client.pipelines.create.call_args
    assert kwargs["serverless"] is True
    assert len(kwargs["libraries"]) == 3


def test_ensure_pipeline_falls_back_to_classic_when_serverless_rejected():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    client.clusters.list_node_types.return_value.node_types = [
        MagicMock(node_type_id="small", num_cores=4, memory_mb=16384, is_deprecated=False)
    ]

    classic_created = MagicMock()
    classic_created.pipeline_id = "classic-id"
    client.pipelines.create.side_effect = [
        DatabricksError("serverless pipelines are not enabled for this workspace"),
        classic_created,
    ]

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "classic-id"
    assert used_serverless is False
    assert client.pipelines.create.call_count == 2
    _, kwargs = client.pipelines.create.call_args
    assert kwargs["serverless"] is False
    assert kwargs["clusters"] is not None


def test_ensure_pipeline_creates_classic_directly_when_not_preferred():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    client.clusters.list_node_types.return_value.node_types = [
        MagicMock(node_type_id="small", num_cores=4, memory_mb=16384, is_deprecated=False)
    ]
    created = MagicMock()
    created.pipeline_id = "classic-id"
    client.pipelines.create.return_value = created

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(prefer_serverless=False), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "classic-id"
    assert used_serverless is False
    assert client.pipelines.create.call_count == 1


def test_start_update_returns_update_id_without_waiting():
    client = _autospec_client()
    response = MagicMock()
    response.update_id = "update-123"
    client.pipelines.start_update.return_value = response

    update_id = pipelines.start_update(client, "pipeline-id")

    assert update_id == "update-123"
    client.pipelines.start_update.assert_called_once_with(
        pipeline_id="pipeline-id", full_refresh=False
    )

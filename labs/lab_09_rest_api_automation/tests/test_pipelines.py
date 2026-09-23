from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, InvalidParameterValue, PermissionDenied

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


def _node_types(client: MagicMock) -> None:
    client.clusters.list_node_types.return_value.node_types = [
        MagicMock(node_type_id="small", num_cores=4, memory_mb=16384, is_deprecated=False)
    ]


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


# --- creation path ------------------------------------------------------


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
    assert len(kwargs["libraries"]) == 1
    assert kwargs["libraries"][0].glob.include == "/Workspace/Users/x/lab09/pipeline/**"


def test_ensure_pipeline_falls_back_to_classic_when_serverless_creation_rejected():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    _node_types(client)

    classic_created = MagicMock()
    classic_created.pipeline_id = "classic-id"
    client.pipelines.create.side_effect = [
        PermissionDenied("serverless pipelines are not enabled for this workspace"),
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
    assert kwargs["clusters"][0].node_type_id == "small"


def test_ensure_pipeline_creation_does_not_swallow_unrelated_errors():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    client.pipelines.create.side_effect = InternalError("service is having a bad day")

    with pytest.raises(InternalError):
        pipelines.ensure_pipeline(client, _cfg(), "/Workspace/Users/x/lab09/pipeline")


def test_ensure_pipeline_creates_classic_directly_when_not_preferred():
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    _node_types(client)
    created = MagicMock()
    created.pipeline_id = "classic-id"
    client.pipelines.create.return_value = created

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(prefer_serverless=False), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "classic-id"
    assert used_serverless is False
    assert client.pipelines.create.call_count == 1


# --- update path (existing pipeline) -------------------------------------


def test_ensure_pipeline_updates_existing_pipeline_to_serverless_when_it_succeeds():
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
    assert kwargs["pipeline_id"] == "existing-id"
    assert kwargs["name"] == "lab09_taxi_pipeline"
    assert kwargs["serverless"] is True


def test_ensure_pipeline_falls_back_to_classic_when_existing_pipeline_serverless_update_rejected():
    """The defect this guards against: an EXISTING pipeline was previously just
    updated with serverless=<preference> with no fallback at all -- claiming
    serverless was used even when the update call for it never succeeded.
    """
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]
    _node_types(client)

    client.pipelines.update.side_effect = [
        InvalidParameterValue("cluster policy forbids serverless pipelines"),
        None,  # the classic-compute update call succeeds
    ]

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "existing-id"
    assert used_serverless is False  # never claim serverless unless it actually was used
    assert client.pipelines.update.call_count == 2
    first_kwargs = client.pipelines.update.call_args_list[0].kwargs
    assert first_kwargs["serverless"] is True
    second_kwargs = client.pipelines.update.call_args_list[1].kwargs
    assert second_kwargs["serverless"] is False
    assert second_kwargs["clusters"] is not None


def test_ensure_pipeline_update_does_not_swallow_unrelated_errors():
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]
    client.pipelines.update.side_effect = InternalError("service is having a bad day")

    with pytest.raises(InternalError):
        pipelines.ensure_pipeline(client, _cfg(), "/Workspace/Users/x/lab09/pipeline")


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

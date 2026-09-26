from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, InvalidParameterValue, PermissionDenied

from lab09 import pipelines


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg(prefer_serverless: bool = True, allow_classic_fallback: bool = True) -> dict:
    """allow_classic_fallback defaults to True here so this shared fixture
    represents an "other workspace" that still wants the classic fallback
    (pipelines.py itself defaults to False -- see config/dev.yml, which sets
    it to False explicitly for the Personal workspace this project targets).
    """
    return {
        "catalog": "dbr_dev",
        "pipeline": {
            "name": "lab09_taxi_pipeline",
            "target_schema": "parvinbadalov",
            "prefer_serverless": prefer_serverless,
            "allow_classic_fallback": allow_classic_fallback,
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


def test_ensure_pipeline_creates_v2_without_touching_v1_when_only_v1_exists():
    """v1 (lab09_taxi_pipeline) existing must never cause ensure_pipeline()
    to update it when v2 (lab09_taxi_pipeline_v2) is what's requested --
    v1 doesn't match v2's name, so it must be created fresh, and v1's
    pipeline_id must never appear in any create/update call.
    """
    client = _autospec_client()
    v1 = MagicMock()
    v1.name = "lab09_taxi_pipeline"
    v1.pipeline_id = "v1-existing-id"
    client.pipelines.list_pipelines.return_value = [v1]
    created = MagicMock()
    created.pipeline_id = "v2-new-id"
    client.pipelines.create.return_value = created

    cfg = _cfg()
    cfg["pipeline"]["name"] = "lab09_taxi_pipeline_v2"
    cfg["pipeline"]["target_schema"] = "lab09"

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, cfg, "/Workspace/Users/someone@example.com/lab09/pipeline"
    )

    assert pipeline_id == "v2-new-id"
    assert used_serverless is True
    client.pipelines.update.assert_not_called()
    client.pipelines.create.assert_called_once()
    _, kwargs = client.pipelines.create.call_args
    assert kwargs["name"] == "lab09_taxi_pipeline_v2"
    assert kwargs["target"] == "lab09"


def test_ensure_pipeline_reuses_v2_without_touching_v1_when_both_exist():
    """Once v2 already exists alongside v1, ensure_pipeline() must find and
    update ONLY v2 by its exact name match -- v1's pipeline_id must never
    be passed to update() (or create()/delete()) at all.
    """
    client = _autospec_client()
    v1 = MagicMock()
    v1.name = "lab09_taxi_pipeline"
    v1.pipeline_id = "v1-existing-id"
    v2 = MagicMock()
    v2.name = "lab09_taxi_pipeline_v2"
    v2.pipeline_id = "v2-existing-id"
    client.pipelines.list_pipelines.return_value = [v1, v2]

    cfg = _cfg()
    cfg["pipeline"]["name"] = "lab09_taxi_pipeline_v2"
    cfg["pipeline"]["target_schema"] = "lab09"

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, cfg, "/Workspace/Users/someone@example.com/lab09/pipeline"
    )

    assert pipeline_id == "v2-existing-id"
    assert used_serverless is True
    client.pipelines.create.assert_not_called()
    client.pipelines.update.assert_called_once()
    _, kwargs = client.pipelines.update.call_args
    assert kwargs["pipeline_id"] == "v2-existing-id"
    assert kwargs["pipeline_id"] != v1.pipeline_id


def test_ensure_pipeline_updates_existing_v2_pipeline_whose_target_is_already_lab09():
    """Reuses the existing v2 persistent pipeline (never creates a
    duplicate) on an ordinary repeat run, where it already targets
    dbr_dev.lab09 from its own creation.

    This deliberately does NOT model retargeting an existing pipeline to a
    *different* schema -- confirmed live (2026-09-24) that Databricks
    rejects that outright for a Default-Storage-catalog DLT pipeline (see
    test_ensure_pipeline_update_target_schema_rejection_propagates_without_classic_fallback
    below), which is exactly why this project uses a separate pipeline,
    lab09_taxi_pipeline_v2, created targeting dbr_dev.lab09 from the start,
    rather than ever retargeting the original lab09_taxi_pipeline (v1).
    """
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline_v2"
    existing.pipeline_id = "v2-pipeline-id"
    client.pipelines.list_pipelines.return_value = [existing]

    cfg = _cfg()
    cfg["pipeline"]["name"] = "lab09_taxi_pipeline_v2"
    cfg["pipeline"]["target_schema"] = "lab09"

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, cfg, "/Workspace/Users/someone@example.com/lab09/pipeline"
    )

    assert pipeline_id == "v2-pipeline-id"
    assert used_serverless is True
    client.pipelines.create.assert_not_called()
    client.pipelines.update.assert_called_once()
    _, kwargs = client.pipelines.update.call_args
    assert kwargs["pipeline_id"] == "v2-pipeline-id"
    assert kwargs["catalog"] == "dbr_dev"
    assert kwargs["target"] == "lab09"


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
        InvalidParameterValue("serverless is not enabled for this workspace's cluster policy"),
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


# --- InvalidParameterValue fallback: positive allow-list, not a denylist ---


def test_is_serverless_capability_rejection_true_for_permission_denied():
    assert pipelines._is_serverless_capability_rejection(PermissionDenied("not authorized"))


def test_is_serverless_capability_rejection_true_for_clearly_serverless_specific_rejection():
    exc = InvalidParameterValue("serverless pipelines are not enabled for this workspace")
    assert pipelines._is_serverless_capability_rejection(exc)


def test_is_serverless_capability_rejection_false_for_target_schema_change_rejection():
    """The real, live-confirmed (2026-09-24) false positive this guards
    against: this message has nothing to do with serverless-vs-classic
    compute, and retrying it as a classic-compute update fails identically.
    """
    exc = InvalidParameterValue(
        "Changing target schema is not allowed. Reason: DLT does not yet support "
        "changing target schema of a pipeline that uses an Default Storage catalog. "
        "Please create a new pipeline if you need to change the target schema."
    )
    assert not pipelines._is_serverless_capability_rejection(exc)


def test_is_serverless_capability_rejection_false_for_unrelated_invalid_parameter_value():
    """A positive allow-list, not a denylist: an InvalidParameterValue that
    doesn't mention serverless at all -- e.g. a genuinely unrelated bad
    argument -- must never be silently treated as "serverless was rejected,
    try classic instead" just because it isn't the one specific
    target-schema message we happened to observe live.
    """
    exc = InvalidParameterValue("Invalid catalog name: 'dbr_dev' contains illegal characters.")
    assert not pipelines._is_serverless_capability_rejection(exc)


def test_ensure_pipeline_update_target_schema_rejection_propagates_without_classic_fallback():
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline_v2"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]
    client.pipelines.update.side_effect = InvalidParameterValue(
        "Changing target schema is not allowed. Reason: DLT does not yet support "
        "changing target schema of a pipeline that uses an Default Storage catalog. "
        "Please create a new pipeline if you need to change the target schema."
    )

    cfg = _cfg()
    cfg["pipeline"]["name"] = "lab09_taxi_pipeline_v2"
    cfg["pipeline"]["target_schema"] = "lab09"

    with pytest.raises(InvalidParameterValue, match="target schema"):
        pipelines.ensure_pipeline(client, cfg, "/Workspace/Users/x/lab09/pipeline")

    # Must NOT attempt a second (classic-compute) update call -- that would
    # fail identically, since the restriction is unrelated to compute type.
    client.pipelines.update.assert_called_once()


def test_ensure_pipeline_update_unrelated_invalid_parameter_value_still_falls_back():
    """Regression guard: a genuine serverless-capability rejection must
    still trigger the classic fallback exactly as before.
    """
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]
    _node_types(client)
    client.pipelines.update.side_effect = [
        InvalidParameterValue("serverless pipelines are not enabled for this workspace"),
        None,
    ]

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "existing-id"
    assert used_serverless is False
    assert client.pipelines.update.call_count == 2


# --- pipeline.allow_classic_fallback safety guard ---------------------------


def test_ensure_pipeline_creates_serverless_normally_with_fallback_disabled():
    """allow_classic_fallback=False must not affect the ordinary, successful
    serverless path at all -- it only changes behavior on a rejection.
    """
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    created = MagicMock()
    created.pipeline_id = "new-id"
    client.pipelines.create.return_value = created

    pipeline_id, used_serverless = pipelines.ensure_pipeline(
        client, _cfg(allow_classic_fallback=False), "/Workspace/Users/x/lab09/pipeline"
    )

    assert pipeline_id == "new-id"
    assert used_serverless is True
    client.pipelines.create.assert_called_once()
    _, kwargs = client.pipelines.create.call_args
    assert kwargs["serverless"] is True


def test_ensure_pipeline_create_rejection_with_fallback_disabled_raises_immediately(monkeypatch):
    """The final safety guard: with allow_classic_fallback=False, a genuine
    serverless-capability rejection must propagate immediately.
    _classic_clusters() must never be called and no classic pipeline
    create is ever attempted, regardless of the serverless error.
    """
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    client.pipelines.create.side_effect = InvalidParameterValue(
        "serverless is not enabled for this workspace"
    )
    classic_clusters_mock = MagicMock(
        side_effect=AssertionError(
            "_classic_clusters must never be called when allow_classic_fallback is false"
        )
    )
    monkeypatch.setattr(pipelines, "_classic_clusters", classic_clusters_mock)

    with pytest.raises(InvalidParameterValue, match="serverless is not enabled"):
        pipelines.ensure_pipeline(
            client, _cfg(allow_classic_fallback=False), "/Workspace/Users/x/lab09/pipeline"
        )

    # No second, classic-compute create attempt.
    client.pipelines.create.assert_called_once()
    classic_clusters_mock.assert_not_called()
    client.clusters.list_node_types.assert_not_called()


def test_ensure_pipeline_update_rejection_with_fallback_disabled_raises_immediately(monkeypatch):
    """Same safety guard on the update (existing-pipeline) path."""
    client = _autospec_client()
    existing = MagicMock()
    existing.name = "lab09_taxi_pipeline"
    existing.pipeline_id = "existing-id"
    client.pipelines.list_pipelines.return_value = [existing]
    client.pipelines.update.side_effect = PermissionDenied(
        "serverless pipelines are not enabled for this workspace"
    )
    classic_clusters_mock = MagicMock(
        side_effect=AssertionError(
            "_classic_clusters must never be called when allow_classic_fallback is false"
        )
    )
    monkeypatch.setattr(pipelines, "_classic_clusters", classic_clusters_mock)

    with pytest.raises(PermissionDenied):
        pipelines.ensure_pipeline(
            client, _cfg(allow_classic_fallback=False), "/Workspace/Users/x/lab09/pipeline"
        )

    client.pipelines.update.assert_called_once()
    classic_clusters_mock.assert_not_called()
    client.clusters.list_node_types.assert_not_called()


def test_ensure_pipeline_fallback_defaults_to_disabled_when_key_absent():
    """Safe-by-default: a config that omits allow_classic_fallback entirely
    must behave as if it were false, matching config/dev.yml's explicit
    `allow_classic_fallback: false` for this strictly-serverless workspace.
    """
    client = _autospec_client()
    client.pipelines.list_pipelines.return_value = []
    client.pipelines.create.side_effect = InvalidParameterValue(
        "serverless is not enabled for this workspace"
    )

    cfg = _cfg()
    del cfg["pipeline"]["allow_classic_fallback"]

    with pytest.raises(InvalidParameterValue):
        pipelines.ensure_pipeline(client, cfg, "/Workspace/Users/x/lab09/pipeline")

    client.pipelines.create.assert_called_once()


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

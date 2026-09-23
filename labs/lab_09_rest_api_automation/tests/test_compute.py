from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, InvalidParameterValue, PermissionDenied

from lab09 import compute


def _spark_version(key: str, name: str) -> MagicMock:
    v = MagicMock()
    v.key = key
    v.name = name
    return v


def _node_type(
    node_type_id: str, num_cores: int, memory_mb: int, deprecated: bool = False
) -> MagicMock:
    nt = MagicMock()
    nt.node_type_id = node_type_id
    nt.num_cores = num_cores
    nt.memory_mb = memory_mb
    nt.is_deprecated = deprecated
    return nt


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def test_resolve_lts_spark_version_prefers_highest_non_ml_lts():
    client = _autospec_client()
    client.clusters.spark_versions.return_value.versions = [
        _spark_version("15.4.x-scala2.12", "15.4 LTS"),
        _spark_version("14.3.x-scala2.12", "14.3 LTS"),
        _spark_version("15.4.x-cpu-ml-scala2.12", "15.4 LTS ML"),
        _spark_version("16.0.x-scala2.12", "16.0 (Beta)"),
    ]

    assert compute.resolve_lts_spark_version(client) == "15.4.x-scala2.12"


def test_resolve_lts_spark_version_falls_back_when_no_lts_available():
    client = _autospec_client()
    client.clusters.spark_versions.return_value.versions = [
        _spark_version("16.0.x-scala2.12", "16.0 (Beta)"),
    ]
    assert compute.resolve_lts_spark_version(client) == "16.0.x-scala2.12"


def test_resolve_node_type_picks_smallest_non_deprecated():
    client = _autospec_client()
    client.clusters.list_node_types.return_value.node_types = [
        _node_type("big", 16, 65536),
        _node_type("tiny-deprecated", 2, 8192, deprecated=True),
        _node_type("small", 4, 16384),
    ]
    assert compute.resolve_node_type(client) == "small"


def test_resolve_node_type_honors_hint_when_present():
    client = _autospec_client()
    client.clusters.list_node_types.return_value.node_types = [
        _node_type("small", 4, 16384),
        _node_type("requested", 8, 32768),
    ]
    assert compute.resolve_node_type(client, node_type_hint="requested") == "requested"


def test_resolve_node_type_ignores_unknown_hint_and_picks_dynamically():
    client = _autospec_client()
    client.clusters.list_node_types.return_value.node_types = [
        _node_type("small", 4, 16384),
        _node_type("big", 16, 65536),
    ]
    assert compute.resolve_node_type(client, node_type_hint="does-not-exist") == "small"


def test_resolve_policy_id_returns_none_when_not_configured():
    client = _autospec_client()
    assert compute.resolve_policy_id(client, None) is None


def test_resolve_policy_id_matches_by_name():
    client = _autospec_client()
    policy = MagicMock()
    policy.name = "lab09-policy"
    policy.policy_id = "abc123"
    client.cluster_policies.list.return_value = [policy]
    assert compute.resolve_policy_id(client, "lab09-policy") == "abc123"


def test_resolve_policy_id_returns_none_when_name_not_found():
    client = _autospec_client()
    client.cluster_policies.list.return_value = []
    assert compute.resolve_policy_id(client, "missing-policy") is None


def test_build_cluster_spec_is_single_node_and_autoterminating_by_default():
    client = _autospec_client()
    client.clusters.spark_versions.return_value.versions = [
        _spark_version("15.4.x-scala2.12", "15.4 LTS")
    ]
    client.clusters.list_node_types.return_value.node_types = [_node_type("small", 4, 16384)]
    client.cluster_policies.list.return_value = []

    cfg = {"compute": {"autotermination_minutes": 20, "single_node": True}}
    spec = compute.build_cluster_spec(client, cfg)

    assert spec.spark_version == "15.4.x-scala2.12"
    assert spec.node_type_id == "small"
    assert spec.autotermination_minutes == 20
    assert spec.num_workers == 0
    assert spec.single_node is True

    kwargs = spec.as_create_kwargs("lab09-temp")
    assert kwargs["spark_conf"]["spark.databricks.cluster.profile"] == "singleNode"
    assert kwargs["custom_tags"]["ResourceClass"] == "SingleNode"
    assert kwargs["autotermination_minutes"] == 20


def test_start_cluster_create_returns_cluster_id_without_waiting():
    client = _autospec_client()
    waiter = MagicMock()
    waiter.cluster_id = "cluster-123"
    client.clusters.create.return_value = waiter

    spec = compute.ClusterSpec(
        spark_version="15.4.x-scala2.12",
        node_type_id="small",
        autotermination_minutes=20,
    )
    cluster_id = compute.start_cluster_create(client, spec, cluster_name="lab09-temp")

    assert cluster_id == "cluster-123"
    waiter.result.assert_not_called()  # must not block on .result()


def test_terminate_cluster_calls_delete():
    client = _autospec_client()
    compute.terminate_cluster(client, "cluster-123")
    client.clusters.delete.assert_called_once_with(cluster_id="cluster-123")


def test_cluster_exists_and_active_false_for_terminated_state():
    client = _autospec_client()
    details = MagicMock()
    details.state = MagicMock()
    details.state.value = "TERMINATED"
    client.clusters.get.return_value = details

    assert compute.cluster_exists_and_active(client, "cluster-123") is False


def test_cluster_exists_and_active_true_for_running_state():
    client = _autospec_client()
    details = MagicMock()
    details.state = MagicMock()
    details.state.value = "RUNNING"
    client.clusters.get.return_value = details

    assert compute.cluster_exists_and_active(client, "cluster-123") is True


def test_cluster_exists_and_active_false_when_lookup_fails():
    client = _autospec_client()
    client.clusters.get.side_effect = RuntimeError("not found")
    assert compute.cluster_exists_and_active(client, "cluster-123") is False


# --- try_start_cluster_create: the explicit-cluster-vs-fallback decision ---


def _spec() -> compute.ClusterSpec:
    return compute.ClusterSpec(
        spark_version="15.4.x-scala2.12", node_type_id="small", autotermination_minutes=20
    )


def test_try_start_cluster_create_returns_cluster_id_on_success():
    client = _autospec_client()
    waiter = MagicMock()
    waiter.cluster_id = "cluster-1"
    client.clusters.create.return_value = waiter

    cluster_id, rejection = compute.try_start_cluster_create(client, _spec(), "lab09-temp")

    assert cluster_id == "cluster-1"
    assert rejection is None


def test_try_start_cluster_create_falls_back_on_permission_denied():
    client = _autospec_client()
    client.clusters.create.side_effect = PermissionDenied("not allowed to create clusters")

    cluster_id, rejection = compute.try_start_cluster_create(client, _spec(), "lab09-temp")

    assert cluster_id is None
    assert isinstance(rejection, PermissionDenied)


def test_try_start_cluster_create_falls_back_on_invalid_parameter_value():
    client = _autospec_client()
    client.clusters.create.side_effect = InvalidParameterValue("cluster policy forbids this shape")

    cluster_id, rejection = compute.try_start_cluster_create(client, _spec(), "lab09-temp")

    assert cluster_id is None
    assert isinstance(rejection, InvalidParameterValue)


def test_try_start_cluster_create_does_not_swallow_unrelated_errors():
    client = _autospec_client()
    client.clusters.create.side_effect = InternalError("service is having a bad day")

    with pytest.raises(InternalError):
        compute.try_start_cluster_create(client, _spec(), "lab09-temp")


def test_try_start_cluster_create_does_not_swallow_plain_exceptions():
    client = _autospec_client()
    client.clusters.create.side_effect = RuntimeError("network error")

    with pytest.raises(RuntimeError):
        compute.try_start_cluster_create(client, _spec(), "lab09-temp")

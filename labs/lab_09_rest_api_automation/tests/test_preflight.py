from unittest.mock import MagicMock, create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, NotFound

from lab09 import compute, monitoring, preflight


def _autospec_client() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _cfg() -> dict:
    return {
        "catalog": "dbr_dev",
        "schema": "parvinbadalov",
        "volume": "lab09_landing",
        "compute": {"autotermination_minutes": 20, "single_node": True},
        "monitoring": {"cluster_timeout_seconds": 900, "cluster_poll_interval_seconds": 10},
    }


def _passing_client() -> MagicMock:
    client = _autospec_client()
    client.current_user.me.return_value.user_name = "someone@example.com"
    client.clusters.spark_versions.return_value.versions = [
        MagicMock(key="15.4.x-scala2.12", name="15.4 LTS")
    ]
    client.clusters.list_node_types.return_value.node_types = [
        MagicMock(node_type_id="small", num_cores=4, memory_mb=16384, is_deprecated=False)
    ]
    client.cluster_policies.list.return_value = []
    client.catalogs.get.return_value.name = "dbr_dev"
    client.schemas.get.return_value.name = "parvinbadalov"
    client.volumes.read.side_effect = NotFound("volume does not exist")
    client.pipelines.list_pipelines.return_value = []
    return client


def test_run_preflight_all_checks_pass_without_cluster_probe():
    client = _passing_client()
    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    assert report.identity == "someone@example.com"
    assert report.passed is True
    assert report.cluster_create_tested is False
    assert report.cluster_create_supported is None
    check_names = {c.name for c in report.checks}
    assert {
        "authenticated_identity",
        "spark_runtimes_listed",
        "node_types_listed",
        "cluster_policies_listed",
        "catalog_access",
        "schema_access",
        "volume_access",
        "files_api_roundtrip",
        "pipeline_list_permission",
        "cluster_create_probe",
    } <= check_names


def test_run_preflight_volume_not_found_is_reported_as_passed():
    client = _passing_client()
    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    volume_check = next(c for c in report.checks if c.name == "volume_access")
    assert volume_check.status == "PASS"
    assert volume_check.passed is True
    assert "does not exist yet" in volume_check.detail


def test_run_preflight_files_roundtrip_is_not_tested_when_volume_missing():
    """The truthfulness fix: an unexercised capability must never be PASS."""
    client = _passing_client()
    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    files_check = next(c for c in report.checks if c.name == "files_api_roundtrip")
    assert files_check.status == "NOT_TESTED"
    assert files_check.passed is False
    assert "not exercised" in files_check.detail
    client.files.upload.assert_not_called()
    client.files.create_directory.assert_not_called()

    # A NOT_TESTED check does not block the overall report, but it is never
    # conflated with an actually-passed check.
    assert report.passed is True


def test_run_preflight_does_files_roundtrip_when_volume_exists():
    client = _passing_client()
    client.volumes.read.side_effect = None
    client.volumes.read.return_value = MagicMock()

    probe_entry = MagicMock()
    client.files.list_directory_contents.return_value = [probe_entry]

    with patch("lab09.preflight.uuid") as fake_uuid:
        fake_uuid.uuid4.return_value.hex = "abc123"
        probe_entry.path = (
            "/Volumes/dbr_dev/parvinbadalov/lab09_landing/_preflight/probe_abc123.txt"
        )
        report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    files_check = next(c for c in report.checks if c.name == "files_api_roundtrip")
    assert files_check.status == "PASS"
    client.files.create_directory.assert_called_once_with(
        "/Volumes/dbr_dev/parvinbadalov/lab09_landing/_preflight"
    )
    client.files.upload.assert_called_once()
    client.files.delete.assert_called_once()


def test_run_preflight_files_roundtrip_fails_when_probe_not_visible_in_listing():
    client = _passing_client()
    client.volumes.read.side_effect = None
    client.volumes.read.return_value = MagicMock()
    client.files.list_directory_contents.return_value = []  # probe never shows up

    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    files_check = next(c for c in report.checks if c.name == "files_api_roundtrip")
    assert files_check.status == "FAIL"
    client.files.delete.assert_called_once()  # cleanup still attempted
    assert report.passed is False


def test_run_preflight_volume_lookup_failure_propagates_as_fail_not_missing():
    """A real error (auth/permission/service) must not be mistaken for "missing"."""
    client = _passing_client()
    client.volumes.read.side_effect = InternalError("service unavailable")

    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    volume_check = next(c for c in report.checks if c.name == "volume_access")
    assert volume_check.status == "FAIL"
    assert report.passed is False


def test_run_preflight_one_failing_check_does_not_abort_the_rest():
    client = _passing_client()
    client.catalogs.get.side_effect = RuntimeError("permission denied")

    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    catalog_check = next(c for c in report.checks if c.name == "catalog_access")
    assert catalog_check.status == "FAIL"
    assert report.passed is False

    # Every other check still ran despite catalog_access failing.
    schema_check = next(c for c in report.checks if c.name == "schema_access")
    assert schema_check.status == "PASS"


def test_run_preflight_records_ci_identity_caveat():
    client = _passing_client()
    report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)
    assert "does NOT prove" in report.ci_identity_caveat


# --- cluster-create probe -------------------------------------------------


def test_run_preflight_cluster_probe_success_terminates_cluster():
    client = _passing_client()
    spec = compute.ClusterSpec(
        spark_version="15.4.x-scala2.12", node_type_id="small", autotermination_minutes=20
    )
    outcome = monitoring.ClusterOutcome(
        cluster_id="probe-cluster", state="RUNNING", timed_out=False
    )

    with (
        patch.object(preflight.compute, "build_cluster_spec", return_value=spec) as build_spec,
        patch.object(
            preflight.compute, "start_cluster_create", return_value="probe-cluster"
        ) as start_create,
        patch.object(
            preflight.monitoring, "poll_cluster_state", return_value=outcome
        ) as poll_state,
        patch.object(preflight.compute, "terminate_cluster") as terminate,
    ):
        report = preflight.run_preflight(client, _cfg(), probe_cluster_create=True)

    assert report.cluster_create_tested is True
    assert report.cluster_create_supported is True
    build_spec.assert_called_once()
    start_create.assert_called_once()
    poll_state.assert_called_once()
    terminate.assert_called_once_with(client, "probe-cluster")

    probe_check = next(c for c in report.checks if c.name == "cluster_create_probe")
    assert probe_check.status == "PASS"


def test_run_preflight_cluster_probe_still_terminates_on_failure_after_create():
    client = _passing_client()
    spec = compute.ClusterSpec(
        spark_version="15.4.x-scala2.12", node_type_id="small", autotermination_minutes=20
    )

    with (
        patch.object(preflight.compute, "build_cluster_spec", return_value=spec),
        patch.object(preflight.compute, "start_cluster_create", return_value="probe-cluster"),
        patch.object(
            preflight.monitoring, "poll_cluster_state", side_effect=RuntimeError("polling exploded")
        ),
        patch.object(preflight.compute, "terminate_cluster") as terminate,
    ):
        report = preflight.run_preflight(client, _cfg(), probe_cluster_create=True)

    assert report.cluster_create_supported is False
    terminate.assert_called_once_with(client, "probe-cluster")

    probe_check = next(c for c in report.checks if c.name == "cluster_create_probe")
    assert probe_check.status == "FAIL"


def test_run_preflight_cluster_probe_not_run_when_not_requested():
    client = _passing_client()
    with patch.object(preflight.compute, "start_cluster_create") as start_create:
        report = preflight.run_preflight(client, _cfg(), probe_cluster_create=False)

    start_create.assert_not_called()
    assert report.cluster_create_tested is False
    probe_check = next(c for c in report.checks if c.name == "cluster_create_probe")
    assert probe_check.status == "NOT_TESTED"
    assert probe_check.passed is False
    assert "not requested" in probe_check.detail

    # A NOT_TESTED cluster-create probe does not block the overall report --
    # run-all's own preflight gate must not fail every run just because the
    # expensive opt-in probe wasn't requested.
    assert report.passed is True

from types import SimpleNamespace

import pytest

from lab09 import client


def _fake_client(host: str, token: str | None = None, client_id: str | None = None):
    return SimpleNamespace(config=SimpleNamespace(host=host, token=token, client_id=client_id))


def _write_cfg(tmp_path, contents: str):
    path = tmp_path / ".databrickscfg"
    path.write_text(contents, encoding="utf-8")
    return path


# --- _read_profile_section ---------------------------------------------------


def test_read_profile_section_returns_none_when_file_missing(tmp_path):
    missing = tmp_path / "does-not-exist"
    assert client._read_profile_section("some-profile", missing) is None


def test_read_profile_section_returns_none_when_profile_missing(tmp_path):
    path = _write_cfg(tmp_path, "[other-profile]\nhost = https://example.azuredatabricks.net\n")
    assert client._read_profile_section("some-profile", path) is None


def test_read_profile_section_returns_the_matching_section(tmp_path):
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n"
    )
    section = client._read_profile_section("lab09-azure-prod-oauth", path)
    assert section["host"] == "https://example.azuredatabricks.net"


# --- _verify_profile_resolution ----------------------------------------------
#
# Replaces an earlier, now-removed approach (suppressing ~19 identity-relevant
# environment variables in os.environ, guarded by a threading.Lock, for the
# duration of a profile-based WorkspaceClient(...) call). That approach had a
# real, unfixable gap even with the lock: os.environ is process-global with no
# thread-local scoping, so the lock only ever protected calls that went
# through get_workspace_client() itself -- it could not and did not protect
# any other, unrelated thread in the same process from observing those
# variables as transiently absent. This approach instead verifies the
# OUTCOME of an already-constructed client against an independent,
# read-only parse of ~/.databrickscfg, mutating no process-wide state at
# all, so there is nothing for a concurrent thread to ever observe
# inconsistently.


def test_verify_profile_resolution_passes_for_a_matching_oauth_style_profile(tmp_path):
    """The exact shape of this project's own lab09-azure-prod-oauth profile:
    a host line and nothing else (no token, no client_id)."""
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n"
    )
    resolved = _fake_client(host="https://example.azuredatabricks.net")

    client._verify_profile_resolution(
        "lab09-azure-prod-oauth", resolved, config_path=path
    )  # no raise


def test_verify_profile_resolution_passes_for_a_matching_pat_style_profile(tmp_path):
    path = _write_cfg(
        tmp_path,
        "[dev]\nhost = https://example.azuredatabricks.net\ntoken = anything-not-compared\n",
    )
    resolved = _fake_client(
        host="https://example.azuredatabricks.net", token="whatever-the-sdk-resolved"
    )

    client._verify_profile_resolution("dev", resolved, config_path=path)  # no raise


def test_verify_profile_resolution_normalizes_host_formatting_differences(tmp_path):
    path = _write_cfg(tmp_path, "[dev]\nhost = example.azuredatabricks.net\n")
    resolved = _fake_client(host="https://EXAMPLE.azuredatabricks.net/")

    client._verify_profile_resolution("dev", resolved, config_path=path)  # no raise


def test_verify_profile_resolution_raises_when_host_mismatches(tmp_path):
    """Direct regression test for the original real-world scenario: an
    ambient DATABRICKS_HOST (or DATABRICKS_CONFIG_FILE) resolving a
    different workspace than the explicitly requested profile declares.
    """
    path = _write_cfg(tmp_path, "[dev]\nhost = https://correct-host.azuredatabricks.net\n")
    resolved = _fake_client(host="https://ambient-env-host.example.com")

    with pytest.raises(client.ProfileResolutionMismatchError, match="host"):
        client._verify_profile_resolution("dev", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_ambient_token_hijacks_a_no_token_profile(tmp_path):
    """The central regression test: this is exactly the real scenario this
    project's OAuth profile work was worried about -- an ambient
    DATABRICKS_TOKEN silently being used instead of a profile that declares
    no token of its own (an OAuth-only profile like lab09-azure-prod-oauth).
    """
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n"
    )
    resolved = _fake_client(
        host="https://example.azuredatabricks.net", token="ambient-token-should-never-be-used"
    )

    with pytest.raises(client.ProfileResolutionMismatchError, match="token"):
        client._verify_profile_resolution("lab09-azure-prod-oauth", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_expected_token_is_missing(tmp_path):
    path = _write_cfg(
        tmp_path, "[dev]\nhost = https://example.azuredatabricks.net\ntoken = something\n"
    )
    resolved = _fake_client(host="https://example.azuredatabricks.net", token=None)

    with pytest.raises(client.ProfileResolutionMismatchError, match="token"):
        client._verify_profile_resolution("dev", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_ambient_client_id_hijacks_the_profile(tmp_path):
    """An ambient DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET redirecting
    authentication to a different OAuth service principal than the profile
    itself declares (or, as here, declares none of).
    """
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n"
    )
    resolved = _fake_client(
        host="https://example.azuredatabricks.net", client_id="unexpected-service-principal"
    )

    with pytest.raises(client.ProfileResolutionMismatchError, match="client_id"):
        client._verify_profile_resolution("lab09-azure-prod-oauth", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_expected_client_id_is_missing(tmp_path):
    path = _write_cfg(
        tmp_path,
        "[m2m-profile]\nhost = https://example.azuredatabricks.net\nclient_id = expected-id\n",
    )
    resolved = _fake_client(host="https://example.azuredatabricks.net", client_id=None)

    with pytest.raises(client.ProfileResolutionMismatchError, match="client_id"):
        client._verify_profile_resolution("m2m-profile", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_config_file_is_missing(tmp_path):
    missing = tmp_path / "does-not-exist"
    resolved = _fake_client(host="https://example.azuredatabricks.net")

    with pytest.raises(client.ProfileResolutionMismatchError, match="lab09-azure-prod-oauth"):
        client._verify_profile_resolution("lab09-azure-prod-oauth", resolved, config_path=missing)


def test_verify_profile_resolution_raises_when_profile_section_is_missing(tmp_path):
    """Covers the DATABRICKS_CONFIG_FILE-redirection scenario: the client
    resolved successfully (presumably against some other file entirely),
    but the profile it claims to be does not exist in the canonical
    ~/.databrickscfg this check always reads.
    """
    path = _write_cfg(tmp_path, "[some-other-profile]\nhost = https://elsewhere.example.com\n")
    resolved = _fake_client(host="https://elsewhere.example.com")

    with pytest.raises(client.ProfileResolutionMismatchError, match="lab09-azure-prod-oauth"):
        client._verify_profile_resolution("lab09-azure-prod-oauth", resolved, config_path=path)


# --- get_workspace_client(): end-to-end wiring -------------------------------


def test_get_workspace_client_uses_explicit_profile_argument(monkeypatch, tmp_path):
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n"
    )
    monkeypatch.setattr(client, "_default_databrickscfg_path", lambda: path)
    captured = {}

    def fake_workspace_client(**kwargs):
        captured.update(kwargs)
        return _fake_client(host="https://example.azuredatabricks.net")

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    client.get_workspace_client(profile="lab09-azure-prod-oauth")

    assert captured == {"profile": "lab09-azure-prod-oauth"}


def test_get_workspace_client_uses_config_profile_env_var(monkeypatch, tmp_path):
    path = _write_cfg(tmp_path, "[some-profile]\nhost = https://example.azuredatabricks.net\n")
    monkeypatch.setattr(client, "_default_databrickscfg_path", lambda: path)
    captured = {}

    def fake_workspace_client(**kwargs):
        captured.update(kwargs)
        return _fake_client(host="https://example.azuredatabricks.net")

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "some-profile")
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    client.get_workspace_client()

    assert captured == {"profile": "some-profile"}


def test_get_workspace_client_falls_back_to_host_token_env_vars(monkeypatch):
    calls = []
    monkeypatch.setattr(client, "WorkspaceClient", lambda **kwargs: calls.append(kwargs) or kwargs)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dummy-token-value")

    client.get_workspace_client()

    # No profile kwarg forwarded, and no ~/.databrickscfg verification is
    # attempted in this path -- WorkspaceClient() itself resolves the
    # host/token straight from the environment, which is the whole point of
    # this path (the pattern GitHub Actions CI uses).
    assert calls == [{}]


def test_get_workspace_client_raises_when_nothing_is_configured(monkeypatch):
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    with pytest.raises(client.ProfileNotSpecifiedError):
        client.get_workspace_client()


def test_get_workspace_client_profile_argument_wins_over_config_profile_env_var(
    monkeypatch, tmp_path
):
    path = _write_cfg(tmp_path, "[explicit-profile]\nhost = https://example.azuredatabricks.net\n")
    monkeypatch.setattr(client, "_default_databrickscfg_path", lambda: path)
    captured = {}

    def fake_workspace_client(**kwargs):
        captured.update(kwargs)
        return _fake_client(host="https://example.azuredatabricks.net")

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "env-profile")
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    client.get_workspace_client(profile="explicit-profile")

    assert captured == {"profile": "explicit-profile"}


def test_get_workspace_client_raises_when_resolution_does_not_match_the_profile(
    monkeypatch, tmp_path
):
    """End-to-end version of the central regression test: if whatever the
    SDK actually resolves (simulated here by the fake WorkspaceClient,
    standing in for an ambient-env-var hijack having already happened
    inside real SDK resolution) does not match the explicitly requested
    profile's own declared host, get_workspace_client() must raise rather
    than silently return a client pointed at the wrong workspace.
    """
    path = _write_cfg(
        tmp_path, "[lab09-azure-prod-oauth]\nhost = https://correct-host.azuredatabricks.net\n"
    )
    monkeypatch.setattr(client, "_default_databrickscfg_path", lambda: path)
    monkeypatch.setattr(
        client,
        "WorkspaceClient",
        lambda **kwargs: _fake_client(host="https://hijacked-by-ambient-env.example.com"),
    )
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)

    with pytest.raises(client.ProfileResolutionMismatchError):
        client.get_workspace_client(profile="lab09-azure-prod-oauth")


def test_load_config_parses_yaml_mapping(tmp_path):
    path = tmp_path / "dev.yml"
    path.write_text("catalog: dbr_dev\nschema: parvinbadalov\n", encoding="utf-8")

    cfg = client.load_config(path)

    assert cfg == {"catalog": "dbr_dev", "schema": "parvinbadalov"}


def test_load_config_rejects_non_mapping_yaml(tmp_path):
    path = tmp_path / "list.yml"
    path.write_text("- one\n- two\n", encoding="utf-8")

    with pytest.raises(ValueError):
        client.load_config(path)


def test_volume_root_path_and_derived_paths():
    cfg = {"catalog": "dbr_dev", "schema": "parvinbadalov", "volume": "lab09_landing"}

    assert client.volume_root_path(cfg) == "/Volumes/dbr_dev/parvinbadalov/lab09_landing"
    assert client.trips_path(cfg) == "/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips"
    assert client.reference_path(cfg) == "/Volumes/dbr_dev/parvinbadalov/lab09_landing/reference"

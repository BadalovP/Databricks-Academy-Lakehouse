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
        "[dev]\nhost = https://example.azuredatabricks.net\ntoken = profile-own-token-value\n",
    )
    resolved = _fake_client(
        host="https://example.azuredatabricks.net", token="profile-own-token-value"
    )

    client._verify_profile_resolution("dev", resolved, config_path=path)  # no raise


def test_verify_profile_resolution_raises_when_same_host_and_presence_but_different_token_value(
    tmp_path,
):
    """The vulnerability this specific fix closes: an earlier version of
    this check compared only whether a token was present, not its value.
    An ambient DATABRICKS_TOKEN with a *different* value than the profile's
    own -- while DATABRICKS_HOST is left unset, so the profile file's own
    host still resolves correctly -- has an identical host and an identical
    "has a token" presence shape as the correct resolution, and would pass
    a presence-only check silently. Confirmed empirically (synthetic values
    only) against the pre-fix implementation before this test was written.
    """
    path = _write_cfg(
        tmp_path,
        "[dev]\nhost = https://correct-host.azuredatabricks.net\ntoken = profile-own-real-token\n",
    )
    resolved = _fake_client(
        host="https://correct-host.azuredatabricks.net",  # unchanged -- DATABRICKS_HOST wasn't set
        token="WRONG-ambient-token-value",  # but DATABRICKS_TOKEN was, and won
    )

    with pytest.raises(client.ProfileResolutionMismatchError, match="token"):
        client._verify_profile_resolution("dev", resolved, config_path=path)


def test_verify_profile_resolution_raises_when_same_presence_but_different_client_id_value(
    tmp_path,
):
    """Same class of vulnerability as the token case above, for client_id:
    an ambient DATABRICKS_CLIENT_ID targeting a different OAuth service
    principal than the profile's own, with the same host and the same
    "has a client_id" presence shape.
    """
    path = _write_cfg(
        tmp_path,
        "[m2m-profile]\nhost = https://example.azuredatabricks.net\n"
        "client_id = profile-own-service-principal\n",
    )
    resolved = _fake_client(
        host="https://example.azuredatabricks.net",
        client_id="WRONG-ambient-service-principal",
    )

    with pytest.raises(client.ProfileResolutionMismatchError, match="client_id"):
        client._verify_profile_resolution("m2m-profile", resolved, config_path=path)


def test_verify_profile_resolution_ignores_default_section_fallback_like_the_sdk_does(tmp_path):
    """Mirrors a documented quirk of the installed SDK's own profile
    resolution (Config._known_file_config_loader reads ConfigParser's raw
    `_sections`, not the DEFAULT-merged per-section view, matching the Go
    SDK's behavior): a [DEFAULT] section's values must NOT be silently
    treated as part of a named profile's own declared values here, or this
    check could disagree with what the SDK itself actually resolved.
    """
    path = _write_cfg(
        tmp_path,
        "[DEFAULT]\ntoken = default-section-token\n"
        "[lab09-azure-prod-oauth]\nhost = https://example.azuredatabricks.net\n",
    )
    # The profile itself declares no token -- if DEFAULT were merged in, this
    # would wrongly expect one.
    resolved = _fake_client(host="https://example.azuredatabricks.net", token=None)

    client._verify_profile_resolution(
        "lab09-azure-prod-oauth", resolved, config_path=path
    )  # no raise


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


def test_get_workspace_client_falls_back_to_host_and_azure_cli_auth_type(monkeypatch):
    """The Azure GitHub Actions workflow authenticates via OIDC (azure/login
    + `az` CLI), never a PAT -- DATABRICKS_HOST + DATABRICKS_AUTH_TYPE=azure-cli
    with no DATABRICKS_TOKEN at all must resolve the same way the existing
    host+token CI pattern does, matching Lab 8's own already-configured
    azure-cli auth flow exactly.
    """
    calls = []
    monkeypatch.setattr(client, "WorkspaceClient", lambda **kwargs: calls.append(kwargs) or kwargs)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "azure-cli")

    client.get_workspace_client()

    assert calls == [{}]


def test_get_workspace_client_raises_when_host_set_but_no_token_or_azure_cli_auth(monkeypatch):
    """DATABRICKS_HOST alone, with neither a token nor azure-cli auth type,
    must still refuse to guess rather than silently attempting some other
    ambient auth method.
    """
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_AUTH_TYPE", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.azuredatabricks.net")

    with pytest.raises(client.ProfileNotSpecifiedError):
        client.get_workspace_client()


def test_get_workspace_client_raises_when_nothing_is_configured(monkeypatch):
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_AUTH_TYPE", raising=False)

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

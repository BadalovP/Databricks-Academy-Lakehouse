import threading
import time

import pytest

from lab09 import client


def test_get_workspace_client_uses_explicit_profile_argument(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        client, "WorkspaceClient", lambda **kwargs: captured.update(kwargs) or kwargs
    )
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    client.get_workspace_client(profile="lab09-azure-prod-oauth")

    assert captured == {"profile": "lab09-azure-prod-oauth"}


def test_get_workspace_client_uses_config_profile_env_var(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        client, "WorkspaceClient", lambda **kwargs: captured.update(kwargs) or kwargs
    )
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

    # No profile kwarg forwarded -- WorkspaceClient() itself resolves the
    # host/token straight from the environment in this path.
    assert calls == [{}]


def test_get_workspace_client_raises_when_nothing_is_configured(monkeypatch):
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    with pytest.raises(client.ProfileNotSpecifiedError):
        client.get_workspace_client()


def test_get_workspace_client_profile_argument_wins_over_config_profile_env_var(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        client, "WorkspaceClient", lambda **kwargs: captured.update(kwargs) or kwargs
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "env-profile")
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    client.get_workspace_client(profile="explicit-profile")

    assert captured == {"profile": "explicit-profile"}


def test_get_workspace_client_suppresses_ambient_host_token_env_vars_around_profile_call(
    monkeypatch,
):
    """Regression test for a real bug found during review: the installed
    Databricks SDK's own Config resolution (databricks.sdk.config.Config
    ._load_from_env / _known_file_config_loader) loads DATABRICKS_HOST /
    DATABRICKS_TOKEN from the environment BEFORE reading a profile file,
    and never overwrites an attribute the environment already populated --
    so WorkspaceClient(profile=...) alone does NOT stop an ambient
    DATABRICKS_TOKEN (e.g. left exported from an earlier, different
    profile/session) from silently overriding the explicitly requested
    profile's own credentials. This directly contradicts this function's
    own documented "first one found wins" resolution order and is exactly
    the ambient-credential risk this project has otherwise been careful to
    avoid (see the dedicated lab09-azure-prod-oauth OAuth profile work).
    get_workspace_client() must suppress DATABRICKS_HOST/DATABRICKS_TOKEN
    for the duration of the profile-based WorkspaceClient(...) call, and
    restore them immediately afterward.
    """
    seen_env_during_call = {}

    def fake_workspace_client(**kwargs):
        import os

        seen_env_during_call["DATABRICKS_HOST"] = os.environ.get("DATABRICKS_HOST")
        seen_env_during_call["DATABRICKS_TOKEN"] = os.environ.get("DATABRICKS_TOKEN")
        return kwargs

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://ambient-env-host.example.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "ambient-env-token-should-never-be-used")

    client.get_workspace_client(profile="lab09-azure-prod-oauth")

    assert seen_env_during_call["DATABRICKS_HOST"] is None
    assert seen_env_during_call["DATABRICKS_TOKEN"] is None
    # Restored afterward -- this function must not permanently mutate the
    # caller's environment.
    import os

    assert os.environ["DATABRICKS_HOST"] == "https://ambient-env-host.example.com"
    assert os.environ["DATABRICKS_TOKEN"] == "ambient-env-token-should-never-be-used"


def test_get_workspace_client_suppresses_the_full_identity_relevant_env_var_set(monkeypatch):
    """Regression test for a second, broader instance of the same class of
    bug: the original fix only suppressed DATABRICKS_HOST/DATABRICKS_TOKEN,
    but the installed SDK resolves WHO a client authenticates as from many
    more environment variables than just those two -- DATABRICKS_CLIENT_ID/
    DATABRICKS_CLIENT_SECRET can redirect to a completely different OAuth
    service principal, DATABRICKS_AUTH_TYPE can force a different auth
    mechanism than the profile's own, DATABRICKS_CONFIG_FILE can make "the
    profile" resolve against a different file entirely, and the ARM_* /
    DATABRICKS_USERNAME / DATABRICKS_PASSWORD variables carry the same class
    of risk. Every one of these must be suppressed for the duration of a
    profile-based call and restored immediately afterward, not just
    host/token.
    """
    seen_env_during_call = {}

    def fake_workspace_client(**kwargs):
        import os

        for name in client._AUTH_IDENTITY_ENV_VARS:
            seen_env_during_call[name] = os.environ.get(name)
        return kwargs

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)

    ambient_values = {
        name: f"ambient-{name.lower()}-value" for name in client._AUTH_IDENTITY_ENV_VARS
    }
    for name, value in ambient_values.items():
        monkeypatch.setenv(name, value)

    client.get_workspace_client(profile="lab09-azure-prod-oauth")

    for name in client._AUTH_IDENTITY_ENV_VARS:
        assert seen_env_during_call[name] is None, f"{name} was not suppressed during the call"

    import os

    for name, value in ambient_values.items():
        assert os.environ[name] == value, f"{name} was not restored after the call"


def test_get_workspace_client_calls_are_thread_safe_across_the_suppression_window(monkeypatch):
    """Regression test for a real gap found during review: os.environ is
    process-global mutable state, so two concurrent get_workspace_client()
    calls in different threads could otherwise interleave their
    suppress/restore windows -- e.g. thread A restores DATABRICKS_TOKEN
    while thread B's WorkspaceClient(...) construction is still in
    progress and depends on it staying suppressed, or vice versa.
    _env_lock must serialize the critical section so the two calls' windows
    never overlap in time.
    """
    intervals = []
    intervals_lock = threading.Lock()

    def fake_workspace_client(**kwargs):
        start = time.monotonic()
        time.sleep(0.05)
        end = time.monotonic()
        with intervals_lock:
            intervals.append((start, end))
        return kwargs

    monkeypatch.setattr(client, "WorkspaceClient", fake_workspace_client)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    threads = [
        threading.Thread(target=client.get_workspace_client, kwargs={"profile": f"profile-{i}"})
        for i in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert len(intervals) == 2
    (start1, end1), (start2, end2) = intervals
    # The two calls' critical sections must not overlap: one must have
    # fully finished before the other started.
    assert (
        end1 <= start2 or end2 <= start1
    ), f"concurrent get_workspace_client() calls overlapped: {intervals}"


def test_get_workspace_client_restores_env_vars_even_if_construction_raises(monkeypatch):
    def _boom(**kwargs):
        raise RuntimeError("auth failed")

    monkeypatch.setattr(client, "WorkspaceClient", _boom)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "https://ambient-env-host.example.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "ambient-env-token-value")

    with pytest.raises(RuntimeError):
        client.get_workspace_client(profile="lab09-azure-prod-oauth")

    import os

    assert os.environ["DATABRICKS_HOST"] == "https://ambient-env-host.example.com"
    assert os.environ["DATABRICKS_TOKEN"] == "ambient-env-token-value"


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

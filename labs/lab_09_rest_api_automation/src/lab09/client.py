"""Databricks SDK client construction and config loading for LAB 09."""

from __future__ import annotations

import configparser
import os
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient


class ProfileNotSpecifiedError(RuntimeError):
    """Raised when no Databricks CLI profile was explicitly provided.

    LAB 09 deliberately refuses to guess a profile. This repository's local
    ~/.databrickscfg has profiles literally named "dev" and "AZURE_DEV" that
    resolve to the same host as "AZURE_PROD" (see config/dev.yml's header
    comment and README.md's "Security model" section). Silently defaulting
    to an unspecified profile risks resolving against a production
    workspace, which is exactly the mistake Lab 8's PAT preflight fix
    (PR #20) had to correct for a different but related reason.
    """


class ProfileResolutionMismatchError(RuntimeError):
    """Raised when a constructed client did not resolve the way its explicitly
    requested profile, read directly from ~/.databrickscfg, says it should.

    See _verify_profile_resolution()'s docstring for the full reasoning.
    """


def _default_databrickscfg_path() -> Path:
    return Path.home() / ".databrickscfg"


def _normalize_host(host: str | None) -> str | None:
    """Loose normalization for comparing a raw ini host value against the
    SDK's own resolved, scheme-qualified host -- good enough for Databricks
    hosts specifically (never a non-default port), without depending on the
    SDK's own private host-normalization helper.
    """
    if not host:
        return host
    host = host.strip()
    if "://" not in host:
        host = "https://" + host
    return host.rstrip("/").lower()


def _read_profile_section(profile: str, config_path: Path) -> dict[str, str] | None:
    """Read one profile's raw section from a .databrickscfg-format file.

    Returns None if the file or the section does not exist. Returns the raw
    section dict directly from ConfigParser's own private `_sections`
    mapping rather than the public `parser[profile]` proxy, deliberately:
    the public proxy transparently merges in `[DEFAULT]` section values as
    fallback, but the installed databricks-sdk's own profile resolution
    (`Config._known_file_config_loader`) explicitly reads `_sections`
    instead of using per-section DEFAULT merging, matching the Go SDK's
    behavior (confirmed by reading its own inline comment). Using the
    public proxy here could silently disagree with what the SDK itself
    actually resolved for a profile whenever `~/.databrickscfg` happens to
    have a `[DEFAULT]` section -- this mirrors the SDK's own semantics
    exactly instead.

    Never returns anything beyond what is present in the file; callers
    still must never dump or log any value read here (a token value must
    never be displayed -- see the credential-safety memory this project has
    followed throughout). Comparing values in memory, without ever
    displaying or logging them, is safe and is exactly what the checks
    below do.
    """
    if not config_path.exists():
        return None
    parser = configparser.ConfigParser()
    parser.read(config_path)
    return parser._sections.get(profile)  # noqa: SLF001 -- see docstring above


def _verify_profile_resolution(
    resolved_profile: str,
    client: WorkspaceClient,
    config_path: Path | None = None,
) -> None:
    """Confirm a constructed client actually resolved the way the explicitly
    requested profile, read directly from the canonical ~/.databrickscfg,
    says it should -- and raise loudly rather than silently proceeding if not.

    This replaces an earlier approach (suppressing DATABRICKS_HOST/
    DATABRICKS_TOKEN, then an expanded list of ~19 identity-relevant
    environment variables, in os.environ for the duration of the
    WorkspaceClient(profile=...) call) that mutated process-global state.
    That approach had a real, unfixable gap even with a threading.Lock
    around the mutation: the lock only ever serialized calls that went
    through this same function -- it could not and did not protect any
    other, unrelated thread in the same process from observing those
    environment variables as transiently absent, since os.environ has no
    thread-local scoping in Python. Reviewing the installed SDK for a
    supported alternative found no public API to construct a Config/
    WorkspaceClient that resolves purely from a named profile file, ignoring
    the environment entirely (no `from_profile` classmethod, no env-skip
    flag on Config.__init__ -- confirmed by inspecting its signature).

    This function instead performs a verify-AFTER-construction check using
    only public, already-resolved client.config attributes, compared
    against a fresh, independent, read-only parse of ~/.databrickscfg --
    never mutating any process state, so there is nothing for a concurrent
    thread to ever observe in an inconsistent state. It also closes a wider
    class of risk than the environment-variable-enumeration approach did:
    it does not need to know about every individual environment variable
    the SDK might resolve identity from (including ones a future SDK
    version could add), only whether the OUTCOME matches what the profile
    itself declares.

    Deliberately always reads the canonical ~/.databrickscfg path, ignoring
    an ambient DATABRICKS_CONFIG_FILE override, even though the SDK itself
    would honor that variable when resolving the profile actually used by
    `client`. This is intentional, not an oversight: this project's own
    documented usage (README.md "Security model", config/dev.yml's header
    comment) is exclusively in terms of ~/.databrickscfg, so an ambient
    DATABRICKS_CONFIG_FILE silently redirecting resolution to a different
    file is itself exactly the kind of ambient-environment interference
    this check exists to catch -- if that happened, the profile's
    canonical entry (or the profile itself) will very likely not match
    what got resolved, and this raises rather than silently trusting it.

    Compares actual VALUES for host and every credential-bearing field the
    profile might declare (token, client_id, client_secret, username,
    password) -- not merely whether each one is present. An earlier version
    of this check compared presence only (e.g. "the profile has *a* token"
    vs. "the resolved client has *a* token"), which left a real gap: an
    ambient DATABRICKS_TOKEN with a *different* value than the profile's
    own, targeting the *same* host, has the identical presence shape as the
    correct resolution and would pass a presence-only check silently.
    Confirmed live (with synthetic values only, never this project's real
    credentials) before this fix, and covered by a regression test after
    it. Comparing values never requires displaying or logging them -- the
    comparison happens entirely in memory, and every error message below
    states only whether a mismatch occurred, never either value.
    """
    path = config_path or _default_databrickscfg_path()
    section = _read_profile_section(resolved_profile, path)
    if section is None:
        raise ProfileResolutionMismatchError(
            f"Could not independently verify profile {resolved_profile!r}: no such "
            f"profile section found in {path}. Refusing to trust the constructed "
            "client's resolution without this check -- this can happen if an "
            "ambient DATABRICKS_CONFIG_FILE environment variable redirected "
            "resolution to a different file than the canonical one."
        )

    expected_host = _normalize_host(section.get("host"))
    actual_host = _normalize_host(client.config.host)
    if expected_host and expected_host != actual_host:
        raise ProfileResolutionMismatchError(
            f"Profile {resolved_profile!r} declares host {expected_host!r} in "
            f"{path}, but the constructed client resolved host {actual_host!r} "
            "instead. Refusing to proceed -- this usually means an ambient "
            "DATABRICKS_HOST (or DATABRICKS_CONFIG_FILE) environment variable "
            "overrode the explicitly requested profile."
        )

    # (config_attr, human name, matching ambient env var(s) to name in the error)
    _CREDENTIAL_FIELDS = (
        ("token", "token", "DATABRICKS_TOKEN"),
        ("client_id", "client_id", "DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET"),
        ("client_secret", "client_secret", "DATABRICKS_CLIENT_SECRET"),
        ("username", "username", "DATABRICKS_USERNAME"),
        ("password", "password", "DATABRICKS_PASSWORD"),
    )
    for config_attr, human_name, env_hint in _CREDENTIAL_FIELDS:
        expected_value = section.get(config_attr) or None
        actual_value = getattr(client.config, config_attr, None) or None
        if expected_value != actual_value:
            raise ProfileResolutionMismatchError(
                f"Profile {resolved_profile!r}'s declared {human_name} in {path} does "
                f"not match what the constructed client actually resolved (values "
                "withheld from this message). Refusing to proceed -- this usually "
                f"means an ambient {env_hint} environment variable overrode the "
                "explicitly requested profile's own authentication."
            )


def get_workspace_client(profile: str | None = None) -> WorkspaceClient:
    """Build a WorkspaceClient using an explicitly chosen auth profile.

    Resolution order (first one found wins):
      1. the ``profile`` argument
      2. the ``DATABRICKS_CONFIG_PROFILE`` environment variable
      3. explicit ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` environment
         variables (the PAT pattern the Personal-workspace GitHub Actions
         workflow uses for CI)
      4. explicit ``DATABRICKS_HOST`` + ``DATABRICKS_AUTH_TYPE=azure-cli``
         (no token at all) -- the OIDC federated-identity pattern the
         Azure GitHub Actions workflow uses instead of a PAT, matching
         Lab 8's own already-configured ``azure/login`` + ``azure-cli``
         auth flow exactly (see .github/workflows/lab09_azure_deployment.yml
         and lab08_azure_prod_manual_run.yml). Recognizing this is safe for
         the same reason branch 3 already is: there is no local profile
         choice to override in a GitHub Actions runner (no ~/.databrickscfg
         exists there at all), so this can never re-create the ambient
         override risk ``_verify_profile_resolution`` exists to catch.

    If none of these are set, raises ``ProfileNotSpecifiedError`` instead of
    falling back to the SDK's own default-profile resolution, which could
    silently pick an unintended profile such as one of the "dev"-named
    profiles that actually point at Azure PROD.

    Reading the installed ``databricks-sdk``'s own ``Config`` resolution
    (``_load_from_env`` / ``_known_file_config_loader``) and confirming it
    empirically (with dummy values only) showed that ``WorkspaceClient(profile=...)``
    on its own does NOT protect a profile's own credentials from being
    silently overridden by an ambient environment variable -- the SDK loads
    env vars before the profile file and never overwrites an attribute env
    already populated. A stale ``DATABRICKS_TOKEN`` (e.g. exported for a
    different profile earlier in the same shell session) could otherwise
    silently win over an explicitly requested profile such as a dedicated
    OAuth profile, with no error and no log message.

    Rather than fighting this by suppressing environment variables (a
    process-global mutation that cannot be made safe against unrelated
    threads in the same process -- see ``_verify_profile_resolution``'s
    docstring for why that approach was replaced), a profile-based
    resolution here is independently verified against ~/.databrickscfg
    immediately after construction, raising ``ProfileResolutionMismatchError``
    loudly instead of silently proceeding if the outcome does not match
    what the profile itself declares.
    """
    resolved_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    has_host = bool(os.environ.get("DATABRICKS_HOST"))
    has_token = bool(os.environ.get("DATABRICKS_TOKEN"))
    has_azure_cli_auth = os.environ.get("DATABRICKS_AUTH_TYPE") == "azure-cli"

    if resolved_profile:
        ws_client = WorkspaceClient(profile=resolved_profile)
        _verify_profile_resolution(resolved_profile, ws_client)
        return ws_client
    if has_host and (has_token or has_azure_cli_auth):
        return WorkspaceClient()
    raise ProfileNotSpecifiedError(
        "No Databricks profile was specified. Pass --profile explicitly or "
        "set DATABRICKS_CONFIG_PROFILE (or DATABRICKS_HOST with DATABRICKS_TOKEN "
        "or DATABRICKS_AUTH_TYPE=azure-cli). LAB 09 refuses to guess, because "
        "this repository's ~/.databrickscfg has profiles named 'dev'/'AZURE_DEV' "
        "that resolve to the Azure PROD host used by Lab 8, not a separate dev "
        "workspace."
    )


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a LAB 09 YAML config file (e.g. ``config/dev.yml``)."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} did not parse to a mapping.")
    return data


def volume_root_path(cfg: dict[str, Any]) -> str:
    """``/Volumes/<catalog>/<schema>/<volume>`` for the configured Lab 9 volume."""
    return f"/Volumes/{cfg['catalog']}/{cfg['schema']}/{cfg['volume']}"


def trips_path(cfg: dict[str, Any]) -> str:
    return f"{volume_root_path(cfg)}/trips"


def reference_path(cfg: dict[str, Any]) -> str:
    return f"{volume_root_path(cfg)}/reference"

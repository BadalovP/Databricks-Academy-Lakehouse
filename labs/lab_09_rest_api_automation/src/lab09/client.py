"""Databricks SDK client construction and config loading for LAB 09."""

from __future__ import annotations

import contextlib
import os
import threading
from pathlib import Path
from typing import Any, Iterator

import yaml
from databricks.sdk import WorkspaceClient

# Every environment variable the installed databricks-sdk (pinned to
# ==0.133.0 -- see pyproject.toml) resolves into a Config attribute that
# determines WHO a client authenticates as or WHICH workspace/config file it
# resolves against, other than DATABRICKS_CONFIG_PROFILE itself (which maps
# to the same `profile` attribute this module already sets via an explicit
# kwarg -- kwargs are applied before env vars are loaded, and an attribute
# already set from a kwarg is never overwritten by _load_from_env(), so that
# one specific variable needs no suppression). Enumerated by reading
# databricks.sdk.config.Config.attributes() directly, not guessed from
# memory -- re-verify this list if the pinned SDK version ever changes.
#
# Deliberately excludes pure behavior/tuning knobs that do not change WHO or
# WHICH workspace is targeted (DATABRICKS_CLUSTER_ID/WAREHOUSE_ID/
# SERVERLESS_COMPUTE_ID, DATABRICKS_DEBUG_*, DATABRICKS_RATE_LIMIT,
# DATABRICKS_METADATA_SERVICE_URL, DATABRICKS_DISABLE_*,
# DATABRICKS_CLI_PATH) -- suppressing those would be scope creep unrelated
# to the credential-hijack risk this guards against.
_AUTH_IDENTITY_ENV_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_ACCOUNT_ID",
    "DATABRICKS_WORKSPACE_ID",
    "DATABRICKS_CLOUD",
    "DATABRICKS_DISCOVERY_URL",
    "DATABRICKS_TOKEN_AUDIENCE",
    "DATABRICKS_OIDC_TOKEN_ENV",
    "DATABRICKS_OIDC_TOKEN_FILEPATH",
    "DATABRICKS_OIDC_TOKEN_FILE",
    "DATABRICKS_USERNAME",
    "DATABRICKS_PASSWORD",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_CONFIG_FILE",
    "DATABRICKS_GOOGLE_SERVICE_ACCOUNT",
    "GOOGLE_CREDENTIALS",
    "DATABRICKS_AZURE_RESOURCE_ID",
    "ARM_USE_MSI",
    "ARM_CLIENT_SECRET",
    "ARM_CLIENT_ID",
    "ARM_TENANT_ID",
    "ARM_ENVIRONMENT",
    "DATABRICKS_AUTH_TYPE",
)

# Serializes the suppress/restore critical section below across threads in
# this process. This is a real but narrow guarantee: it only prevents two
# concurrent get_workspace_client() calls in this process from stepping on
# each other's suppression window. It cannot and does not protect against
# unrelated code elsewhere in the same process reading os.environ during
# that window -- os.environ is inherently process-global mutable state, and
# no in-process lock can make a third party's unsynchronized read of it
# safe. Lab 9's own call sites (cli.py's main(), one-off scripts) are all
# single-threaded with exactly one call each, so this is currently a
# defense-in-depth guarantee rather than one anything in this codebase
# actually depends on today.
_env_lock = threading.Lock()


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


@contextlib.contextmanager
def _env_suppressed(*names: str) -> Iterator[None]:
    """Temporarily unset the given environment variables, restoring them after.

    Never touches the real values beyond removing/restoring them in-process;
    nothing is logged or displayed. Holds _env_lock for the duration, so two
    concurrent calls from different threads in this process cannot interleave
    their suppress/restore windows -- see _env_lock's own comment for the
    precise (narrower-than-total) guarantee this does and does not provide.
    """
    with _env_lock:
        saved = {name: os.environ.pop(name, None) for name in names}
        try:
            yield
        finally:
            for name, value in saved.items():
                if value is not None:
                    os.environ[name] = value


def get_workspace_client(profile: str | None = None) -> WorkspaceClient:
    """Build a WorkspaceClient using an explicitly chosen auth profile.

    Resolution order (first one found wins):
      1. the ``profile`` argument
      2. the ``DATABRICKS_CONFIG_PROFILE`` environment variable
      3. explicit ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` environment
         variables (the pattern GitHub Actions uses for CI)

    If none of these are set, raises ``ProfileNotSpecifiedError`` instead of
    falling back to the SDK's own default-profile resolution, which could
    silently pick an unintended profile such as one of the "dev"-named
    profiles that actually point at Azure PROD.

    This order is enforced explicitly, not just documented: reading the
    installed ``databricks-sdk``'s own ``Config`` resolution
    (``_load_from_env`` / ``_known_file_config_loader``) and confirming it
    empirically (with dummy values only) showed that ``WorkspaceClient(profile=...)``
    on its own does NOT protect a profile's own credentials from being
    silently overridden by an ambient environment variable -- the SDK loads
    env vars before the profile file and never overwrites an attribute env
    already populated. A stale ``DATABRICKS_TOKEN`` (e.g. exported for a
    different profile earlier in the same shell session) would otherwise
    silently win over an explicitly requested profile such as a dedicated
    OAuth profile, with no error and no log message. This risk is not
    limited to host/token: ``DATABRICKS_CLIENT_ID``/``DATABRICKS_CLIENT_SECRET``
    can redirect authentication to an entirely different OAuth service
    principal, ``DATABRICKS_AUTH_TYPE`` can force a different auth mechanism
    than the profile's own, and ``DATABRICKS_CONFIG_FILE`` can make "the
    profile" resolve against a different file altogether -- see
    ``_AUTH_IDENTITY_ENV_VARS`` for the full, empirically-enumerated set this
    suppresses for the duration of the profile-based call only, restoring
    every one of them immediately afterward.
    """
    resolved_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    has_explicit_host_token = bool(os.environ.get("DATABRICKS_HOST")) and bool(
        os.environ.get("DATABRICKS_TOKEN")
    )

    if resolved_profile:
        with _env_suppressed(*_AUTH_IDENTITY_ENV_VARS):
            return WorkspaceClient(profile=resolved_profile)
    if has_explicit_host_token:
        return WorkspaceClient()
    raise ProfileNotSpecifiedError(
        "No Databricks profile was specified. Pass --profile explicitly or "
        "set DATABRICKS_CONFIG_PROFILE (or DATABRICKS_HOST/DATABRICKS_TOKEN). "
        "LAB 09 refuses to guess, because this repository's ~/.databrickscfg "
        "has profiles named 'dev'/'AZURE_DEV' that resolve to the Azure PROD "
        "host used by Lab 8, not a separate dev workspace."
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

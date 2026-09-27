"""Databricks SDK client construction and config loading for LAB 09."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Any, Iterator

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


@contextlib.contextmanager
def _env_suppressed(*names: str) -> Iterator[None]:
    """Temporarily unset the given environment variables, restoring them after.

    Never touches the real values beyond removing/restoring them in-process;
    nothing is logged or displayed.
    """
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
    on its own does NOT protect a profile's host/token from being silently
    overridden by an ambient ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` left set
    in the shell -- the SDK loads env vars before the profile file and never
    overwrites an attribute env already populated. A stale
    ``DATABRICKS_TOKEN`` (e.g. exported for a different profile earlier in
    the same shell session) would otherwise silently win over an explicitly
    requested profile such as a dedicated OAuth profile, with no error and
    no log message -- exactly the ambient-credential risk this function's
    own docstring already claimed not to have. Suppressing those two
    variables for the duration of this call only closes that gap.
    """
    resolved_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    has_explicit_host_token = bool(os.environ.get("DATABRICKS_HOST")) and bool(
        os.environ.get("DATABRICKS_TOKEN")
    )

    if resolved_profile:
        with _env_suppressed("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
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

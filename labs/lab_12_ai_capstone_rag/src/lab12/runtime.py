"""Runtime guards shared by the Databricks notebooks."""

from __future__ import annotations

import re
from typing import Any

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_host(host: str) -> str:
    value = host.strip().rstrip("/").lower()
    if value and not value.startswith("https://"):
        value = f"https://{value}"
    return value


def assert_workspace(expected_host: str, client: Any | None = None) -> str:
    """Refuse to run unless the notebook executes in the expected workspace.

    The bundle already pins a host per target; this second check stops a notebook copied or
    run by hand in the wrong workspace from writing tables or creating AI Search resources there.
    """
    expected = normalize_host(expected_host)
    if not expected:
        raise ValueError("expected_host is required so the notebook can verify its workspace.")
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    actual = normalize_host(str(client.config.host))
    if actual != expected:
        raise RuntimeError(f"Wrong workspace: running in {actual!r}, expected {expected!r}.")
    return actual


def flag(value: str) -> bool:
    """Parse a 'true'/'false' widget value strictly; anything else is an error, not False."""
    lowered = str(value).strip().lower()
    if lowered not in ("true", "false"):
        raise ValueError(f"Expected 'true' or 'false', got {value!r}.")
    return lowered == "true"


def identifier(value: str, label: str) -> str:
    cleaned = str(value).strip()
    if not _IDENTIFIER.fullmatch(cleaned):
        raise ValueError(f"Invalid {label}: {value!r}")
    return cleaned


def index_name(catalog: str, schema: str, explicit: str = "", table: str = "document_chunks") -> str:
    """The AI Search index lives beside its source table unless explicitly overridden."""
    if explicit.strip():
        parts = explicit.strip().split(".")
        if len(parts) != 3 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
            raise ValueError(f"Invalid index name: {explicit!r}")
        return explicit.strip()
    return f"{identifier(catalog, 'catalog')}.{identifier(schema, 'schema')}.{table}_index"


def source_table(catalog: str, schema: str, table: str = "document_chunks") -> str:
    return f"{identifier(catalog, 'catalog')}.{identifier(schema, 'schema')}.{table}"

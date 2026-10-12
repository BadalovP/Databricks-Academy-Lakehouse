"""Read-only Lakehouse Federation contract helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REQUIRED_MAINTENANCE_COLUMNS = frozenset(
    {"maintenance_id", "station_id", "status", "severity", "updated_at"}
)


@dataclass(frozen=True)
class FederationSource:
    catalog: str
    table: str

    @property
    def qualified_table(self) -> str:
        parts = [self.catalog, *self.table.split(".")]
        if len(parts) != 3 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
            raise ValueError("Federation source must be catalog.schema.table identifiers.")
        return ".".join(f"`{part}`" for part in parts)


def validate_maintenance_columns(columns: set[str]) -> None:
    missing = REQUIRED_MAINTENANCE_COLUMNS - columns
    if missing:
        raise ValueError(f"Federated maintenance source is missing columns: {sorted(missing)}")


def select_statement(source: FederationSource) -> str:
    """Return the only SQL shape this adapter issues against the external system."""
    return (
        "SELECT maintenance_id, station_id, status, severity, updated_at "
        f"FROM {source.qualified_table}"
    )

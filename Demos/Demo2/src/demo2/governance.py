"""Fail-closed governance semantics used by tests and SQL generation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def allowed_countries_for_user(
    username: str,
    mappings: Iterable[Mapping[str, Any]],
) -> set[str] | None:
    """Return None for explicit all-access, a country set, or empty for no access."""
    normalized_user = username.casefold()
    matched = [
        row for row in mappings if str(row.get("user_name", "")).casefold() == normalized_user
    ]
    if any(bool(row.get("all_access")) for row in matched):
        return None
    return {str(row["country"]) for row in matched if row.get("country")}


def filter_rows_for_user(
    rows: Iterable[Mapping[str, Any]],
    username: str,
    mappings: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    allowed = allowed_countries_for_user(username, mappings)
    if allowed is None:
        return [dict(row) for row in rows]
    return [dict(row) for row in rows if row.get("country") in allowed]


def mask_pii(value: Any, *, can_view_pii: bool) -> Any:
    return value if can_view_pii else "***MASKED***"

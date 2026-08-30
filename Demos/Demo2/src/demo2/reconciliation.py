"""Medallion reconciliation helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def reconciliation_result(
    *,
    bronze_count: int,
    valid_count: int,
    warn_count: int,
    quarantine_count: int,
    fact_count: int,
    trusted_duplicate_count: int = 0,
    fact_duplicate_count: int = 0,
    orphan_customer_count: int = 0,
    orphan_product_count: int = 0,
    orphan_date_count: int = 0,
) -> dict[str, Any]:
    trusted_count = valid_count + warn_count
    checks = {
        "bronze_equals_trusted_plus_quarantine": bronze_count == trusted_count + quarantine_count,
        "trusted_equals_fact": trusted_count == fact_count,
        "trusted_duplicates_zero": trusted_duplicate_count == 0,
        "fact_duplicates_zero": fact_duplicate_count == 0,
        "orphan_customer_zero": orphan_customer_count == 0,
        "orphan_product_zero": orphan_product_count == 0,
        "orphan_date_zero": orphan_date_count == 0,
    }
    return {
        "bronze_count": bronze_count,
        "valid_count": valid_count,
        "warn_count": warn_count,
        "trusted_count": trusted_count,
        "quarantine_count": quarantine_count,
        "fact_count": fact_count,
        "checks": checks,
        "passed": all(checks.values()),
    }


def duplicate_key_count(rows: Iterable[Mapping[str, Any]], key: str) -> int:
    seen: set[Any] = set()
    duplicates: set[Any] = set()
    for row in rows:
        value = row.get(key)
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return len(duplicates)

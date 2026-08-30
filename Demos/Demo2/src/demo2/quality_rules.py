"""Deterministic RetailPulse order classification."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from demo2.config import DEMO_AS_OF_TIMESTAMP, VALID_DISCOUNT_MAX, WARN_DISCOUNT_MAX
from demo2.hashing import canonical_order_hash


def _timestamp(value: Any) -> datetime:
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def is_future_order(value: Any) -> bool:
    return _timestamp(value) > _timestamp(DEMO_AS_OF_TIMESTAMP)


def discount_status(value: Any) -> str:
    if value is None:
        return "QUARANTINE"
    discount = Decimal(str(value))
    if discount > WARN_DISCOUNT_MAX:
        return "QUARANTINE"
    if discount > VALID_DISCOUNT_MAX:
        return "WARN"
    return "VALID"


def _ranking_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("_source_generated_at") or ""),
        str(row.get("_source_file") or ""),
        str(row.get("_row_hash") or canonical_order_hash(row)),
    )


def rank_duplicate_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    materialized = [deepcopy(dict(row)) for row in rows]
    groups: dict[Any, list[dict[str, Any]]] = {}
    for row in materialized:
        row["_row_hash"] = row.get("_row_hash") or canonical_order_hash(row)
        groups.setdefault(row.get("order_line_id"), []).append(row)
    ranked: list[dict[str, Any]] = []
    for group in groups.values():
        for rank, row in enumerate(sorted(group, key=_ranking_key, reverse=True), start=1):
            row["_duplicate_rank"] = rank
            ranked.append(row)
    return ranked


def classify_order_records(
    rows: Iterable[Mapping[str, Any]],
    *,
    customer_ids: set[str],
    product_ids: set[str],
) -> list[dict[str, Any]]:
    classified: list[dict[str, Any]] = []
    for row in rank_duplicate_rows(rows):
        quarantine: list[str] = []
        warnings: list[str] = []
        dimensions: list[str] = []

        if row["_duplicate_rank"] > 1:
            quarantine.append("DUPLICATE_ORDER_LINE_ID")
            dimensions.append("uniqueness")
        customer_id = row.get("customer_id")
        product_id = row.get("product_id")
        if not customer_id:
            quarantine.append("CUSTOMER_ID_MISSING")
            dimensions.append("completeness")
        elif customer_id not in customer_ids:
            quarantine.append("UNKNOWN_CUSTOMER_ID")
            dimensions.append("referential_integrity")
        if not product_id:
            quarantine.append("PRODUCT_ID_MISSING")
            dimensions.append("completeness")
        elif product_id not in product_ids:
            quarantine.append("UNKNOWN_PRODUCT_ID")
            dimensions.append("referential_integrity")
        quantity = row.get("quantity")
        if quantity is None or int(quantity) <= 0:
            quarantine.append("NON_POSITIVE_QUANTITY")
            dimensions.append("validity")
        discount_result = discount_status(row.get("discount_pct"))
        if discount_result == "WARN":
            warnings.append("HIGH_DISCOUNT")
            dimensions.append("validity")
        elif discount_result == "QUARANTINE":
            quarantine.append("INVALID_DISCOUNT")
            dimensions.append("validity")
        if is_future_order(row.get("order_timestamp")):
            quarantine.append("FUTURE_ORDER_TIMESTAMP")
            dimensions.append("timeliness")

        status = "QUARANTINE" if quarantine else ("WARN" if warnings else "VALID")
        quantity_decimal = Decimal(str(quantity or 0))
        unit_price = Decimal(str(row.get("unit_price") or 0))
        discount = Decimal(str(row.get("discount_pct") or 0))
        gross = (quantity_decimal * unit_price).quantize(Decimal("0.01"))
        discount_amount = (gross * discount).quantize(Decimal("0.01"))
        result = {
            **row,
            "_dq_status": status,
            "_dq_warn_reasons": warnings,
            "_dq_quarantine_reasons": quarantine,
            "_dq_dimensions": list(dict.fromkeys(dimensions)),
            "gross_amount": gross,
            "discount_amount": discount_amount,
            "net_amount": (gross - discount_amount).quantize(Decimal("0.01")),
        }
        classified.append(result)
    return classified


def status_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    materialized = list(rows)
    counts = Counter(str(row["_dq_status"]) for row in materialized)
    return {
        "total": len(materialized),
        "VALID": counts["VALID"],
        "WARN": counts["WARN"],
        "QUARANTINE": counts["QUARANTINE"],
    }

"""Canonical business-row hashing shared by generators and tests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Mapping

from demo2.config import ORDER_BUSINESS_FIELDS


def _canonical_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical_decimal(value: Any, scale: int) -> str | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc
    quantum = Decimal(1).scaleb(-scale)
    return format(decimal_value.quantize(quantum, rounding=ROUND_HALF_UP), f".{scale}f")


def normalize_order_business_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize exactly the locked business fields in their locked order."""
    normalized: dict[str, Any] = {}
    for field in ORDER_BUSINESS_FIELDS:
        value = row.get(field)
        if field == "quantity":
            normalized[field] = None if value is None else int(value)
        elif field == "unit_price":
            normalized[field] = _canonical_decimal(value, 2)
        elif field == "discount_pct":
            normalized[field] = _canonical_decimal(value, 4)
        elif field == "order_timestamp":
            normalized[field] = _canonical_timestamp(value)
        else:
            normalized[field] = value
    return normalized


def canonical_order_json(row: Mapping[str, Any]) -> str:
    return json.dumps(
        normalize_order_business_row(row),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=False,
    )


def canonical_order_hash(row: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_order_json(row).encode("utf-8")).hexdigest()

"""Frozen business constants and fail-closed runtime configuration checks."""

from __future__ import annotations

from decimal import Decimal

SUPPORTED_BUNDLE_TARGETS = frozenset({"azure_dev", "azure_prod"})


def validate_runtime_configuration(
    *,
    bundle_target: str,
    catalog: str,
    schema: str,
    volume_name: str,
    expected_catalog: str,
    expected_schema: str,
    expected_volume_name: str,
) -> None:
    """Require a supported target and an exact match to its bundle configuration."""
    values = {
        "bundle_target": bundle_target,
        "catalog": catalog,
        "schema": schema,
        "volume_name": volume_name,
        "expected_catalog": expected_catalog,
        "expected_schema": expected_schema,
        "expected_volume_name": expected_volume_name,
    }
    missing = sorted(name for name, value in values.items() if not value or not value.strip())
    if missing:
        raise RuntimeError(f"Missing required Demo 2 configuration: {', '.join(missing)}")

    if bundle_target not in SUPPORTED_BUNDLE_TARGETS:
        raise RuntimeError(f"Unsupported Demo 2 bundle target: {bundle_target}")

    actual = (catalog, schema, volume_name)
    expected = (expected_catalog, expected_schema, expected_volume_name)
    if actual != expected:
        raise RuntimeError(
            "Demo 2 runtime configuration does not match the explicit bundle target: "
            f"received={actual!r}, expected={expected!r}"
        )


DEMO_AS_OF_TIMESTAMP = "2026-09-01T12:00:00Z"
V1_BATCH_ID = "DEMO2_V1_INITIAL"
V1_BATCH_LOADED_AT = "2026-09-01T09:00:00Z"
V2_BATCH_ID = "DEMO2_V2_SCHEMA_EVOLUTION"
V2_BATCH_LOADED_AT = "2026-09-01T10:00:00Z"

ORDER_BUSINESS_FIELDS = (
    "order_line_id",
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "order_timestamp",
    "order_status",
    "sales_channel",
    "coupon_code",
)

V1_ORDER_FIELDS = ORDER_BUSINESS_FIELDS[:9]
EVOLVED_ORDER_FIELDS = ORDER_BUSINESS_FIELDS[9:]

VALID_DISCOUNT_MAX = Decimal("0.30")
WARN_DISCOUNT_MAX = Decimal("0.50")
ALERT_QUARANTINE_RATE_PCT = Decimal("5.00")

EXPECTED_V2_COUNTS = {
    "total": 100,
    "VALID": 92,
    "WARN": 2,
    "QUARANTINE": 6,
}

SCD2_TRACKED_COLUMNS = (
    "customer_name",
    "email",
    "country",
    "city",
    "loyalty_tier",
)

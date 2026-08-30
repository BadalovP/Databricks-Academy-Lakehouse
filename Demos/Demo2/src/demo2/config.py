"""Frozen Demo 2 business and runtime constants."""

from __future__ import annotations

from decimal import Decimal

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"
VOLUME_NAME = "demo2_ecommerce"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

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

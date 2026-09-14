"""
LAB 08 - TravelOps

Module:
src/travelops/config.py

Purpose:
Centralizes deterministic names and source-table metadata for the TravelOps CI/CD
lakehouse.

Responsibilities:
- define the official source tables discovered in samples.wanderbricks
- define Bronze/Silver/Gold dataset names used by tests and documentation
- provide deterministic raw landing subpaths for seed logic

Inputs:
No runtime inputs are read in local tests. Databricks notebooks receive target
catalog/schema and raw Volume catalog/schema/name from bundle job parameters.

Outputs:
Pure Python constants and helper return values.

Idempotency:
All helpers are deterministic and side-effect free.

Environment behavior:
The same logical source list is used in personal_dev, personal_prod and
azure_prod. Personal raw Volume schemas intentionally differ from application
target schemas.
"""

from __future__ import annotations

from dataclasses import dataclass

SOURCE_CATALOG = "samples"
SOURCE_SCHEMA = "wanderbricks"


@dataclass(frozen=True)
class SourceTable:
    """Describes one governed source table and its deterministic raw landing folder."""

    name: str
    primary_key: str
    raw_folder: str

    @property
    def full_name(self) -> str:
        """Return the fully-qualified Unity Catalog source table name."""

        return f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{self.name}"


SOURCE_TABLES: tuple[SourceTable, ...] = (
    SourceTable("bookings", "booking_id", "bookings"),
    SourceTable("booking_updates", "booking_update_id", "booking_updates"),
    SourceTable("payments", "payment_id", "payments"),
    SourceTable("users", "user_id", "users"),
    SourceTable("properties", "property_id", "properties"),
    SourceTable("reviews", "review_id", "reviews"),
    SourceTable("destinations", "destination_id", "destinations"),
)

BRONZE_TABLES = tuple(f"{table.name}_bronze" for table in SOURCE_TABLES)
SILVER_TABLES = (
    "bookings_silver",
    "current_bookings_silver",
    "payments_silver",
    "users_silver",
    "properties_silver",
    "reviews_silver",
    "destinations_silver",
)
GOLD_TABLES = (
    "gold_daily_booking_revenue",
    "gold_property_performance",
    "gold_destination_performance",
    "gold_payment_reconciliation",
    "gold_review_score",
    "gold_production_health",
)


def raw_volume_path(catalog: str, schema: str, volume: str, folder: str) -> str:
    """Build the Databricks UC Volume path used by notebooks and Lakeflow code."""

    return f"/Volumes/{catalog}/{schema}/{volume}/raw/{folder}"

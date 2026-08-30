"""Deterministic, offline source data for RetailPulse."""

from __future__ import annotations

import csv
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping

from demo2.config import (
    V1_BATCH_ID,
    V1_BATCH_LOADED_AT,
    V2_BATCH_ID,
    V2_BATCH_LOADED_AT,
)


def customer_snapshots() -> dict[str, list[dict[str, object]]]:
    initial = [
        {
            "customer_id": "C001",
            "customer_name": "Anna Kowalska",
            "email": "anna.kowalska@example.com",
            "country": "PL",
            "city": "Warsaw",
            "loyalty_tier": "STANDARD",
        },
        {
            "customer_id": "C002",
            "customer_name": "Marek Nowak",
            "email": "marek.nowak@example.com",
            "country": "PL",
            "city": "Krakow",
            "loyalty_tier": "PREMIUM",
        },
        {
            "customer_id": "C003",
            "customer_name": "Sofia Rossi",
            "email": "sofia.rossi@example.com",
            "country": "IT",
            "city": "Milan",
            "loyalty_tier": "STANDARD",
        },
        {
            "customer_id": "C004",
            "customer_name": "Lukas Weber",
            "email": "lukas.weber@example.com",
            "country": "DE",
            "city": "Berlin",
            "loyalty_tier": "STANDARD",
        },
        {
            "customer_id": "C005",
            "customer_name": "Emma Dubois",
            "email": "emma.dubois@example.com",
            "country": "FR",
            "city": "Paris",
            "loyalty_tier": "PREMIUM",
        },
        {
            "customer_id": "C006",
            "customer_name": "Daniel Evans",
            "email": "daniel.evans@example.com",
            "country": "GB",
            "city": "London",
            "loyalty_tier": "STANDARD",
        },
        {
            "customer_id": "C007",
            "customer_name": "Lucia Garcia",
            "email": "lucia.garcia@example.com",
            "country": "ES",
            "city": "Madrid",
            "loyalty_tier": "STANDARD",
        },
        {
            "customer_id": "C008",
            "customer_name": "Nora Jensen",
            "email": "nora.jensen@example.com",
            "country": "DK",
            "city": "Copenhagen",
            "loyalty_tier": "PREMIUM",
        },
    ]
    current = deepcopy(initial)
    current[0]["loyalty_tier"] = "PREMIUM"
    current[2]["loyalty_tier"] = "PREMIUM"
    current[5]["city"] = "Manchester"
    return {"20260801": initial, "20260830": current}


def products() -> list[dict[str, object]]:
    return [
        {
            "product_id": "P001",
            "product_name": "Wireless Headphones",
            "category": "Electronics",
            "brand": "NovaSound",
            "unit_price": "89.90",
        },
        {
            "product_id": "P002",
            "product_name": "Smart Speaker",
            "category": "Electronics",
            "brand": "HomeWave",
            "unit_price": "129.00",
        },
        {
            "product_id": "P003",
            "product_name": "Running Shoes",
            "category": "Sports",
            "brand": "Stride",
            "unit_price": "74.50",
        },
        {
            "product_id": "P004",
            "product_name": "Yoga Mat",
            "category": "Sports",
            "brand": "Balance",
            "unit_price": "32.00",
        },
        {
            "product_id": "P005",
            "product_name": "Coffee Maker",
            "category": "Home",
            "brand": "BrewLab",
            "unit_price": "99.99",
        },
        {
            "product_id": "P006",
            "product_name": "Desk Lamp",
            "category": "Home",
            "brand": "Luma",
            "unit_price": "45.25",
        },
        {
            "product_id": "P007",
            "product_name": "Travel Backpack",
            "category": "Travel",
            "brand": "Roam",
            "unit_price": "68.00",
        },
        {
            "product_id": "P008",
            "product_name": "Insulated Bottle",
            "category": "Travel",
            "brand": "Arctic",
            "unit_price": "28.75",
        },
        {
            "product_id": "P009",
            "product_name": "Skin Care Set",
            "category": "Beauty",
            "brand": "PureGlow",
            "unit_price": "54.40",
        },
        {
            "product_id": "P010",
            "product_name": "Cotton Hoodie",
            "category": "Fashion",
            "brand": "Northline",
            "unit_price": "59.00",
        },
    ]


def _base_order(index: int, *, evolved: bool) -> dict[str, object]:
    product = products()[(index - 1) % len(products())]
    timestamp = datetime(2026, 8, 2, 8, tzinfo=timezone.utc) + timedelta(hours=index * 6)
    row: dict[str, object] = {
        "order_line_id": f"OL{index:04d}",
        "order_id": f"O{((index - 1) // 2) + 1:04d}",
        "customer_id": f"C{((index - 1) % 8) + 1:03d}",
        "product_id": product["product_id"],
        "quantity": (index % 3) + 1,
        "unit_price": product["unit_price"],
        "discount_pct": "0.10" if index % 4 == 0 else "0.00",
        "order_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "order_status": ("COMPLETED", "SHIPPED", "PROCESSING")[index % 3],
    }
    if evolved:
        row["sales_channel"] = ("WEB", "MOBILE", "MARKETPLACE")[index % 3]
        row["coupon_code"] = f"SAVE{index % 7:02d}"
    return row


def _with_source_metadata(
    row: dict[str, object],
    *,
    batch_id: str,
    batch_loaded_at: str,
    source_file: str,
    record_seq: int,
    generated_at: str,
) -> dict[str, object]:
    return {
        **row,
        "_source_batch_id": batch_id,
        "_batch_loaded_at": batch_loaded_at,
        "_source_generated_at": generated_at,
        "_source_file": source_file,
        "_source_record_seq": record_seq,
    }


def generate_v1_orders(count: int = 24) -> list[dict[str, object]]:
    rows = []
    for index in range(1, count + 1):
        row = _base_order(index, evolved=False)
        rows.append(
            _with_source_metadata(
                row,
                batch_id=V1_BATCH_ID,
                batch_loaded_at=V1_BATCH_LOADED_AT,
                source_file="orders_v1_20260901_090000.json",
                record_seq=index,
                generated_at=f"2026-09-01T09:{index:02d}:00Z",
            )
        )
    return rows


def generate_v2_orders() -> list[dict[str, object]]:
    rows = [_base_order(index, evolved=True) for index in range(1, 100)]
    for index, row in enumerate(rows, start=1):
        row["order_line_id"] = f"V2OL{index:03d}"
        row["order_id"] = f"V2O{((index - 1) // 2) + 1:03d}"

    rows[92]["discount_pct"] = "0.40"
    rows[93]["discount_pct"] = "0.50"
    rows[94]["customer_id"] = None
    rows[95]["product_id"] = "P999"
    rows[96]["quantity"] = -1
    rows[97]["discount_pct"] = "0.75"
    rows[98]["order_timestamp"] = "2026-09-01T12:00:01Z"

    physical = [
        _with_source_metadata(
            row,
            batch_id=V2_BATCH_ID,
            batch_loaded_at=V2_BATCH_LOADED_AT,
            source_file="orders_v2_20260901_100000.json",
            record_seq=index,
            generated_at=f"2026-09-01T10:{index // 60:02d}:{index % 60:02d}Z",
        )
        for index, row in enumerate(rows, start=1)
    ]
    duplicate_loser = deepcopy(physical[0])
    duplicate_loser["_source_file"] = "orders_v2_duplicate_20260901_095959.json"
    duplicate_loser["_source_record_seq"] = 100
    duplicate_loser["_source_generated_at"] = "2026-09-01T09:59:59Z"
    physical.append(duplicate_loser)
    return physical


def write_csv(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    materialized = list(rows)
    if not materialized:
        raise ValueError("Cannot write an empty CSV")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(materialized[0]))
        writer.writeheader()
        writer.writerows(materialized)


def write_json_lines(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=True, separators=(",", ":")))
            handle.write("\n")


def write_reference_data(root: Path) -> list[Path]:
    snapshots = customer_snapshots()
    paths = [
        root / "source/customers/customers_snapshot_2026_08_01.csv",
        root / "source/customers/customers_snapshot_2026_08_30.csv",
        root / "source/products/products.csv",
    ]
    write_csv(paths[0], snapshots["20260801"])
    write_csv(paths[1], snapshots["20260830"])
    write_csv(paths[2], products())
    return paths

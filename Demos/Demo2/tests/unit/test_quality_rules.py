from decimal import Decimal

from demo2.config import DEMO_AS_OF_TIMESTAMP
from demo2.data_generation import customer_snapshots, generate_v2_orders
from demo2.quality_rules import (
    classify_order_records,
    discount_status,
    is_future_order,
)


def _classified():
    customer_ids = {row["customer_id"] for rows in customer_snapshots().values() for row in rows}
    product_ids = {f"P{index:03d}" for index in range(1, 11)}
    return classify_order_records(
        generate_v2_orders(), customer_ids=customer_ids, product_ids=product_ids
    )


def test_discount_boundaries():
    assert discount_status(Decimal("0.30")) == "VALID"
    assert discount_status(Decimal("0.50")) == "WARN"
    assert discount_status(Decimal("0.51")) == "QUARANTINE"


def test_business_time_boundary():
    assert not is_future_order(DEMO_AS_OF_TIMESTAMP)
    assert is_future_order("2026-09-01T12:00:01Z")


def test_duplicate_winner_and_loser_are_both_preserved():
    duplicate_rows = [row for row in _classified() if row["order_line_id"] == "V2OL001"]
    assert len(duplicate_rows) == 2
    assert {row["_duplicate_rank"] for row in duplicate_rows} == {1, 2}
    winner = next(row for row in duplicate_rows if row["_duplicate_rank"] == 1)
    loser = next(row for row in duplicate_rows if row["_duplicate_rank"] == 2)
    assert winner["_dq_status"] == "VALID"
    assert loser["_dq_status"] == "QUARANTINE"
    assert "DUPLICATE_ORDER_LINE_ID" in loser["_dq_quarantine_reasons"]


def test_amount_calculation():
    row = next(row for row in _classified() if row["_dq_status"] == "WARN")
    assert row["net_amount"] == row["gross_amount"] - row["discount_amount"]

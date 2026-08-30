import json

from demo2.config import EXPECTED_V2_COUNTS
from demo2.data_generation import customer_snapshots, generate_v1_orders, generate_v2_orders
from demo2.quality_rules import classify_order_records, status_counts


def _references():
    snapshots = customer_snapshots()
    customer_ids = {row["customer_id"] for rows in snapshots.values() for row in rows}
    product_ids = {f"P{index:03d}" for index in range(1, 11)}
    return customer_ids, product_ids


def test_customer_population_is_stable_and_has_real_history():
    snapshots = customer_snapshots()
    first = {row["customer_id"]: row for row in snapshots["20260801"]}
    second = {row["customer_id"]: row for row in snapshots["20260830"]}
    assert first.keys() == second.keys()
    assert first["C001"]["loyalty_tier"] == "STANDARD"
    assert second["C001"]["loyalty_tier"] == "PREMIUM"


def test_v1_keys_are_genuinely_absent():
    rows = generate_v1_orders()
    assert rows
    assert all("sales_channel" not in row for row in rows)
    assert all("coupon_code" not in row for row in rows)
    encoded = json.dumps(rows[0])
    assert "sales_channel" not in encoded
    assert "coupon_code" not in encoded


def test_v2_has_exact_physical_and_business_counts():
    rows = generate_v2_orders()
    assert len(rows) == 100
    assert len({row["order_line_id"] for row in rows}) == 99
    assert all(row["sales_channel"] for row in rows)
    assert all(row["coupon_code"] for row in rows)


def test_v2_exact_92_2_6_classification():
    customers, products = _references()
    actual = status_counts(
        classify_order_records(generate_v2_orders(), customer_ids=customers, product_ids=products)
    )
    assert actual == EXPECTED_V2_COUNTS
    assert actual["total"] == actual["VALID"] + actual["WARN"] + actual["QUARANTINE"]

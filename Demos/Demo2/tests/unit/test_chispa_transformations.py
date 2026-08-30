from decimal import Decimal

import pytest
from chispa import assert_df_equality

from demo2.transformations import add_nullable_evolved_columns, normalize_orders

pytestmark = pytest.mark.spark


def test_v1_schema_normalization_adds_nullable_evolved_columns(spark):
    source = spark.createDataFrame(
        [
            (
                "OL1",
                2,
                "10.00",
                "0.10",
                "2026-08-01T10:00:00Z",
                "2026-09-01T09:00:00Z",
                "2026-09-01T08:59:00Z",
            )
        ],
        "order_line_id string, quantity int, unit_price string, discount_pct string, order_timestamp string, _batch_loaded_at string, _source_generated_at string",
    )
    actual = normalize_orders(source).select(
        "order_line_id", "quantity", "unit_price", "discount_pct", "sales_channel", "coupon_code"
    )
    expected = spark.createDataFrame(
        [("OL1", 2, Decimal("10.00"), Decimal("0.1000"), None, None)],
        "order_line_id string, quantity int, unit_price decimal(18,2), discount_pct decimal(9,4), sales_channel string, coupon_code string",
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_explicit_helper_is_idempotent_for_v2(spark):
    source = spark.createDataFrame(
        [("WEB", "SAVE10")],
        "sales_channel string, coupon_code string",
    )
    actual = add_nullable_evolved_columns(source)
    assert actual.columns == ["sales_channel", "coupon_code"]
    assert actual.collect()[0].asDict() == {"sales_channel": "WEB", "coupon_code": "SAVE10"}

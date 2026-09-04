"""DQX quality rules for Olist Gold tables."""

from typing import Any


REQUIRED_STRING_COLUMNS = [
    "order_item_sk",
    "order_id",
    "customer_sk",
    "product_sk",
    "seller_sk",
    "customer_id",
    "product_id",
    "seller_id",
    "order_status",
]


def build_gold_order_item_dqx_checks() -> list[dict[str, Any]]:
    """Return DQX checks for gold_fact_order_items."""

    checks = [
        {
            "name": f"{column}_required",
            "criticality": "error",
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {
                    "column": column,
                    "trim_strings": True,
                },
            },
        }
        for column in REQUIRED_STRING_COLUMNS
    ]

    checks.extend(
        [
            {
                "name": "date_sk_required",
                "criticality": "error",
                "check": {
                    "function": "is_not_null",
                    "arguments": {
                        "column": "date_sk",
                    },
                },
            },
            {
                "name": "valid_price_range",
                "criticality": "error",
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": "price",
                        "min_limit": 0,
                        "max_limit": 100000,
                    },
                },
            },
            {
                "name": "valid_freight_range",
                "criticality": "error",
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": "freight_value",
                        "min_limit": 0,
                        "max_limit": 100000,
                    },
                },
            },
            {
                "name": "valid_item_total_range",
                "criticality": "error",
                "check": {
                    "function": "is_in_range",
                    "arguments": {
                        "column": "item_total_value",
                        "min_limit": 0,
                        "max_limit": 200000,
                    },
                },
            },
            {
                "name": "unique_order_item_surrogate_key",
                "criticality": "error",
                "check": {
                    "function": "is_unique",
                    "arguments": {
                        "columns": ["order_item_sk"],
                    },
                },
            },
        ]
    )

    return checks
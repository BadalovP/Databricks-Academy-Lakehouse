"""Unit tests for Olist DQX rule configuration."""

from src.dqx_rules import (
    build_gold_order_item_dqx_checks,
)


def test_gold_order_item_dqx_rules():
    """Protect the Gold contract from missing or non-critical DQX rules."""

    checks = build_gold_order_item_dqx_checks()

    rule_names = {
        rule["name"]
        for rule in checks
    }

    assert len(checks) == 14

    assert "order_item_sk_required" in rule_names
    assert "valid_price_range" in rule_names
    assert "valid_freight_range" in rule_names
    assert "valid_item_total_range" in rule_names
    assert "unique_order_item_surrogate_key" in rule_names

    assert all(
        rule["criticality"] == "error"
        for rule in checks
    )
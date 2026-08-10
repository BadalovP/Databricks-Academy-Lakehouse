from pathlib import Path

import yaml


LAB_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = LAB_ROOT / "contracts"


def load_contract(version: str):
    path = CONTRACT_DIR / f"online_retail_{version}.yml"

    assert path.exists(), f"Missing contract file: {path}"

    with open(path, "r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    assert isinstance(contract, dict), f"{path.name} must contain a YAML mapping"
    assert "contract" in contract, f"{path.name} is missing the 'contract' section"
    assert "schema" in contract, f"{path.name} is missing the 'schema' section"
    assert "columns" in contract["schema"], (
        f"{path.name} is missing schema.columns"
    )

    return contract


def column_map(contract: dict):
    return {
        column["name"]: column
        for column in contract["schema"]["columns"]
    }


def test_contract_v1_loads():
    contract = load_contract("v1")

    assert contract["contract"]["version"] == 1
    assert contract["contract"]["name"] == "online_retail"


def test_contract_v2_loads():
    contract = load_contract("v2")

    assert contract["contract"]["version"] == 2
    assert contract["contract"]["name"] == "online_retail"


def test_v1_has_eight_source_columns():
    contract = load_contract("v1")

    assert len(contract["schema"]["columns"]) == 8


def test_v2_has_ten_columns():
    contract = load_contract("v2")

    assert len(contract["schema"]["columns"]) == 10


def test_v2_supersedes_v1():
    contract = load_contract("v2")

    assert contract["contract"]["supersedes"] == 1


def test_v2_adds_only_approved_columns():
    v1 = column_map(load_contract("v1"))
    v2 = column_map(load_contract("v2"))

    added_columns = set(v2) - set(v1)

    assert added_columns == {
        "loyalty_tier",
        "sales_channel",
    }


def test_quantity_is_widened():
    v1 = column_map(load_contract("v1"))
    v2 = column_map(load_contract("v2"))

    assert v1["Quantity"]["type"] == "integer"
    assert v2["Quantity"]["type"] == "long"


def test_v2_allowed_loyalty_values():
    v2 = column_map(load_contract("v2"))

    assert set(v2["loyalty_tier"]["allowed_values"]) == {
        "STANDARD",
        "SILVER",
        "GOLD",
    }


def test_v2_allowed_sales_channels():
    v2 = column_map(load_contract("v2"))

    assert set(v2["sales_channel"]["allowed_values"]) == {
        "ONLINE",
        "MARKETPLACE",
        "DIRECT",
    }

from lab07.contracts import load_contract


def test_contract_key(project_root=None):
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    c = load_contract(root / "contracts" / "business_license_silver_v1.yml")
    assert c["scd_key_columns"] == ["license_number"]

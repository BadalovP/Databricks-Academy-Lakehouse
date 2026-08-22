"""Static contract tests for Lab 06 External V2 secure-view governance."""

import json
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = LAB_ROOT / "notebooks" / "lab06e_08_governance.ipynb"


def _code() -> str:
    payload = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in payload["cells"]
        if cell.get("cell_type") == "code"
    )


def test_governance_targets_external_v2_schema_and_secure_views():
    code = _code()

    assert '"target_schema"' in code
    assert '"parvinbadalov_lab06_ext"' in code
    assert 'fact_encounters = f"{target_schema_fqn}.fact_encounters"' in code
    assert 'dim_patient = f"{target_schema_fqn}.dim_patient"' in code
    assert "vw_fact_encounters_secure" in code
    assert "vw_dim_patient_secure" in code


def test_rls_is_implemented_in_secure_view():
    code = _code()

    assert "CREATE OR REPLACE VIEW {secure_fact_view}" in code
    assert "SESSION_USER()" in code
    assert "organization_id = '*'" in code
    assert "CAST(f.organization_id AS STRING)" in code
    assert "lab06_user_organization_access" in code


def test_cls_is_implemented_in_secure_view():
    code = _code()

    for column in ["ssn", "first_name", "last_name", "address"]:
        assert f'"{column}"' in code

    assert "CREATE OR REPLACE VIEW {secure_patient_view}" in code
    assert "***MASKED***" in code
    assert "lab06_patient_data_privileged_users" in code


def test_native_policies_are_not_attached_to_base_tables():
    code = _code()

    assert "SET ROW FILTER" not in code
    assert "SET MASK" not in code
    assert "DROP ROW FILTER" in code
    assert "DROP MASK" in code


def test_demo_restores_pipeline_identity_and_does_not_write_gold_data():
    code = _code()

    assert "VALUES ('{escaped_user}', '*')" in code
    assert "Current user restored to full / privileged access." in code
    assert "overwrite_external_delta(" not in code
    assert ".save(" not in code

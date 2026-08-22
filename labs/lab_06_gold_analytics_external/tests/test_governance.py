"""Static contract tests for Lab 06 External V2 governance."""

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


def test_governance_targets_external_v2_schema():
    code = _code()
    assert 'target_schema", "parvinbadalov_lab06_ext"' in code
    assert 'fact_encounters = f"{target_schema_fqn}.fact_encounters"' in code
    assert 'dim_patient = f"{target_schema_fqn}.dim_patient"' in code


def test_rls_uses_organization_row_filter():
    code = _code()
    assert "CREATE OR REPLACE FUNCTION {org_filter_function}" in code
    assert "SESSION_USER()" in code
    assert "SET ROW FILTER {org_filter_function}" in code
    assert "ON (organization_id)" in code
    assert "organization_id = '*'" in code


def test_cls_masks_patient_sensitive_columns():
    code = _code()
    for column in ["ssn", "first_name", "last_name", "address"]:
        assert f'"{column}"' in code

    assert "CREATE OR REPLACE FUNCTION {mask_function}" in code
    assert "ELSE '***MASKED***'" in code
    assert "SET MASK {mask_function}" in code


def test_demo_restores_full_access_for_job_identity():
    code = _code()
    assert "'*'," in code
    assert "full organization access" in code
    assert "patient-data privileged access" in code


def test_governance_is_not_embedded_in_gold_data_write_logic():
    code = _code()
    assert "overwrite_external_delta(" not in code
    assert ".save(" not in code

"""The secret scan and the change-scope check that CI and the release workflow run."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scan = _load("scan_secrets")
scope = _load("check_change_scope")

# Credential-shaped values assembled at runtime so this file never contains one literally.
_HEX32 = "0123456789abcdef" * 2


@pytest.mark.parametrize(
    "line, rule",
    [
        ("token = " + "dapi" + _HEX32, "databricks_pat"),
        ("secret: " + "dose" + _HEX32, "databricks_oauth_secret"),
        ("ghp_" + "A" * 36, "github_token"),
        ("AKIA" + "ABCDEFGHIJKLMNOP", "aws_access_key"),
        ("-----BEGIN " + "RSA PRIVATE KEY-----", "private_key"),
        (
            "Endpoint=sb://x/;SharedAccessKey" + "=" + "a" * 43 + "=",
            "azure_key_in_connection_string",
        ),
        ("abc" + "8Q~" + "b" * 34, "azure_ad_client_secret"),
        ("eyJ" + "a" * 12 + ".eyJ" + "b" * 12 + "." + "c" * 12, "jwt"),
    ],
)
def test_each_credential_format_is_detected(line, rule) -> None:
    assert (1, rule) in scan.scan_text("first line\n" + line) or (2, rule) in scan.scan_text(
        "first line\n" + line
    )


@pytest.mark.parametrize(
    "line",
    [
        "DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}",
        "SharedAccessKey=<from the Key Vault-backed secret scope>",
        "databricks_pat placeholder dapi<redacted>",
        "run 343513312512955 and update d5e0e599-c195-4c08-8217-d554ebf2b301",
    ],
)
def test_references_and_placeholders_are_not_findings(line) -> None:
    assert scan.scan_text(line) == []


def test_the_scan_covers_the_final_project_and_its_two_workflows_only() -> None:
    assert scan.SCANNED_PATHS == (
        "final_project",
        ".github/workflows/final_project_ci.yml",
        ".github/workflows/final_project_cd.yml",
    )
    files = scan.tracked_files()
    assert any(path.name == "final_project_cd.yml" for path in files)
    assert all(
        path.relative_to(scan.REPOSITORY_ROOT).as_posix().startswith(("final_project/", ".github/"))
        for path in files
    )


def test_the_tracked_tree_has_no_credential_shaped_value() -> None:
    assert scan.main() == 0


def test_only_final_project_paths_and_its_workflows_are_in_scope() -> None:
    assert scope.out_of_scope(
        [
            "final_project/src/urbanflow/cdc_sql.py",
            ".github/workflows/final_project_cd.yml",
            "Demos/Demo3/notebooks/x.py",
            "labs/lab_08_cicd_travelops/README.md",
            ".github/workflows/lab08_cicd.yml",
            ".github/terraform/lab10-azurerm-backend.tf",
        ]
    ) == [
        "Demos/Demo3/notebooks/x.py",
        "labs/lab_08_cicd_travelops/README.md",
        ".github/workflows/lab08_cicd.yml",
        ".github/terraform/lab10-azurerm-backend.tf",
    ]


@pytest.mark.parametrize(
    "path, deployable",
    [
        ("final_project/docs/ARCHITECTURE.md", False),
        ("final_project/README.md", False),
        ("final_project/evidence/2026-10-12_personal_dev_step4_batch003.json", False),
        ("final_project/tests/test_cdc_pipeline.py", False),
        (".github/workflows/final_project_cd.yml", False),
        ("final_project/notebooks/11_federation_cdc.py", True),
        ("final_project/src/urbanflow/cdc_sql.py", True),
        ("final_project/resources/urbanflow_final.job.yml", True),
        ("final_project/knowledge_base/urbanflow_rebalancing_procedure.md", True),
    ],
)
def test_documentation_evidence_and_tests_alone_never_need_a_release(path, deployable) -> None:
    assert scope.is_deployable(path) is deployable

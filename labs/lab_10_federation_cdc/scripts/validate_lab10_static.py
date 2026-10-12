"""Offline Lab 10 contract, syntax, safety, and secret checks.

Each ``check_*`` function takes file contents and raises AssertionError on a violation, so the
tests in ``tests/test_validate_lab10_static.py`` can prove that unsafe changes are rejected.
``main()`` reads the real repository files and runs every check.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
REPO = LAB.parents[1]
NOTEBOOKS = LAB / "notebooks"
EXPECTED_HOST = "https://adb-7405618361895520.0.azuredatabricks.net"
FORBIDDEN_HOSTS = (
    "https://adb-7405604503619901.1.azuredatabricks.net",  # dbr_dev
    "https://adb-7405615123305702.2.azuredatabricks.net",  # dbr_dev_trial
)
EXPECTED_SUBJECT_ENVIRONMENT = "lab10-azure"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def notebook_source(name: str, raw: str) -> str:
    """Code of a Jupyter notebook; every code cell must compile and carry no outputs."""
    notebook = json.loads(raw)
    require(notebook.get("nbformat") == 4, f"{name}: expected nbformat 4")
    source_parts: list[str] = []
    for index, cell in enumerate(notebook.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        compile(source, f"{name}#cell-{index}", "exec")
        source_parts.append(source)
        require(not cell.get("outputs"), f"{name}#cell-{index}: outputs must be stripped")
    return "\n".join(source_parts)


def check_notebooks(sources: dict[str, str]) -> None:
    """``sources`` maps notebook file name to its code (see ``notebook_source``)."""
    require(len(sources) == 4, "Expected exactly four Lab 10 notebooks")

    setup = sources["00_setup.ipynb"]
    for forbidden in ("DROP TABLE", "TRUNCATE TABLE", "CREATE OR REPLACE TABLE"):
        require(forbidden not in setup.upper(), f"Setup contains destructive SQL: {forbidden}")
    for token in (
        "CREATE SCHEMA IF NOT EXISTS",
        "delta.enableChangeDataFeed = true",
        "FROM {FOREIGN_CATALOG}.public.lab10_customers",
        "VALUES ('{PIPELINE}', CAST(0 AS BIGINT)",
    ):
        require(token in setup, f"Setup contract is missing {token!r}")

    federation = sources["01_federation_validation.ipynb"]
    require("{FOREIGN_CATALOG}.public.lab10_customers" in federation, "Missing federated customers query")
    require("{FOREIGN_CATALOG}.public.lab10_orders" in federation, "Missing federated orders query")
    require("LEFT JOIN {BASE}.customers_delta" in federation, "Missing Federation/Delta join")

    cdc = sources["02_incremental_cdc_sync.ipynb"]
    required_cdc = (
        "table_changes(",
        "'insert', 'update_postimage', 'delete'",
        "WHEN MATCHED AND s._change_type = 'delete'",
        "MERGE INTO {TARGET}",
        "MERGE INTO {HISTORY}",
        "assert differences == 0",
        "assert reverse_differences == 0",
        "MERGE INTO {CHECKPOINT}",
    )
    for token in required_cdc:
        require(token in cdc, f"CDC contract is missing {token!r}")
    checkpoint_position = cdc.index("MERGE INTO {CHECKPOINT}")
    require(
        cdc.index("assert differences == 0") < checkpoint_position
        and cdc.index("assert reverse_differences == 0") < checkpoint_position,
        "Checkpoint must advance only after two-way reconciliation",
    )

    final_validation = sources["03_final_validation.ipynb"]
    for token in (
        "VERSION AS OF {checkpoint}",
        "original_events.exceptAll",
        "recorded_events.exceptAll",
        '"source_target_differences"',
        "dbutils.notebook.exit",
    ):
        require(token in final_validation, f"Final validation contract is missing {token!r}")


def check_terraform(versions: str, main_tf: str) -> None:
    require(f'host = "{EXPECTED_HOST}"' in versions, "Azure provider host is not pinned")
    for forbidden_host in FORBIDDEN_HOSTS:
        require(forbidden_host not in versions, "Azure provider targets a forbidden workspace")
    code = re.sub(r"(?m)^\s*#.*$", "", main_tf)
    for forbidden in (
        r"\bexisting_cluster_id\s*=",
        r"\bnew_cluster\s*\{",
        r"\bjob_cluster_key\s*=",
        r"\bjob_cluster\s*\{",
        r"\bschedule\s*\{",
        r"\bcontinuous\s*\{",
    ):
        require(not re.search(forbidden, code), f"Azure Job violates safety contract: {forbidden}")
    task_blocks = re.findall(r"(?m)^\s{2}task\s*\{", code)
    require(len(task_blocks) == 3, "Azure Job must contain exactly three tasks")
    require(
        len(re.findall(r"(?m)^\s+disable_auto_optimization\s*=\s*true\b", code)) == 3,
        "Every Azure Job task must disable serverless auto-retry (disable_auto_optimization = true)",
    )
    for notebook in (
        "01_federation_validation.ipynb",
        "02_incremental_cdc_sync.ipynb",
        "03_final_validation.ipynb",
    ):
        require(notebook in main_tf, f"Azure Job is missing {notebook}")


def job_notebook_paths(main_tf: str) -> list[str]:
    """Repository-relative ``notebook_path`` values of a Job, with ``local.notebook_root`` resolved."""
    root = re.search(r'(?m)^\s*notebook_root\s*=\s*"([^"]+)"', main_tf)
    paths = re.findall(r'(?m)^\s*notebook_path\s*=\s*"([^"]+)"', main_tf)
    return [path.replace("${local.notebook_root}", root.group(1) if root else "${local.notebook_root}") for path in paths]


def check_job_notebook_paths(name: str, main_tf: str, existing: set[str]) -> None:
    """Every Job task must point at a notebook that exists in the repository (Git-sourced tasks)."""
    paths = job_notebook_paths(main_tf)
    require(len(paths) == 3, f"{name}: expected three notebook paths")
    for path in paths:
        require(path in existing, f"{name}: notebook_path {path!r} does not exist in the repository")


def check_workflows(cd: str, run: str, all_lab10_workflows: list[str]) -> None:
    for name, content in (("CD", cd), ("run", run)):
        require(EXPECTED_HOST in content, f"{name} workflow is not pinned to dbr_dev_trial_2")
        require(
            f"environment: {EXPECTED_SUBJECT_ENVIRONMENT}" in content,
            f"{name} workflow lacks {EXPECTED_SUBJECT_ENVIRONMENT}",
        )
        require("DATABRICKS_AUTH_TYPE: github-oidc" in content, f"{name} workflow lacks native OIDC")
        require("DATABRICKS_CLIENT_ID:" in content, f"{name} workflow lacks the deployer client ID")
        require("id-token: write" in content, f"{name} workflow cannot request a GitHub OIDC token")
        require("azure/login" not in content, f"{name} workflow must not reuse Azure subscription login")
        require("DATABRICKS_TOKEN" not in content, f"{name} workflow must not use a Databricks token")
        require("\n  push:" not in content, f"{name} workflow must not deploy on push")
        require("\n  pull_request:" not in content, f"{name} workflow must not deploy on pull request")
        require("terraform destroy" not in content.lower(), f"{name} workflow contains destroy")
        require("auto-approve" not in content.lower(), f"{name} workflow contains auto-approve")
        # The service principal's application ID is the SCIM userName; it must be checked.
        require(".userName" in content, f"{name} workflow does not verify the principal's application ID")
        for forbidden_host in FORBIDDEN_HOSTS:
            require(
                re.search(rf"FORBIDDEN_HOST_\w+:\s*{re.escape(forbidden_host)}", content) is not None,
                f"{name} workflow does not refuse {forbidden_host}",
            )
    require("APPLY-LAB10-AZURE-PROD" in cd, "CD apply confirmation is missing")
    require(
        "terraform apply -input=false -no-color lab10-azure-prod.tfplan" in cd,
        "CD must apply the saved plan",
    )
    require("HCP_TERRAFORM_TOKEN" in cd, "CD is missing HCP Terraform authentication")
    require("RUN-LAB10-AZURE-PROD" in run, "Run confirmation is missing")
    require("idempotency_token" in run, "Job execution must use an idempotency token")
    # Matches both "uses:" on its own line and the "- uses:" step shorthand.
    mutable_action = re.compile(r"(?m)^\s*(?:-\s+)?uses:\s*[^\s]+@(main|master|v\d+)\s*(?:#.*)?$")
    require(
        not any(mutable_action.search(content) for content in all_lab10_workflows),
        "Lab 10 workflows must pin third-party actions to immutable commit SHAs",
    )


SECRET_PATTERNS = {
    "Databricks PAT": re.compile(r"\bdapi[a-zA-Z0-9]{20,}\b"),
    "GitHub token": re.compile(r"\b(?:ghp|github_pat)_[a-zA-Z0-9_]{20,}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}


def find_secret_material(files: dict[str, str]) -> list[str]:
    """``files`` maps a display name to its text; returns one finding per match."""
    return [
        f"{name}: {label}"
        for name, text in files.items()
        for label, pattern in SECRET_PATTERNS.items()
        if pattern.search(text)
    ]


def _repository_texts() -> dict[str, str]:
    candidates = [
        *LAB.rglob("*"),
        *(REPO / ".github" / "workflows").glob("lab10_*.yml"),
    ]
    texts: dict[str, str] = {}
    for path in candidates:
        if not path.is_file() or any(part in {".terraform", "__pycache__"} for part in path.parts):
            continue
        if path.suffix in {".tfstate", ".pyc"} or ".tfstate." in path.name:
            continue
        texts[str(path.relative_to(REPO))] = path.read_text(encoding="utf-8", errors="ignore")
    return texts


def main() -> int:
    notebooks = {
        path.name: notebook_source(path.name, path.read_text(encoding="utf-8"))
        for path in sorted(NOTEBOOKS.glob("*.ipynb"))
    }
    check_notebooks(notebooks)
    azure = LAB / "terraform_azure_prod"
    check_terraform(
        (azure / "versions.tf").read_text(encoding="utf-8"),
        (azure / "main.tf").read_text(encoding="utf-8"),
    )
    existing = {path.relative_to(REPO).as_posix() for path in NOTEBOOKS.glob("*.ipynb")}
    for root in ("terraform", "terraform_azure_prod"):
        check_job_notebook_paths(root, (LAB / root / "main.tf").read_text(encoding="utf-8"), existing)
    workflow_dir = REPO / ".github" / "workflows"
    check_workflows(
        (workflow_dir / "lab10_azure_prod_cd.yml").read_text(encoding="utf-8"),
        (workflow_dir / "lab10_azure_prod_run.yml").read_text(encoding="utf-8"),
        [path.read_text(encoding="utf-8") for path in sorted(workflow_dir.glob("lab10_*.yml"))],
    )
    findings = find_secret_material(_repository_texts())
    require(not findings, "Potential secret material found:\n" + "\n".join(findings))
    print("PASS: Lab 10 offline syntax, CDC, Terraform, notebook path, workflow, and secret contracts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

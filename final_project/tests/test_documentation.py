from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent  # final_project/ sits directly under the repository root
LINK_PATTERN = re.compile(r"!?\[[^]]*\]\(([^)]+)\)")


def test_all_local_markdown_links_resolve() -> None:
    missing: list[str] = []
    for markdown_path in PROJECT_ROOT.rglob("*.md"):
        if {".venv", ".pytest_cache", "node_modules"} & set(markdown_path.parts):
            continue  # third-party packages, not project documentation
        text = markdown_path.read_text(encoding="utf-8")
        for target in LINK_PATTERN.findall(text):
            target = target.strip().strip("<>").split("#", maxsplit=1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (markdown_path.parent / unquote(target)).resolve()
            if not resolved.exists() or REPOSITORY_ROOT not in (resolved, *resolved.parents):
                missing.append(f"{markdown_path.relative_to(PROJECT_ROOT)} -> {target}")
    assert not missing, "Broken or out-of-repository links:\n" + "\n".join(missing)


def test_readme_contains_five_balanced_mermaid_diagrams() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    diagrams = re.findall(r"```mermaid\s+.*?```", readme, flags=re.DOTALL)
    assert len(diagrams) == 5


DOCS = PROJECT_ROOT / "docs"
REQUIRED_DOCS = (
    "LABS_1_TO_12_COVERAGE.md",
    "ARCHITECTURE.md",
    "COST_AND_PERFORMANCE.md",
    "RELEASE_AND_VALIDATION.md",
    "DASHBOARD.md",
    "DEMO_DAY_PRESENTATION.md",
)
# Demo 3 live identifiers and figures. Final Project docs must not present them as its own.
DEMO3_EVIDENCE = (
    "96337578882467",
    "fb8a0b8a-cdf8-45c4-bff6-2d117a516fb9",
    "ba6710ed",
    "991496516229387",
    "urbanflow-20260929T195132Z",
    "1,886,318",
    "1,888,085",
    "2,520",
    "3ed106620db591d9",
)


def _project_markdown() -> list[Path]:
    return [
        path
        for path in PROJECT_ROOT.rglob("*.md")
        if not {".venv", ".pytest_cache", "node_modules"} & set(path.parts)
    ]


def test_the_final_project_documentation_set_exists_and_is_linked_from_the_readme() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    assert readme.startswith("# UrbanFlow 2.0")
    for name in REQUIRED_DOCS:
        assert (DOCS / name).exists(), name
        assert f"docs/{name}" in readme, name


def test_no_document_presents_demo3_live_evidence() -> None:
    for path in _project_markdown():
        text = path.read_text(encoding="utf-8")
        for identifier in DEMO3_EVIDENCE:
            assert identifier not in text, f"{path.relative_to(PROJECT_ROOT)} names {identifier}"


def test_coverage_matrix_has_every_lab_and_claims_no_unrecorded_live_run() -> None:
    matrix = (DOCS / "LABS_1_TO_12_COVERAGE.md").read_text(encoding="utf-8")
    rows = [line for line in matrix.splitlines() if re.match(r"\| (\d+) \|", line)]
    assert [int(re.match(r"\| (\d+) \|", row).group(1)) for row in rows] == list(range(1, 13))
    for row in rows:
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        assert len(cells) == 7, row
        assert cells[5] or cells[6]
        assert "Validated live" not in cells[6], row  # no Final Project live run recorded yet
    release = (DOCS / "RELEASE_AND_VALIDATION.md").read_text(encoding="utf-8")
    assert "No live run yet" in release


def test_final_project_cd_is_manual_confirmed_approved_and_pinned() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/final_project_cd.yml").read_text(
        encoding="utf-8"
    )
    trigger = workflow[workflow.index("on:") : workflow.index("permissions:")]
    assert "workflow_dispatch:" in trigger
    for automatic in ("push:", "pull_request:", "schedule:"):
        assert automatic not in trigger
    assert 'expected="RELEASE_URBANFLOW_FINAL_${TARGET^^}"' in workflow
    assert "environment: final-project-approval" in workflow
    assert "environment: final-project-${{ inputs.target }}" in workflow
    assert "needs: approve-release" in workflow
    assert "--fail-on-active-runs" in workflow
    assert "--phase pre" in workflow and "--phase post" in workflow
    assert 'host="https://dbc-1750318a-76a9.cloud.databricks.com"' in workflow
    assert 'host="https://adb-7405618361895520.0.azuredatabricks.net"' in workflow
    assert "Refusing a shared Academy workspace" in workflow
    assert "DATABRICKS_TOKEN" not in workflow  # no personal access token
    for forbidden in (
        "bundle destroy",
        "pipelines delete",
        "jobs delete",
        "databricks permissions",
    ):
        assert forbidden not in workflow

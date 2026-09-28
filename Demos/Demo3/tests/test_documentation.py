from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
LINK_PATTERN = re.compile(r"!?\[[^]]*\]\(([^)]+)\)")


def test_all_local_markdown_links_resolve() -> None:
    missing: list[str] = []
    for markdown_path in PROJECT_ROOT.rglob("*.md"):
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


def test_static_workflow_has_no_cloud_execution_step() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/demo3_urbanflow.yml").read_text(
        encoding="utf-8"
    )
    forbidden = (
        "azure/login",
        "databricks bundle deploy",
        "databricks bundle run",
        "databricks jobs run",
        "databricks pipelines start-update",
    )
    for command in forbidden:
        assert command not in workflow
    assert "python scripts/validate_bundle.py" in workflow

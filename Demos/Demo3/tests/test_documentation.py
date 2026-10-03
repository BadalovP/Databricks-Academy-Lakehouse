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


def test_workflow_keeps_live_control_plane_validation_manually_gated_and_read_only() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/demo3_urbanflow.yml").read_text(
        encoding="utf-8"
    )
    forbidden = (
        "databricks bundle deploy",
        "databricks bundle run",
        "databricks jobs run",
        "databricks pipelines start-update",
        "databricks clusters start",
        "databricks clusters restart",
        "databricks clusters resize",
        "databricks clusters delete",
        "DATABRICKS_AZURE_TOKEN",
        "${{ secrets.",
    )
    for command in forbidden:
        assert command not in workflow

    assert "python scripts/validate_bundle.py" in workflow
    assert "run_live_control_plane" in workflow
    assert (
        workflow.count(
            "github.event_name == 'workflow_dispatch' && inputs.run_live_control_plane == true"
        )
        == 2
    )
    assert "environment: azure-release-approval" in workflow
    assert "needs: approve-live-control-plane" in workflow
    assert "environment: azure-prod" in workflow
    assert "azure/login@v2" in workflow
    assert "databricks current-user me" in workflow
    assert "databricks bundle validate -t azure" in workflow
    assert "databricks bundle plan" not in workflow
    assert "databricks jobs get" in workflow
    # The read-only path verifies the unified Job and the isolated pipeline, never the four
    # component Jobs retired on 2026-10-03.
    assert "databricks jobs get 991496516229387" in workflow
    assert "databricks pipelines get fb8a0b8a-cdf8-45c4-bff6-2d117a516fb9" in workflow
    assert "parvinbadalov_urbanflow_lakeflow" in workflow
    for retired in ("404404108673495", "11834365763936", "974964732439608", "860666167537092"):
        assert retired not in workflow
    for mutation in (
        "bundle deploy",
        "jobs run-now",
        "pipelines start-update",
        "databricks permissions",
    ):
        assert mutation not in workflow

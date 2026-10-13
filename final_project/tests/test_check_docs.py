"""The documentation checker finds real defects, and the committed docs have none."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_docs", Path(__file__).resolve().parents[1] / "scripts" / "check_docs.py"
)
docs = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = docs
_SPEC.loader.exec_module(docs)


def _write(tmp_path: Path, text: str) -> Path:
    (tmp_path / "exists.md").write_text("# Exists\n\n## Real Section\n", encoding="utf-8")
    page = tmp_path / "page.md"
    page.write_text(text, encoding="utf-8")
    return page


def test_broken_links_missing_anchors_and_remote_images_are_reported(tmp_path) -> None:
    page = _write(
        tmp_path,
        "# Page\n\n## Local Heading\n\n"
        "[ok](exists.md) [ok anchor](exists.md#real-section) [here](#local-heading)\n"
        "[broken](missing.md) [bad anchor](exists.md#nope) [bad local](#nowhere)\n"
        "![remote](https://example.com/x.png) ![badge](https://img.shields.io/badge/a-b-green)\n"
        "`[not a link](inside-code.md)`\n",
    )
    problems = docs.check_links(page)
    assert len(problems) == 4, problems
    assert any("broken link missing.md" in p for p in problems)
    assert any("missing anchor #nope" in p for p in problems)
    assert any("missing anchor #nowhere" in p for p in problems)
    assert any("remote image https://example.com/x.png" in p for p in problems)


def test_mermaid_blocks_need_a_known_type_and_balanced_brackets(tmp_path) -> None:
    page = _write(
        tmp_path,
        "```mermaid\nflowchart LR\n  A[ok] --> B[(db)]\n```\n\n"
        "```mermaid\nnotADiagram\n  A --> B\n```\n\n"
        "```mermaid\nflowchart LR\n  A[open --> B\n```\n",
    )
    problems = docs.check_mermaid(page)
    assert len(problems) == 2, problems
    assert "starts with 'notADiagram'" in problems[0]
    assert "unbalanced []" in problems[1]


def test_er_cardinality_markers_are_not_unbalanced_braces(tmp_path) -> None:
    page = _write(
        tmp_path,
        "```mermaid\nerDiagram\n  A ||--o{ B : has\n  B }|..|| C : uses\n"
        "  A {\n    string id PK\n  }\n```\n",
    )
    assert docs.check_mermaid(page) == []


def test_github_heading_slugs() -> None:
    assert docs.slug("🧭 Table of contents") == "-table-of-contents"
    assert docs.slug("10. CDC deep dive") == "10-cdc-deep-dive"
    assert docs.slug("CI/CD — protected `deploy`") == "cicd--protected-deploy"
    assert docs.slug("Labs 1–12") == "labs-112"


def test_duplicate_headings_get_numbered_anchors(tmp_path) -> None:
    page = tmp_path / "dup.md"
    page.write_text("# A\n\n## Notes\n\n## Notes\n\n```\n## Not a heading\n```\n", encoding="utf-8")
    assert docs.anchors(page) == {"a", "notes", "notes-1"}


def test_the_committed_documentation_passes() -> None:
    assert docs.main() == 0

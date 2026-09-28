from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_GUIDANCE = (
    "**What:**",
    "**Why:**",
    "**Input:**",
    "**Output:**",
    "**Key concepts:**",
    "**Expected result:**",
    "**How to explain it to my supervisor:**",
    "**Rerun and cost considerations:**",
)


def _cells(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# Databricks notebook source")
    return text.split("# COMMAND ----------")


def _is_markdown(cell: str) -> bool:
    content_lines = [
        line.strip()
        for line in cell.splitlines()
        if line.strip() and line.strip() != "# Databricks notebook source"
    ]
    return bool(content_lines) and content_lines[0] == "# MAGIC %md"


@pytest.mark.parametrize(
    "notebook",
    sorted(path.relative_to(PROJECT_ROOT) for path in (PROJECT_ROOT / "notebooks").glob("*.py")),
)
def test_every_code_cell_has_complete_preceding_markdown(notebook: Path) -> None:
    cells = _cells(PROJECT_ROOT / notebook)
    code_indexes = [index for index, cell in enumerate(cells) if not _is_markdown(cell)]

    assert code_indexes, f"{notebook} must contain code cells"
    for index in code_indexes:
        assert index > 0, f"{notebook} begins with an undocumented code cell"
        guidance = cells[index - 1]
        assert _is_markdown(
            guidance
        ), f"Code cell {index + 1} in {notebook} must be immediately preceded by Markdown"
        for heading in REQUIRED_GUIDANCE:
            assert heading in guidance, f"Code cell {index + 1} in {notebook} is missing {heading}"


@pytest.mark.parametrize(
    "notebook",
    sorted(path.relative_to(PROJECT_ROOT) for path in (PROJECT_ROOT / "notebooks").glob("*.py")),
)
def test_notebook_has_intro_and_teaching_summary(notebook: Path) -> None:
    text = (PROJECT_ROOT / notebook).read_text(encoding="utf-8")
    assert "**Prerequisites:**" in text
    assert "**Learning objectives:**" in text
    assert "## What we learned" in text
    assert "**Common errors:**" in text
    assert "**Review questions:**" in text
    assert "**Presentation summary:**" in text

"""Report documentation coverage for Demo2 Olist notebooks and tests.

The checker is intentionally read-only. It treats a Markdown cell immediately
before a non-empty code cell as documented, and uses the Python AST to verify
that every test function has a meaningful docstring.
"""

import ast
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def notebook_coverage(path: Path) -> tuple[int, int]:
    """Return non-empty code-cell and immediately documented-cell counts."""

    cells = json.loads(path.read_text(encoding="utf-8")).get("cells", [])
    code_cells = [
        index
        for index, cell in enumerate(cells)
        if cell.get("cell_type") == "code"
        and "".join(cell.get("source", [])).strip()
    ]
    documented = sum(
        bool(
            index > 0
            and cells[index - 1].get("cell_type") == "markdown"
            and "".join(cells[index - 1].get("source", [])).strip()
        )
        for index in code_cells
    )
    return len(code_cells), documented


def databricks_python_coverage(path: Path) -> tuple[int, int]:
    """Return executable source-cell and preceding ``%md`` counts."""

    text = path.read_text(encoding="utf-8")
    sections = re.split(r"(?m)^# COMMAND ----------\s*$", text)
    executable_indexes = []
    for index, section in enumerate(sections):
        lines = [
            line.strip()
            for line in section.splitlines()
            if line.strip()
            and not line.lstrip().startswith("#")
        ]
        if lines or any(
            re.match(r"\s*# MAGIC %(run|sql)\b", line)
            for line in section.splitlines()
        ):
            executable_indexes.append(index)

    documented = sum(
        index > 0 and "# MAGIC %md" in sections[index - 1]
        for index in executable_indexes
    )
    return len(executable_indexes), documented


def test_coverage(path: Path) -> tuple[int, int]:
    """Return test-function and meaningful-docstring counts."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    tests = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]
    documented = sum(bool(ast.get_docstring(node)) for node in tests)
    return len(tests), documented


def report() -> int:
    """Print one measurable row per applicable file and return failures."""

    failures = 0
    for path in sorted(ROOT.rglob("*.ipynb")):
        total, documented = notebook_coverage(path)
        if total:
            missing = total - documented
            failures += missing
            print(f"IPYNB|{path.relative_to(ROOT)}|{total}|{documented}|{missing}")

    for path in sorted(ROOT.rglob("*.py")):
        if not re.search(
            r"(?m)^# Databricks notebook source\s*$",
            path.read_text(encoding="utf-8"),
        ):
            continue
        total, documented = databricks_python_coverage(path)
        missing = total - documented
        failures += missing
        print(f"DBPY|{path.relative_to(ROOT)}|{total}|{documented}|{missing}")

    for path in sorted(ROOT.rglob("test_*.py")):
        total, documented = test_coverage(path)
        missing = total - documented
        failures += missing
        print(f"TEST|{path.relative_to(ROOT)}|{total}|{documented}|{missing}")

    return failures


if __name__ == "__main__":
    raise SystemExit(1 if report() else 0)
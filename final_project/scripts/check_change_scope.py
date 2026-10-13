"""Fail when a change set that touches the Final Project also touches anything outside it.

The Final Project must leave Demo 3, the completed labs and shared infrastructure byte-for-byte
unchanged, so a pull request may change only ``final_project/**``, the two Final Project
workflows and the repository homepage ``README.md``. It also classifies the change as deployable or not: documentation and evidence
alone never need a release.

Usage: ``python scripts/check_change_scope.py --base <sha> [--head <sha>]`` - exit 1 when a file
is out of scope. Prints ``deployable=true|false`` (also to ``$GITHUB_OUTPUT`` when set).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ALLOWED_FILES = (
    ".github/workflows/final_project_ci.yml",
    ".github/workflows/final_project_cd.yml",
    # The repository homepage presents the whole portfolio, including the Final Project.
    # It is the ONLY file outside final_project/ that a Final Project change may edit.
    "README.md",
)
# Exact paths outside final_project/ that never change a deployment.
NON_DEPLOYABLE_FILES = frozenset({"README.md"})
ALLOWED_PREFIX = "final_project/"
# Changes that never alter what a bundle deployment puts in a workspace.
NON_DEPLOYABLE_PREFIXES = (
    "final_project/docs/",
    "final_project/evidence/",
    "final_project/tests/",
    ".github/workflows/",
)


def out_of_scope(paths: list[str]) -> list[str]:
    return [
        path for path in paths if not (path.startswith(ALLOWED_PREFIX) or path in ALLOWED_FILES)
    ]


def is_deployable(path: str) -> bool:
    if path in NON_DEPLOYABLE_FILES or path.startswith(NON_DEPLOYABLE_PREFIXES):
        return False
    # Top-level Markdown (README) only; knowledge_base/*.md is RAG input and does deploy.
    relative = path.removeprefix(ALLOWED_PREFIX)
    return not (path.startswith(ALLOWED_PREFIX) and "/" not in relative and path.endswith(".md"))


def changed_files(base: str, head: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...{head}"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args(argv)
    paths = changed_files(args.base, args.head)
    outside = out_of_scope(paths)
    deployable = any(is_deployable(path) for path in paths)
    for path in outside:
        print(f"::error::outside the Final Project scope: {path}")
    print(f"{len(paths)} changed file(s); {len(outside)} outside the scope.")
    print(f"deployable={'true' if deployable else 'false'}")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
            handle.write(f"deployable={'true' if deployable else 'false'}\n")
    return 1 if outside else 0


if __name__ == "__main__":
    sys.exit(main())

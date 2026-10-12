"""Fail when a git-tracked Final Project file contains a credential-shaped value.

Runs in CI and in the release workflow before anything authenticates. It complements the
marker test in ``tests/test_evidence_paths_and_generator.py``: that test refuses connection-string
markers in evidence and docs; this scan looks for real credential formats in every tracked file
of the Final Project and its two workflows. Matches are reported by file, line and rule only -
the matched value is never printed.

Usage: ``python scripts/scan_secrets.py`` (from ``final_project/``) - exit 1 on any finding.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent
SCANNED_PATHS = (
    "final_project",
    ".github/workflows/final_project_ci.yml",
    ".github/workflows/final_project_cd.yml",
)
RULES: dict[str, re.Pattern[str]] = {
    "databricks_pat": re.compile(r"\bdapi[0-9a-f]{32}(?:-\d)?\b"),
    "databricks_oauth_secret": re.compile(r"\bdose[0-9a-f]{32}\b"),
    "github_token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})\b"),
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "private_key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "azure_key_in_connection_string": re.compile(
        r"(?:SharedAccessKey|AccountKey)=[A-Za-z0-9+/]{20,}={0,2}"
    ),
    "azure_ad_client_secret": re.compile(
        r"(?<![A-Za-z0-9])[A-Za-z0-9_.\-]{3}[78]Q~[A-Za-z0-9_.~\-]{31,34}"
    ),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
}


def scan_text(text: str) -> list[tuple[int, str]]:
    """``(line number, rule)`` for every credential-shaped value in ``text``."""
    findings = []
    for number, line in enumerate(text.splitlines(), start=1):
        for rule, pattern in RULES.items():
            if pattern.search(line):
                findings.append((number, rule))
    return findings


def tracked_files() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", *SCANNED_PATHS],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=True,
    ).stdout.decode("utf-8")
    return [REPOSITORY_ROOT / name for name in listed.split("\0") if name]


def main() -> int:
    files = tracked_files()
    if not files:
        print("No tracked files found; refusing a vacuous scan.", file=sys.stderr)
        return 1
    findings = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue  # binary files and files deleted in the working tree
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        findings.extend(f"{relative}:{line}: {rule}" for line, rule in scan_text(text))
    for finding in findings:
        print(f"::error::credential-shaped value at {finding}")
    print(f"Scanned {len(files)} tracked files; {len(findings)} finding(s).")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())

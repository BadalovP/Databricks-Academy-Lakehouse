"""No credentials or generated artifacts in the Lab 11 tree."""

from __future__ import annotations

import re
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".venv", ".pytest_cache", ".ruff_cache", "__pycache__", "build"}
PATTERNS = {
    "Databricks PAT": re.compile(r"\bdapi[0-9a-f]{32}\b"),
    "Databricks OAuth secret": re.compile(r"\bdose[0-9a-f]{32,}\b"),
    "GitHub token": re.compile(r"\b(?:ghp|gho|github_pat)_[A-Za-z0-9_]{20,}\b"),
    "private key": re.compile("-----BEGIN [A-Z ]*" + "PRIVATE KEY-----"),
    "assigned client secret": re.compile(
        r"(?i)client_secret\s*[=:]\s*[\"'][A-Za-z0-9._~-]{16,}[\"']"
    ),
}


def lab_files():
    for path in LAB.rglob("*"):
        if path.is_file() and not SKIP_DIRS.intersection(path.relative_to(LAB).parts):
            yield path


def test_no_secret_material_in_the_lab():
    findings = [
        f"{path.relative_to(LAB)}: {label}"
        for path in lab_files()
        for label, pattern in PATTERNS.items()
        if pattern.search(path.read_text(encoding="utf-8", errors="ignore"))
    ]
    assert findings == []


def test_the_patterns_do_catch_secrets():
    sample = "token dapi" + "0" * 32 + " dose" + "a" * 40
    assert PATTERNS["Databricks PAT"].search(sample) and PATTERNS["Databricks OAuth secret"].search(
        sample
    )


def test_no_env_files_or_local_state_are_part_of_the_lab():
    names = {path.name for path in lab_files()}
    assert ".env" not in names
    assert not any(name.endswith((".tfstate", ".pem", ".key")) for name in names)
    assert ".venv/" in (LAB / ".gitignore").read_text(encoding="utf-8")

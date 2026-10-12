"""Shared helpers for the Lab 11 scripts: import path, logging, evidence files."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
if str(LAB / "src") not in sys.path:
    sys.path.insert(0, str(LAB / "src"))

EVIDENCE = LAB / "evidence"


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_evidence(name: str, payload: dict) -> Path:
    """Write sanitized JSON evidence. Callers must never include credentials."""
    EVIDENCE.mkdir(exist_ok=True)
    path = EVIDENCE / name
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return path

"""JSON report generation and exit-code determination for LAB 09.

Every field is a real observed value from this run -- nothing here is
invented. Fields that were never populated (e.g. a NO_NEW_DATA run has no
month_landed, or an early failure never reached the reconciliation
notebook) are left as their explicit default (None/0/False) rather than
guessed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = (
    "status",
    "month",
    "month_landed",
    "file_bytes",
    "volume_path",
    "pipeline_id",
    "update_id",
    "cluster_id",
    "job_id",
    "run_id",
    "bronze_rows",
    "silver_valid_rows",
    "rejected_rows",
    "gold_rows",
    "failed_rules",
    "reconciliation_passed",
    "cluster_cleaned_up",
    "started_at",
    "finished_at",
    "duration_seconds",
)


@dataclass
class Report:
    status: str = "UNKNOWN"
    month: str | None = None
    month_landed: bool = False
    file_bytes: int = 0
    volume_path: str | None = None
    pipeline_id: str | None = None
    pipeline_serverless: bool | None = None
    update_id: str | None = None
    cluster_id: str | None = None
    job_id: int | None = None
    run_id: int | None = None
    bronze_rows: int | None = None
    silver_valid_rows: int | None = None
    rejected_rows: int | None = None
    gold_rows: int | None = None
    failed_rules: dict[str, int] = field(default_factory=dict)
    reconciliation_passed: bool | None = None
    cluster_cleaned_up: bool = False
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None
    error: str | None = None

    def mark_started(self) -> None:
        self.started_at = datetime.now(timezone.utc).isoformat()

    def mark_finished(self) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()
        if self.started_at:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.finished_at)
            self.duration_seconds = (end - start).total_seconds()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_report(report: Report, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = report.as_dict()
    for key in REQUIRED_FIELDS:
        if key not in payload:
            raise ValueError(f"Report is missing required field {key!r}.")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    logger.info("Wrote LAB 09 report to %s", path)
    return path


def exit_code_for(report: Report) -> int:
    """Non-zero whenever the run did not fully succeed.

    A NO_NEW_DATA status is not a failure by itself (Phase 4/15), but the
    pipeline/notebook/reconciliation steps still ran in that case and their
    outcome (reconciliation_passed) still determines the exit code.
    """
    if report.status not in ("SUCCESS", "NO_NEW_DATA"):
        return 1
    if report.reconciliation_passed is False:
        return 1
    if report.reconciliation_passed is None:
        # Reconciliation never ran/completed -- treat as failure rather than
        # silently reporting success without having actually verified it.
        return 1
    return 0

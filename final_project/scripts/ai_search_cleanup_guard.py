"""Temporary safety net: delete the UrbanFlow 2.0 DEV AI Search endpoint before it outlives its window.

The approved assistant evaluation creates ONE billable AI Search endpoint, which must be deleted
within three hours of creation even if the operator's session is interrupted. This script runs as
a small scheduled serverless Job in the same workspace, every 30 minutes, while the window lasts:

- endpoint present and at least ``max_age_minutes`` old (150), or the guard's own expiry passed:
  delete the index, then the endpoint. A 30-minute schedule therefore deletes it by 3 hours.
- endpoint absent and the expiry passed: pause this guard's schedule, so it stops running.
- otherwise: report and do nothing.

It can delete only the one endpoint and index named below; any other name is refused. The
operator deletes the guard Job after the verified cleanup.

Usage (Job parameters): ``--expires-at-ms <epoch ms> [--job-id {{job.id}}]``.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass

ENDPOINT = "urbanflow-final-dev"
INDEX = "workspace.urbanflow_final_dev.rag_document_chunks_index"
MAX_AGE_MINUTES = 150
SCHEDULE = "0 0/30 * * * ?"


@dataclass(frozen=True)
class Decision:
    action: str  # "delete", "pause" or "none"
    reason: str


def decide(
    *, endpoint_created_ms: int | None, now_ms: int, expires_at_ms: int,
    max_age_minutes: int = MAX_AGE_MINUTES,
) -> Decision:  # fmt: skip
    """What the guard does on one run; pure, so it is tested offline."""
    if endpoint_created_ms is None:
        if now_ms >= expires_at_ms:
            return Decision("pause", "endpoint absent and the guard window has expired")
        return Decision("none", "endpoint absent; the guard stays armed until its expiry")
    age_minutes = (now_ms - endpoint_created_ms) / 60_000
    if age_minutes >= max_age_minutes:
        return Decision(
            "delete", f"endpoint is {age_minutes:.0f} min old (limit {max_age_minutes})"
        )
    if now_ms >= expires_at_ms:
        return Decision("delete", "the guard window has expired while the endpoint still exists")
    return Decision("none", f"endpoint is {age_minutes:.0f} min old; within the window")


def _missing(error: Exception) -> bool:
    text = f"{type(error).__name__} {error}".lower()
    return any(marker in text for marker in ("not found", "notfound", "does not exist", "404"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expires-at-ms", type=int, required=True)
    parser.add_argument("--job-id", type=int, default=0)
    args = parser.parse_args(argv)

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import CronSchedule, JobSettings, PauseStatus

    workspace = WorkspaceClient()
    now_ms = int(time.time() * 1000)
    try:
        endpoint = workspace.vector_search_endpoints.get_endpoint(ENDPOINT)
        created_ms = int(endpoint.creation_timestamp)
    except Exception as error:  # SDK error types vary; anything but "missing" must surface
        if not _missing(error):
            raise
        created_ms = None
    decision = decide(
        endpoint_created_ms=created_ms, now_ms=now_ms, expires_at_ms=args.expires_at_ms
    )
    report = {"endpoint": ENDPOINT, "created_ms": created_ms, "now_ms": now_ms,
              "decision": decision.action, "reason": decision.reason}  # fmt: skip
    if decision.action == "delete":
        try:
            workspace.vector_search_indexes.delete_index(INDEX)
            report["index"] = "delete requested"
        except Exception as error:
            if not _missing(error):
                raise
            report["index"] = "absent"
        for _ in range(20):  # the endpoint can be deleted once its index is gone
            try:
                workspace.vector_search_indexes.get_index(INDEX)
            except Exception as error:
                if _missing(error):
                    break
                raise
            time.sleep(15)
        workspace.vector_search_endpoints.delete_endpoint(ENDPOINT)
        report["endpoint_action"] = "delete requested"
    elif decision.action == "pause" and args.job_id:
        workspace.jobs.update(
            args.job_id,
            new_settings=JobSettings(
                schedule=CronSchedule(
                    quartz_cron_expression=SCHEDULE,
                    timezone_id="UTC",
                    pause_status=PauseStatus.PAUSED,
                )
            ),
        )
        report["guard"] = "schedule paused"
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    # Serverless Python tasks run under IPython, which reports even SystemExit(0) as a failure.
    main()

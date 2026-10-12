"""Validate a terminal UrbanFlow 2.0 platform Job run and its final validation result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

EXPECTED_TASKS = (
    "preflight",
    "land_sources",
    "station_source_check",
    "station_silver",
    "station_gold",
    "historical_trips",
    "weather_enrichment",
    "validate_medallion",
    "federation_cdc",
    "should_refresh_lakeflow",
    "refresh_lakeflow",
)
FINAL_TASK = "validate_medallion"
# The Lakeflow refresh runs only when run_lakeflow=true; otherwise Databricks excludes it.
MAY_BE_EXCLUDED = frozenset({"refresh_lakeflow"})


def _state(value: dict[str, Any]) -> tuple[str, str]:
    state = value.get("state") or {}
    return str(state.get("life_cycle_state", "")), str(state.get("result_state", ""))


def validate_job_run(
    payload: dict[str, Any],
    *,
    expected_job_id: int,
    final_output: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if int(payload.get("job_id", -1)) != expected_job_id:
        raise ValueError(f"Run belongs to Job {payload.get('job_id')}, expected {expected_job_id}.")
    lifecycle, result = _state(payload)
    if (lifecycle, result) != ("TERMINATED", "SUCCESS"):
        raise ValueError(
            f"Platform Job ended as {lifecycle}/{result}, expected TERMINATED/SUCCESS."
        )
    by_key = {task.get("task_key"): task for task in payload.get("tasks") or []}
    if set(by_key) != set(EXPECTED_TASKS):
        raise ValueError(f"Unexpected task set: {sorted(by_key)}.")
    task_states = {}
    for key in EXPECTED_TASKS:
        task_lifecycle, task_result = _state(by_key[key])
        allowed = {("TERMINATED", "SUCCESS")}
        if key in MAY_BE_EXCLUDED:
            allowed |= {("TERMINATED", "EXCLUDED"), ("SKIPPED", "EXCLUDED")}
        if (task_lifecycle, task_result) not in allowed:
            raise ValueError(f"Task {key} ended as {task_lifecycle}/{task_result}.")
        if int(by_key[key].get("attempt_number", 0)) != 0:
            raise ValueError(f"Task {key} used an unexpected retry attempt.")
        task_states[key] = {"life_cycle_state": task_lifecycle, "result_state": task_result}
    if payload.get("repair_history"):
        raise ValueError("Release acceptance requires an unrepaired complete run.")

    final_run_id = by_key[FINAL_TASK].get("run_id")
    if not final_run_id:
        raise ValueError("Final validation task has no run ID.")
    validation = None
    if final_output is not None:
        result_text = (final_output.get("notebook_output") or {}).get("result")
        try:
            validation = json.loads(result_text)
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError("Final validation notebook did not return JSON.") from error
        if validation.get("status") != "PASS":
            raise ValueError(f"Final validation result is not PASS: {validation!r}.")
    return {
        "status": "PASS",
        "job_id": expected_job_id,
        "run_id": payload.get("run_id"),
        "final_task_run_id": final_run_id,
        "tasks": task_states,
        "validation": validation,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--final-output", type=Path)
    args = parser.parse_args()
    run_payload = json.loads(args.run.read_text(encoding="utf-8"))
    output_payload = (
        json.loads(args.final_output.read_text(encoding="utf-8")) if args.final_output else None
    )
    print(
        json.dumps(
            validate_job_run(run_payload, expected_job_id=args.job_id, final_output=output_payload),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

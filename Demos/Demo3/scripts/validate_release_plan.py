"""Fail-closed checks for the UrbanFlow selected-resource DAB release plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

RESOURCE_KEY = "resources.jobs.urbanflow_end_to_end"
EXPECTED_NAME = "[azure] UrbanFlow End-to-End"
EXPECTED_METADATA_PATH = (
    "/Workspace/Users/parvinbadalov@softserve.academy/"
    ".bundle/demo3_urbanflow/azure/state/metadata.json"
)
EXPECTED_TASK_KEYS = {
    "01_preflight",
    "02_station_source_check",
    "03_station_silver",
    "04_station_gold",
    "05_historical_trips",
    "06_weather_enrichment",
    "07_final_validation",
}


def _load(path: Path) -> dict[str, Any] | list[Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, (dict, list)):
        raise ValueError(f"{path} must contain a JSON object or array.")
    return value


def _listed_jobs(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else payload
    return [job for job in jobs if isinstance(job, dict)]


def _deleted_changes(value: Any, path: str = "") -> list[str]:
    deleted: list[str] = []
    if isinstance(value, dict):
        if str(value.get("action", "")).lower() in {"delete", "remove"}:
            deleted.append(path or "<root>")
        for key, item in value.items():
            deleted.extend(_deleted_changes(item, f"{path}.{key}".strip(".")))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            deleted.extend(_deleted_changes(item, f"{path}[{index}]"))
    return deleted


def validate_release_plan(
    plan_payload: dict[str, Any],
    *,
    phase: str,
    jobs_payload: dict[str, Any] | list[Any] | None = None,
) -> dict[str, Any]:
    resources = plan_payload.get("plan")
    if not isinstance(resources, dict) or set(resources) != {RESOURCE_KEY}:
        raise ValueError(
            f"Plan must contain exactly {RESOURCE_KEY}; found {sorted(resources or {})}."
        )
    resource = resources[RESOURCE_KEY]
    action = str(resource.get("action", "")).lower()
    if action not in {"create", "update", "skip"}:
        raise ValueError(f"Refusing unsupported or destructive plan action {action!r}.")
    deletes = _deleted_changes(resource.get("changes", {}))
    if deletes:
        raise ValueError(f"Plan contains forbidden delete/remove changes: {deletes}.")

    remote = resource.get("remote_state") or {}
    planned = (resource.get("new_state") or {}).get("value") or remote
    if planned.get("name") != EXPECTED_NAME:
        raise ValueError(
            f"Planned Job name is {planned.get('name')!r}, expected {EXPECTED_NAME!r}."
        )
    metadata_path = (planned.get("deployment") or {}).get("metadata_file_path")
    if metadata_path != EXPECTED_METADATA_PATH:
        raise ValueError(
            f"Plan uses deployment metadata {metadata_path!r}, expected {EXPECTED_METADATA_PATH!r}."
        )
    tasks = planned.get("tasks") or []
    if len(tasks) != 7:
        raise ValueError(f"Planned unified Job must have seven tasks, found {len(tasks)}.")
    if {task.get("task_key") for task in tasks} != EXPECTED_TASK_KEYS:
        raise ValueError("Planned unified Job task keys do not match the reviewed DAG.")
    if {task.get("existing_cluster_id") for task in tasks} != {"0702-132442-toro5spu"}:
        raise ValueError("Every planned task must use GP1 0702-132442-toro5spu.")
    if any("new_cluster" in task for task in tasks):
        raise ValueError("The planned unified Job must not create compute.")
    if action in {"update", "skip"} and remote.get("name") != EXPECTED_NAME:
        raise ValueError(f"Remote Job name is {remote.get('name')!r}, expected {EXPECTED_NAME!r}.")
    matches = []
    if jobs_payload is not None:
        matches = [
            job
            for job in _listed_jobs(jobs_payload)
            if job.get("settings", {}).get("name") == EXPECTED_NAME
        ]
        if len(matches) > 1:
            raise ValueError(
                f"Found {len(matches)} exact-name Jobs; refusing an ambiguous deployment."
            )
        if action == "create" and matches:
            raise ValueError(
                "Plan says create, but the exact-name Job already exists; bind or recover deployment state before retrying."
            )

    if phase == "post":
        if action != "skip":
            raise ValueError(f"Post-deploy plan must be unchanged/skip, found {action!r}.")
        job_id = remote.get("job_id")
        if not job_id:
            raise ValueError("Post-deploy plan did not expose the managed Job ID.")
        if matches and str(matches[0].get("job_id")) != str(job_id):
            raise ValueError("Plan Job ID and exact-name Job lookup disagree.")
    elif phase != "pre":
        raise ValueError(f"Unknown validation phase {phase!r}.")
    return {"status": "PASS", "phase": phase, "action": action, "job_id": remote.get("job_id")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--phase", choices=("pre", "post"), required=True)
    parser.add_argument("--jobs", type=Path)
    args = parser.parse_args()
    plan_payload = _load(args.plan)
    if not isinstance(plan_payload, dict):
        raise ValueError("The DAB plan must be a JSON object.")
    result = validate_release_plan(
        plan_payload,
        phase=args.phase,
        jobs_payload=_load(args.jobs) if args.jobs else None,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

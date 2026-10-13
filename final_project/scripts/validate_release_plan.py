"""Fail-closed checks for an UrbanFlow 2.0 Asset Bundle plan before and after a deployment.

`databricks bundle plan --output json` is checked so a release can only create, update or leave
unchanged the Final Project's own resources - never delete anything, never add a resource the
reviewed bundle does not declare, and never introduce cluster compute or a schedule.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

BUNDLE_NAME = "urbanflow_final_project"
TARGETS = ("personal_dev", "azure_prod")
ALLOWED_RESOURCES = {
    "resources.jobs.urbanflow_final_platform": "UrbanFlow 2.0 Platform",
    "resources.jobs.urbanflow_final_rag": "UrbanFlow 2.0 Mobility RAG",
    "resources.pipelines.urbanflow_final_pipeline": "UrbanFlow 2.0 Medallion Pipeline",
    "resources.schemas.urbanflow_final_schema": None,
    "resources.schemas.urbanflow_final_lakeflow_schema": None,
    "resources.volumes.urbanflow_final_landing": None,
    "resources.dashboards.urbanflow_final_dashboard": "UrbanFlow 2.0 Operations",
}
ALLOWED_ACTIONS = {"create", "update", "skip"}
EXPECTED_PLATFORM_TASKS = {
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
}


def _load(path: Path) -> dict[str, Any] | list[Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, (dict, list)):
        raise ValueError(f"{path} must contain a JSON object or array.")
    return value


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


def _planned(resource: dict[str, Any]) -> dict[str, Any]:
    remote = resource.get("remote_state") or {}
    return (resource.get("new_state") or {}).get("value") or remote


def _check_job(key: str, planned: dict[str, Any]) -> None:
    for forbidden in ("schedule", "trigger", "continuous", "job_clusters"):
        if planned.get(forbidden):
            raise ValueError(f"{key} must not define {forbidden}.")
    tasks = planned.get("tasks") or []
    if not tasks:
        raise ValueError(f"{key} has no tasks.")
    for task in tasks:
        if "existing_cluster_id" in task or "new_cluster" in task or "job_cluster_key" in task:
            raise ValueError(f"{key} task {task.get('task_key')} uses cluster compute.")
    if key.endswith("urbanflow_final_platform"):
        keys = {task.get("task_key") for task in tasks}
        if keys != EXPECTED_PLATFORM_TASKS:
            raise ValueError(f"Platform Job tasks differ from the reviewed DAG: {sorted(keys)}.")


def _check_pipeline(key: str, planned: dict[str, Any]) -> None:
    if planned.get("continuous"):
        raise ValueError(f"{key} must be triggered, not continuous.")
    if planned.get("serverless") is not True:
        raise ValueError(f"{key} must use serverless compute.")
    if planned.get("clusters"):
        raise ValueError(f"{key} must not declare clusters.")
    if "urbanflow_final_lakeflow" not in str(planned.get("schema", "")):
        raise ValueError(f"{key} must write to the isolated Lakeflow schema.")


def validate_release_plan(
    plan_payload: dict[str, Any],
    *,
    phase: str,
    target: str,
    jobs_payload: dict[str, Any] | list[Any] | None = None,
) -> dict[str, Any]:
    if target not in TARGETS:
        raise ValueError(f"Unknown target {target!r}.")
    if phase not in {"pre", "post"}:
        raise ValueError(f"Unknown validation phase {phase!r}.")
    resources = plan_payload.get("plan")
    if not isinstance(resources, dict) or not resources:
        raise ValueError("The plan contains no resources.")
    unexpected = sorted(set(resources) - set(ALLOWED_RESOURCES))
    if unexpected:
        raise ValueError(f"Plan contains resources outside the reviewed bundle: {unexpected}.")

    actions: dict[str, str] = {}
    for key, resource in resources.items():
        action = str(resource.get("action", "")).lower()
        if action not in ALLOWED_ACTIONS:
            raise ValueError(f"Refusing unsupported or destructive action {action!r} on {key}.")
        deletes = _deleted_changes(resource.get("changes", {}))
        if deletes:
            raise ValueError(f"{key} contains forbidden delete/remove changes: {deletes}.")
        planned = _planned(resource)
        expected_name = ALLOWED_RESOURCES[key]
        name = str(planned.get("name") or planned.get("display_name") or "")
        # Production mode keeps the name; development mode prefixes "[dev <user>] ".
        if expected_name and not (f"[{target}]" in name and name.endswith(expected_name)):
            raise ValueError(f"{key} is named {name!r}, expected '[{target}] {expected_name}'.")
        if key.startswith("resources.jobs."):
            _check_job(key, planned)
            metadata = (planned.get("deployment") or {}).get("metadata_file_path", "")
            if f"/.bundle/{BUNDLE_NAME}/{target}/state/metadata.json" not in str(metadata):
                raise ValueError(f"{key} uses unexpected deployment metadata {metadata!r}.")
        if key.startswith("resources.pipelines."):
            _check_pipeline(key, planned)
        actions[key] = action

    if jobs_payload is not None:
        jobs = jobs_payload.get("jobs", []) if isinstance(jobs_payload, dict) else jobs_payload
        for key in (
            "resources.jobs.urbanflow_final_platform",
            "resources.jobs.urbanflow_final_rag",
        ):
            if key not in resources:
                continue
            name = str(_planned(resources[key]).get("name"))
            matches = [job for job in jobs if job.get("settings", {}).get("name") == name]
            if len(matches) > 1:
                raise ValueError(f"Found {len(matches)} Jobs named {name!r}; refusing to deploy.")
            if actions[key] == "create" and matches:
                raise ValueError(f"Plan creates {name!r}, but that Job already exists unmanaged.")

    if phase == "post" and any(action != "skip" for action in actions.values()):
        changed = sorted(key for key, action in actions.items() if action != "skip")
        raise ValueError(f"Post-deploy plan must be unchanged, but these still change: {changed}.")
    job_ids = {
        key.rsplit(".", 1)[-1]: (resources[key].get("remote_state") or {}).get("job_id")
        for key in resources
        if key.startswith("resources.jobs.")
    }
    return {
        "status": "PASS",
        "phase": phase,
        "target": target,
        "actions": actions,
        "job_ids": job_ids,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--phase", choices=("pre", "post"), required=True)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--jobs", type=Path)
    args = parser.parse_args()
    plan_payload = _load(args.plan)
    if not isinstance(plan_payload, dict):
        raise ValueError("The bundle plan must be a JSON object.")
    result = validate_release_plan(
        plan_payload,
        phase=args.phase,
        target=args.target,
        jobs_payload=_load(args.jobs) if args.jobs else None,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

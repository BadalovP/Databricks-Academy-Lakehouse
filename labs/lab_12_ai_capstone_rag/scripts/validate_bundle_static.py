"""Offline bundle safety checks used by CI; never authenticates to Databricks."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_TARGETS = {
    "personal_dev": {
        "host": "https://dbc-1750318a-76a9.cloud.databricks.com",
        "schema": "parvinbadalov_lab12_rag_dev",
        "mode": "development",
    },
    "azure_prod": {
        "host": "https://adb-7405618361895520.0.azuredatabricks.net",
        "schema": "parvinbadalov_lab12_rag_prod",
        "mode": "production",
    },
}
LIVE_FLAGS = ("allow_table_writes", "allow_ai_search_mutation", "run_live_inference")
# A run without overrides must evaluate the full suite, baseline included.
FULL_EVALUATION_DEFAULTS = {
    "eval_question_limit": "18",
    "eval_question_ids": "",
    "eval_include_baseline": "true",
}


def check(bundle: dict, resources: dict) -> list[str]:
    """Return a list of violations (empty means the bundle is safe)."""
    problems: list[str] = []
    if bundle["bundle"]["name"] != "lab12_ai_capstone_rag":
        problems.append("unexpected bundle name")
    if "resources/*.yml" not in bundle.get("include", []):
        problems.append("resources/*.yml is not included")
    if "artifacts" in bundle:
        problems.append("wheel artifacts are not used: serverless caches a wheel by version")
    sync = bundle.get("sync", {})
    if not {".venv/**", "**/__pycache__/**"} <= set(sync.get("exclude", [])):
        problems.append("sync must exclude .venv/** and **/__pycache__/**")
    broad = [p for p in sync.get("include", []) if p.endswith("/**")]
    if broad:
        problems.append(f"unanchored directory includes also match inside .venv: {broad}")

    targets = bundle.get("targets", {})
    if set(targets) != set(EXPECTED_TARGETS):
        problems.append(f"targets must be exactly {sorted(EXPECTED_TARGETS)}, got {sorted(targets)}")
    if any(target.get("default") for target in targets.values()):
        problems.append("no target may be the default; every command must name -t explicitly")
    schemas, hosts = set(), set()
    for name, expected in EXPECTED_TARGETS.items():
        target = targets.get(name, {})
        workspace = target.get("workspace", {})
        if workspace.get("host") != expected["host"]:
            problems.append(f"{name}: host must be {expected['host']}")
        if target.get("mode") != expected["mode"]:
            problems.append(f"{name}: mode must be {expected['mode']}")
        if "${bundle.target}" not in str(workspace.get("root_path", "")):
            problems.append(f"{name}: root_path must contain the target so state is separate")
        variables = target.get("variables", {})
        if variables.get("schema") != expected["schema"]:
            problems.append(f"{name}: schema must be {expected['schema']}")
        schemas.add(variables.get("schema"))
        hosts.add(workspace.get("host"))
    if len(schemas) != len(EXPECTED_TARGETS) or len(hosts) != len(EXPECTED_TARGETS):
        problems.append("targets must not share a schema or a host")

    jobs = resources["resources"]["jobs"]
    if len(jobs) != 1:
        problems.append("exactly one Job is expected")
    job = jobs.get("lab12_rag_workflow", {})
    for forbidden in ("schedule", "continuous", "trigger", "triggers"):
        if forbidden in job:
            problems.append(f"automatic execution is forbidden: {forbidden}")
    defaults = {item["name"]: item["default"] for item in job.get("parameters", [])}
    for flag in LIVE_FLAGS:
        if defaults.get(flag) != "false":
            problems.append(f"{flag} must default to 'false'")
    for name, value in FULL_EVALUATION_DEFAULTS.items():
        if defaults.get(name) != value:
            problems.append(f"{name} must default to {value!r} (full evaluation)")
    if defaults.get("expected_host") != "${workspace.host}":
        problems.append("expected_host must default to ${workspace.host}")
    names = set(defaults)
    for task in job.get("tasks", []):
        key = task.get("task_key")
        if task.get("environment_key") != "lab12_serverless":
            problems.append(f"{key}: must use the lab12_serverless environment")
        if "libraries" in task:
            problems.append(f"{key}: task libraries are not supported on serverless")
        # Serverless retries failed tasks by default; a retry of provision_ai_search would sync
        # an existing index, a mutation nobody approved for that run.
        if task.get("disable_auto_optimization") is not True or task.get("max_retries", 0) != 0:
            problems.append(f"{key}: automatic retries must be disabled")
        for field in ("existing_cluster_id", "new_cluster", "job_cluster_key"):
            if field in task:
                problems.append(f"{key}: {field} is forbidden; serverless only")
        # A Job parameter overrides a same-named task parameter (seen in Lab 10).
        shadowed = names & set(task.get("notebook_task", {}).get("base_parameters", {}))
        if shadowed:
            problems.append(f"{key}: base_parameters shadowed by Job parameters: {sorted(shadowed)}")
    if "job_clusters" in job:
        problems.append("job_clusters are forbidden; serverless only")
    return problems


def main() -> int:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    resources = yaml.safe_load(
        (ROOT / "resources" / "lab12_job.job.yml").read_text(encoding="utf-8")
    )
    problems = check(bundle, resources)
    if problems:
        for problem in problems:
            print(f"FAIL: {problem}")
        return 1
    print(
        "Offline bundle safety checks passed: two pinned targets (no default), separate schemas, "
        "serverless-only manual Job, every live flag defaults to false."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

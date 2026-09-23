"""Persistent Lab 9 Databricks Job management: find-or-create, reset, run.

Exactly one stable Lab 9 job is used across runs (Phase 12): a new
duplicate job is never created. On every run the existing job's task is
reset to point at that run's freshly created temporary cluster id, so the
job never points at a dead cluster across executions. If explicit cluster
creation turns out to be forbidden, the same job task can instead be
pointed at a `new_cluster` definition (the documented fallback -- see
compute.ClusterSpec.as_new_cluster_dict()).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_svc

logger = logging.getLogger(__name__)


def find_job_by_name(client: WorkspaceClient, name: str):
    """Return the existing Lab 9 job with this exact name, or None."""
    for job in client.jobs.list(name=name):
        if job.settings and job.settings.name == name:
            return job
    return None


def _task_settings(
    cfg: dict[str, Any],
    notebook_path: str,
    cluster_id: str | None,
    new_cluster: dict[str, Any] | None,
) -> jobs_svc.Task:
    task_key = cfg["job"]["task_key"]
    notebook_task = jobs_svc.NotebookTask(notebook_path=notebook_path)
    if cluster_id:
        return jobs_svc.Task(
            task_key=task_key, notebook_task=notebook_task, existing_cluster_id=cluster_id
        )
    if new_cluster:
        return jobs_svc.Task(
            task_key=task_key,
            notebook_task=notebook_task,
            new_cluster=jobs_svc.ClusterSpec.from_dict(new_cluster),
        )
    raise ValueError("Either cluster_id or new_cluster must be provided for the Lab 9 job task.")


def ensure_job(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    notebook_path: str,
    cluster_id: str | None = None,
    new_cluster: dict[str, Any] | None = None,
) -> int:
    """Find the Lab 9 job by name; create it only if missing. Returns job_id."""
    name = cfg["job"]["name"]
    existing = find_job_by_name(client, name)
    if existing is not None:
        logger.info(
            "Job %s already exists (id=%s); will reset before running.", name, existing.job_id
        )
        return existing.job_id

    task = _task_settings(cfg, notebook_path, cluster_id, new_cluster)
    created = client.jobs.create(name=name, tasks=[task])
    logger.info("Created job %s (id=%s).", name, created.job_id)
    return created.job_id


def reset_job_cluster(
    client: WorkspaceClient,
    job_id: int,
    cfg: dict[str, Any],
    notebook_path: str,
    cluster_id: str | None = None,
    new_cluster: dict[str, Any] | None = None,
) -> None:
    """Reset the existing job so its task points at the current run's cluster.

    This is what prevents a persistent job from accumulating stale
    existing_cluster_id references to clusters that were already
    terminated by a previous run's cleanup step.
    """
    name = cfg["job"]["name"]
    task = _task_settings(cfg, notebook_path, cluster_id, new_cluster)
    client.jobs.reset(job_id=job_id, new_settings=jobs_svc.JobSettings(name=name, tasks=[task]))
    logger.info("Reset job %s (id=%s) to point at cluster_id=%s.", name, job_id, cluster_id)


def run_job_now(client: WorkspaceClient, job_id: int) -> int:
    """Trigger the job and return the run_id immediately (no waiting)."""
    waiter = client.jobs.run_now(job_id=job_id)
    return waiter.run_id


def get_run_output_json(client: WorkspaceClient, run_id: int, task_key: str) -> dict[str, Any]:
    """Retrieve and parse the reconciliation notebook's dbutils.notebook.exit() JSON payload.

    Locates the specific task run within `run_id` by task_key, then calls
    get_run_output(task_run_id) -- notebook output is only available from
    the task run, not the top-level job run.
    """
    run = client.jobs.get_run(run_id=run_id)
    task_runs = run.tasks or []
    matching = [t for t in task_runs if t.task_key == task_key]
    if not matching:
        raise RuntimeError(f"No task with task_key={task_key!r} found in run {run_id}.")
    task_run_id = matching[0].run_id

    output = client.jobs.get_run_output(run_id=task_run_id)
    notebook_output = getattr(output, "notebook_output", None)
    result = getattr(notebook_output, "result", None) if notebook_output else None
    if result is None:
        error = getattr(output, "error", None)
        raise RuntimeError(
            f"Reconciliation notebook task {task_key!r} (run {task_run_id}) produced no "
            f"notebook_output.result. error={error!r}"
        )
    return json.loads(result)

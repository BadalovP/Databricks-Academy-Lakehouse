"""Persistent Lab 9 Databricks Job management: find-or-create, reset, run.

Exactly one stable Lab 9 job is used across runs (Phase 12): a new
duplicate job is never created. On every run the existing job's task is
reset to point at that run's chosen compute, in exactly one of three
mutually exclusive ways:

  A. existing_cluster_id -- a standalone cluster this run created via
     compute.try_start_cluster_create() (compute_mode="explicit_cluster").
  B. new_cluster -- a job-managed classic cluster Databricks provisions
     and tears down itself as part of the run
     (compute_mode="job_cluster"; see compute.ClusterSpec.as_new_cluster_dict()).
  C. serverless -- no cluster reference of any kind
     (compute_mode="serverless_job"). Confirmed via the installed
     databricks-sdk==0.133.0's Task field set and cross-checked against
     Databricks' own Jobs API docs: omitting existing_cluster_id,
     new_cluster, AND job_cluster_key is how a notebook_task runs on
     serverless compute. `environment_key` is deliberately NOT set here --
     it is documented as applicable (and required) only for Python
     script/wheel/dbt tasks, not notebook tasks; setting it on a
     notebook_task is rejected by the Jobs API.

This avoids a persistent job pointing at a dead standalone cluster across
executions in mode A, and lets the same job be run against whichever
compute mode a given workspace actually supports (see README.md "Cluster
fallback behavior" for why the Personal workspace this project has tested
against needs mode C).

Independent of compute mode, every task also carries NotebookTask.base_parameters
(see _notebook_base_parameters()) so the reconciliation notebook queries
cfg["pipeline"]["target_schema"] -- the pipeline's actual OUTPUT schema --
instead of silently relying on its own hardcoded widget defaults. This
matters because the pipeline's output schema (config/dev.yml's
pipeline.target_schema, e.g. "lab09") is deliberately NOT the same schema
as the landing volume's schema (cfg["schema"]) -- see volumes.ensure_output_schema().
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute as compute_svc
from databricks.sdk.service import jobs as jobs_svc

logger = logging.getLogger(__name__)


def find_job_by_name(client: WorkspaceClient, name: str):
    """Return the existing Lab 9 job with this exact name, or None."""
    for job in client.jobs.list(name=name):
        if job.settings and job.settings.name == name:
            return job
    return None


def _notebook_base_parameters(cfg: dict[str, Any]) -> dict[str, str]:
    """Override the reconciliation notebook's hardcoded widget defaults so it
    queries the pipeline's actual OUTPUT schema (cfg["pipeline"]["target_schema"],
    e.g. "lab09") instead of silently falling back to its
    dbutils.widgets.text("schema", "parvinbadalov") default -- which would
    reconcile against the wrong (and possibly stale/nonexistent) tables once
    the pipeline itself writes to a different schema. Every value must be a
    plain str: NotebookTask.base_parameters is typed Dict[str, str] in the
    installed databricks-sdk==0.133.0.
    """
    tables = cfg["tables"]
    return {
        "catalog": str(cfg["catalog"]),
        "schema": str(cfg["pipeline"]["target_schema"]),
        "bronze_table": str(tables["bronze"]),
        "silver_table": str(tables["silver"]),
        "quarantine_table": str(tables["quarantine"]),
        "gold_table": str(tables["gold"]),
    }


def _task_settings(
    cfg: dict[str, Any],
    notebook_path: str,
    cluster_id: str | None,
    new_cluster: dict[str, Any] | None,
    serverless: bool = False,
) -> jobs_svc.Task:
    task_key = cfg["job"]["task_key"]
    notebook_task = jobs_svc.NotebookTask(
        notebook_path=notebook_path, base_parameters=_notebook_base_parameters(cfg)
    )

    modes_selected = sum([bool(cluster_id), bool(new_cluster), bool(serverless)])
    if modes_selected != 1:
        new_cluster_repr = "set" if new_cluster else None
        raise ValueError(
            "Exactly one of cluster_id, new_cluster, or serverless=True must be provided for "
            f"the Lab 9 job task; got {modes_selected} selected "
            f"(cluster_id={cluster_id!r}, new_cluster={new_cluster_repr!r}, "
            f"serverless={serverless!r})."
        )

    if serverless:
        # No existing_cluster_id / new_cluster / job_cluster_key at all.
        return jobs_svc.Task(task_key=task_key, notebook_task=notebook_task)
    if cluster_id:
        return jobs_svc.Task(
            task_key=task_key, notebook_task=notebook_task, existing_cluster_id=cluster_id
        )
    return jobs_svc.Task(
        task_key=task_key,
        notebook_task=notebook_task,
        # Task.new_cluster is typed Optional[compute.ClusterSpec] -- NOT
        # jobs.ClusterSpec, which is a different, unrelated struct (used
        # for JobSettings.job_clusters list entries: job_cluster_key +
        # new_cluster + libraries). Confirmed by inspecting both classes'
        # dataclass fields directly; using jobs.ClusterSpec here silently
        # built the wrong object (e.g. it has no node_type_id field at
        # all) until a test that actually inspected the constructed
        # object's own fields caught it.
        new_cluster=compute_svc.ClusterSpec.from_dict(new_cluster),
    )


def ensure_job(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    notebook_path: str,
    cluster_id: str | None = None,
    new_cluster: dict[str, Any] | None = None,
    serverless: bool = False,
) -> int:
    """Find the Lab 9 job by name; create it only if missing. Returns job_id."""
    name = cfg["job"]["name"]
    existing = find_job_by_name(client, name)
    if existing is not None:
        logger.info(
            "Job %s already exists (id=%s); will reset before running.", name, existing.job_id
        )
        return existing.job_id

    task = _task_settings(cfg, notebook_path, cluster_id, new_cluster, serverless)
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
    serverless: bool = False,
) -> None:
    """Reset the existing job so its task points at the current run's chosen compute.

    This is what prevents a persistent job from accumulating stale
    existing_cluster_id references to clusters that were already
    terminated by a previous run's cleanup step (mode A). Modes B and C
    have no such staleness concern -- Databricks provisions that compute
    fresh each run -- but the same job is still reused every time; a new
    duplicate job is never created for any mode.
    """
    name = cfg["job"]["name"]
    task = _task_settings(cfg, notebook_path, cluster_id, new_cluster, serverless)
    client.jobs.reset(job_id=job_id, new_settings=jobs_svc.JobSettings(name=name, tasks=[task]))
    logger.info(
        "Reset job %s (id=%s) to point at cluster_id=%s new_cluster=%s serverless=%s.",
        name,
        job_id,
        cluster_id,
        bool(new_cluster),
        serverless,
    )


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

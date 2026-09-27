"""Idempotent Azure deployment for LAB 09's three-task reconciliation Job.

Deploys the SAME `lab09` Python implementation the Personal-workspace
deployment uses (`lab09.volumes`, `lab09.pipelines`, `lab09.workspace`,
`lab09.jobs`), configured via config/azure.yml instead of config/dev.yml --
environment-specific configuration, not a forked implementation. See
README.md "Two-environment architecture".

Code-reuse mechanism: this project's actual `src/lab09` package source is
uploaded as plain Workspace Files (not a wheel). A wheel was considered and
rejected for this project: it would add a new build-time dependency
(neither `build` nor `wheel` is currently installed or required anywhere
else in this project) and a version-bump/rebuild step for something a plain
source upload already solves -- notebooks/02_ingest_data.py locates this
project's own root via its own notebook path (never a hardcoded identity)
and inserts "<root>/src" onto sys.path before importing `lab09.landing`
directly, the exact same function this script itself calls locally below.

Deliberately does NOT create a duplicate orchestration Job: exactly one Job
(config/azure.yml's job.name, "lab09_taxi_reconciliation_job") is
find-or-created, with three dependent tasks in its own task graph
(ingestion -> Lakeflow pipeline -> reconciliation), never three separate
Jobs. The ingestion and reconciliation tasks share one named Job cluster
(config/azure.yml's job.shared_job_cluster_key); the Lakeflow pipeline task
uses its own pipeline-managed compute (see config/azure.yml's
pipeline.prefer_serverless), independent of that shared cluster.

Never triggers a run: this script only ensures the Job DEFINITION exists
and is up to date. Running it is a separate, explicit action (see
.github/workflows/lab09_azure_deployment.yml).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute as compute_svc
from databricks.sdk.service.jobs import (
    JobCluster,
    JobRunAs,
    JobSettings,
    NotebookTask,
    PipelineTask,
    Task,
    TaskDependency,
)

from lab09 import compute, jobs, pipelines, volumes, workspace
from lab09.client import get_workspace_client, load_config, normalize_host

logger = logging.getLogger(__name__)


class ResourceOwnershipError(RuntimeError):
    """Raised when a Job or pipeline found by name was not created by the
    identity running this deployment. This Azure workspace is shared by
    100+ other students (confirmed via a read-only inventory before this
    script was written) who could, in principle, name their own resources
    identically -- matching by name alone is never sufficient proof that a
    found resource is this project's own before resetting/updating it.
    """


#: Explicit, narrow, ID-based allowlist for resources whose recorded creator
#: is expected to differ from the identity now running this script, because
#: of a specific, already-verified, one-off identity migration -- never a
#: name-based or blanket bypass. Each entry records the exact immutable
#: resource id together with the specific prior-creator identity this
#: project itself independently verified (via a read-only Databricks check)
#: before approving that migration. Adding an entry here is itself a
#: reviewable code change, never a runtime decision: a resource id that is
#: not listed still requires an exact creator_user_name match, with no
#: exception.
#:
#: 2026-09-27: github-lab08-travelops (Lab 8's existing, shared service
#: principal) created these two resources while deploying Lab 9's Azure
#: target, before the dedicated github-lab09-taxi-automation identity
#: existed. Both are confirmed, by this project's own read-only inspection,
#: to be the real, singular Lab 9 Job/pipeline this session itself deployed
#: (not another student's resource) -- see README.md "Two-environment
#: architecture" for the full account.
APPROVED_IDENTITY_MIGRATIONS: dict[str, str] = {
    "374991019372414": "3ec7e8df-66a2-4102-ab57-e4448b4e0e01",  # lab09_taxi_reconciliation_job
    "93a49a14-366e-4224-b2b3-587ef0b7a028": "3ec7e8df-66a2-4102-ab57-e4448b4e0e01",  # lab09_taxi_pipeline_v2
}


def verify_owned_by_current_identity(
    resource: Any, identity: str, description: str, resource_id: str | None = None
) -> None:
    """Refuse to proceed unless `resource.creator_user_name` matches `identity`
    exactly -- unless `resource_id` is listed in APPROVED_IDENTITY_MIGRATIONS
    AND the resource's actual creator matches that entry's own recorded
    prior creator exactly (never just any creator). Called on every
    Job/pipeline this script finds by name, before any reset/update call --
    see ResourceOwnershipError's and APPROVED_IDENTITY_MIGRATIONS' own
    docstrings for why this check, and its one narrow exception, exist.
    """
    creator = getattr(resource, "creator_user_name", None)
    if creator == identity:
        return
    if resource_id is not None:
        expected_prior_creator = APPROVED_IDENTITY_MIGRATIONS.get(resource_id)
        if expected_prior_creator is not None and creator == expected_prior_creator:
            logger.info(
                "%s (id=%s): creator %r does not match the current identity %r, but this "
                "exact resource id is on the explicit, reviewed identity-migration "
                "allowlist with that exact prior creator -- allowed.",
                description,
                resource_id,
                creator,
                identity,
            )
            return
    raise ResourceOwnershipError(
        f"{description} was found by name, but its creator ({creator!r}) does not "
        f"match the current identity ({identity!r}), and its id is not on the explicit "
        "identity-migration allowlist. Refusing to reset/update it: this workspace is "
        "shared by many other students, and a name match alone is not positive proof "
        "of ownership."
    )


LAB_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = LAB_ROOT / "config" / "azure.yml"
PIPELINE_SOURCE_DIR = LAB_ROOT / "pipeline"
RECONCILE_NOTEBOOK_PATH = LAB_ROOT / "notebooks" / "01_reconcile_counts.py"
INGEST_NOTEBOOK_PATH = LAB_ROOT / "notebooks" / "02_ingest_data.py"
SRC_LAB09_DIR = LAB_ROOT / "src" / "lab09"


@dataclass
class DeploymentReport:
    host: str | None = None
    identity: str | None = None
    schema_ensured: str | None = None
    volume_ensured: str | None = None
    project_root_path: str | None = None
    uploaded_source_files: list[str] | None = None
    ingestion_notebook_path: str | None = None
    reconciliation_notebook_path: str | None = None
    pipeline_id: str | None = None
    pipeline_serverless: bool | None = None
    job_id: int | None = None
    job_created: bool | None = None  # True = newly created, False = existing job reset

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def upload_project_source(client: WorkspaceClient, cfg: dict[str, Any]) -> tuple[str, list[str]]:
    """Upload this project's own src/lab09/*.py as plain workspace Files.

    See this module's docstring for why a wheel was not used instead.
    """
    root = workspace.lab09_root_path(client, cfg)
    source_subpath = cfg.get("workspace", {}).get("source_subpath", "src")
    target_src_dir = f"{root}/{source_subpath}/lab09"
    client.workspace.mkdirs(target_src_dir)
    uploaded = []
    for local_path in sorted(SRC_LAB09_DIR.glob("*.py")):
        target_path = f"{target_src_dir}/{local_path.name}"
        workspace.upload_workspace_file(client, local_path, target_path)
        uploaded.append(target_path)
    return f"{root}/{source_subpath}", uploaded


def upload_azure_config(client: WorkspaceClient, cfg: dict[str, Any], config_path: Path) -> str:
    root = workspace.lab09_root_path(client, cfg)
    target_path = f"{root}/config/azure.yml"
    client.workspace.mkdirs(f"{root}/config")
    workspace.upload_workspace_file(client, config_path, target_path)
    return target_path


def build_shared_job_cluster(client: WorkspaceClient, cfg: dict[str, Any]) -> JobCluster:
    """Build the ingestion/reconciliation tasks' shared job cluster.

    Deliberately does NOT reuse compute.build_cluster_spec()'s single-node,
    autoterminating shape (that is for an interactive/all-purpose
    demonstration cluster) -- this workspace's "Job Compute" cluster policy
    was confirmed live (2026-09-27) to require a materially different,
    fixed automated-cluster shape: a specific node type, 1-2 workers (never
    single-node), and outright rejects `cluster_name` and
    `autotermination_minutes` on any automated cluster regardless of
    policy (see compute.ClusterSpec.as_new_cluster_dict()'s docstring and
    its regression test) -- that fix is what makes the dict this function
    returns actually acceptable to the Jobs API.
    """
    job_compute_cfg = cfg["job_compute"]
    policy_id = compute.resolve_policy_id(client, job_compute_cfg["policy_name"])
    spark_version = compute.resolve_lts_spark_version(client)
    spec = compute.ClusterSpec(
        spark_version=spark_version,
        node_type_id=job_compute_cfg["node_type_id"],
        autotermination_minutes=20,  # discarded by as_new_cluster_dict() below
        num_workers=job_compute_cfg["num_workers"],
        policy_id=policy_id,
        single_node=False,
        custom_tags={"purpose": "lab09-azure-shared-job-compute"},
    )
    new_cluster_dict = spec.as_new_cluster_dict()
    return JobCluster(
        job_cluster_key=cfg["job"]["shared_job_cluster_key"],
        new_cluster=compute_svc.ClusterSpec.from_dict(new_cluster_dict),
    )


def build_task_graph(
    cfg: dict[str, Any],
    pipeline_id: str,
    ingestion_notebook_path: str,
    reconciliation_notebook_path: str,
    shared_cluster: JobCluster,
) -> tuple[list[JobCluster], list[Task]]:
    job_cfg = cfg["job"]
    ingestion_key = job_cfg["ingestion_task_key"]
    pipeline_key = job_cfg["pipeline_task_key"]
    reconciliation_key = job_cfg["reconciliation_task_key"]
    shared_key = shared_cluster.job_cluster_key

    tasks = [
        Task(
            task_key=ingestion_key,
            notebook_task=NotebookTask(notebook_path=ingestion_notebook_path),
            job_cluster_key=shared_key,
        ),
        Task(
            task_key=pipeline_key,
            pipeline_task=PipelineTask(pipeline_id=pipeline_id),
            depends_on=[TaskDependency(task_key=ingestion_key)],
        ),
        Task(
            task_key=reconciliation_key,
            notebook_task=NotebookTask(
                notebook_path=reconciliation_notebook_path,
                base_parameters=jobs.notebook_base_parameters(cfg),
            ),
            job_cluster_key=shared_key,
            depends_on=[TaskDependency(task_key=pipeline_key)],
        ),
    ]
    return [shared_cluster], tasks


def ensure_three_task_job(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    job_clusters: list[JobCluster],
    tasks: list[Task],
    identity: str,
) -> tuple[int, bool]:
    """Find-or-create the persistent, no-schedule reconciliation Job.

    Returns (job_id, created) -- created=True only when a new Job object
    was made; an existing Job is reset in place, never duplicated. Raises
    ResourceOwnershipError instead of resetting a same-named Job this
    identity did not create.

    Always explicitly sets run_as from cfg["run_as_user_name"] (if present)
    on both create and reset -- confirmed live that jobs.reset() does NOT
    silently revert an already-set run_as when it is omitted from the call,
    but this is set explicitly on every call regardless, rather than
    relying on that undocumented behavior alone. See config/azure.yml's own
    comment for why a separate "Run As" identity is used here at all.
    """
    name = cfg["job"]["name"]
    run_as_user_name = cfg.get("run_as_user_name")
    run_as = JobRunAs(user_name=run_as_user_name) if run_as_user_name else None

    existing = jobs.find_job_by_name(client, name)
    if existing is not None:
        verify_owned_by_current_identity(
            existing, identity, f"Job {name!r}", resource_id=str(existing.job_id)
        )
        client.jobs.reset(
            job_id=existing.job_id,
            new_settings=JobSettings(
                name=name, job_clusters=job_clusters, tasks=tasks, run_as=run_as
            ),
        )
        return existing.job_id, False
    created = client.jobs.create(name=name, job_clusters=job_clusters, tasks=tasks, run_as=run_as)
    return created.job_id, True


def deploy(client: WorkspaceClient, cfg: dict[str, Any], config_path: Path) -> DeploymentReport:
    report = DeploymentReport()
    report.host = client.config.host
    report.identity = client.current_user.me().user_name

    volumes.ensure_output_schema(client, cfg)
    report.schema_ensured = f"{cfg['catalog']}.{cfg['pipeline']['target_schema']}"
    volumes.ensure_volume(client, cfg)
    report.volume_ensured = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"

    workspace.upload_pipeline_sources(client, PIPELINE_SOURCE_DIR, cfg)
    reconciliation_path = workspace.upload_notebook(client, RECONCILE_NOTEBOOK_PATH, cfg)
    ingestion_path = workspace.upload_notebook(client, INGEST_NOTEBOOK_PATH, cfg)
    report.reconciliation_notebook_path = reconciliation_path
    report.ingestion_notebook_path = ingestion_path

    project_root, uploaded_source = upload_project_source(client, cfg)
    upload_azure_config(client, cfg, config_path)
    report.project_root_path = project_root
    report.uploaded_source_files = uploaded_source

    pipeline_dir_ws = workspace.pipeline_source_dir(client, cfg)
    existing_pipeline = pipelines.find_pipeline_by_name(client, cfg["pipeline"]["name"])
    if existing_pipeline is not None:
        verify_owned_by_current_identity(
            existing_pipeline,
            report.identity,
            f"Pipeline {cfg['pipeline']['name']!r}",
            resource_id=existing_pipeline.pipeline_id,
        )
    pipeline_id, used_serverless = pipelines.ensure_pipeline(client, cfg, pipeline_dir_ws)
    report.pipeline_id = pipeline_id
    report.pipeline_serverless = used_serverless

    shared_cluster = build_shared_job_cluster(client, cfg)
    job_clusters, tasks = build_task_graph(
        cfg, pipeline_id, ingestion_path, reconciliation_path, shared_cluster
    )
    job_id, created = ensure_three_task_job(client, cfg, job_clusters, tasks, report.identity)
    report.job_id = job_id
    report.job_created = created

    return report


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Idempotently deploy LAB 09's three-task Azure reconciliation Job "
            "(ingestion -> Lakeflow pipeline -> reconciliation). Never triggers a run."
        )
    )
    parser.add_argument(
        "--profile",
        default=None,
        help=(
            "Databricks CLI auth profile to use for a local run. Omit in CI, where "
            "DATABRICKS_HOST + DATABRICKS_AUTH_TYPE=azure-cli (OIDC) is used instead -- "
            "see get_workspace_client()'s docstring for the full resolution order."
        ),
    )
    parser.add_argument(
        "--confirm-host",
        required=True,
        help="The exact Databricks host you intend to target; the run aborts on any mismatch.",
    )
    parser.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH), help="Path to config/azure.yml."
    )
    parser.add_argument("--report-path", default="evidence/azure_deployment_report.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    client = get_workspace_client(args.profile)

    actual_host = normalize_host(client.config.host)
    expected_host = normalize_host(args.confirm_host)
    if actual_host != expected_host:
        parser.error(
            f"Profile {args.profile!r} resolved to a host that does not match --confirm-host "
            f"{expected_host!r}. Refusing to proceed without an exact match."
        )

    config_path = Path(args.config)
    cfg = load_config(config_path)
    report = deploy(client, cfg, config_path)

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

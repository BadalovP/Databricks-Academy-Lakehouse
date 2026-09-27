"""One-shot, uninterrupted LAB 09 classic-compute end-to-end demonstration.

Separate from `lab09.cli run-all`: that command's compute mode is driven by
config/dev.yml's `compute.preferred_mode` (serverless_job, because the
Personal workspace this project was built against has no classic-compute
worker environment at all -- see config/dev.yml's comment). This script
exists specifically to prove the literal "create clusters" requirement in a
workspace that *does* support classic compute, in a single invocation with
no manual step in the middle (see evidence/CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md
section 9 for why the previous attempt needed a manual cluster restart and
does not count as this).

This intentionally does NOT reuse lab09.cli's `run-all`/`_finish` or
lab09.jobs's persistent-job helpers: those are built around one stable,
reused pipeline+job across runs (see jobs.py's module docstring), while this
script creates and deletes its own uniquely-named, throwaway cluster, Job
and notebook every run -- a genuinely different lifecycle, not a variation
worth forcing into the same functions. It does reuse the real, already
security-reviewed building blocks that ARE the same concern in both cases:
lab09.client.get_workspace_client() (profile resolution + verify-after
credential check), lab09.compute (spec building, dynamic runtime/node-type
resolution, create, terminate-and-verify), and lab09.monitoring (explicit,
loggable polling -- no SDK `.result()` waiter is used here either).

Safety model:
  - Refuses to run without --confirm-billable (this creates a real, billable
    cluster) and --confirm-host (the resolved profile's host must match
    exactly, or the run aborts before creating anything).
  - Never looks up, lists-by-name-guess, or otherwise references any
    pre-existing cluster: the only cluster this script ever touches is the
    one it creates itself this run, identified by a random per-run tag
    (see find_cluster_by_unique_tag()). GP1/GP2 or any other pre-existing
    cluster in the target workspace are never read, started, stopped, or
    modified by this script.
  - Exactly one application-level cluster-create attempt -- no retry loop.
    If the create call itself raises without returning a cluster_id (e.g. a
    transport-level timeout), this searches for a cluster carrying this
    run's own unique tag rather than assuming either "nothing was created"
    or guessing which existing cluster might be it (see
    find_cluster_by_unique_tag()).
  - Cleanup (job delete, notebook delete, cluster terminate-and-verify) is
    always attempted in a `finally` block, regardless of where the run
    stopped or failed, and only ever targets the job_id/notebook_path/
    cluster_id this specific run itself created.
  - Termination is independently verified (lab09.compute.terminate_and_verify_cluster)
    -- a delete request being accepted is never treated as proof of
    TERMINATED.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task
from databricks.sdk.service.workspace import ImportFormat, Language

from lab09 import compute, jobs, monitoring
from lab09.client import get_workspace_client

logger = logging.getLogger(__name__)

TASK_KEY = "validate"
EXPECTED_RESULT = "OK:42"


def _notebook_source() -> bytes:
    """A trivial notebook: computes 6*42/6 ... simply returns OK:42 as proof
    of actual execution on the cluster, not a hardcoded/pre-baked value.
    """
    return (
        "# Databricks notebook source\n"
        "import json\n"
        "\n"
        'result = {"result": "OK:" + str(6 * 7)}\n'
        "dbutils.notebook.exit(json.dumps(result))\n"
    ).encode("utf-8")


@dataclass
class ValidationReport:
    status: str = "UNKNOWN"
    host: str | None = None
    identity: str | None = None
    cluster_policy_name: str | None = None
    cluster_policy_id: str | None = None
    node_type_id: str | None = None
    spark_version: str | None = None
    cluster_name: str | None = None
    cluster_id: str | None = None
    cluster_create_required_tag_search: bool = False
    cluster_reached_running: bool | None = None
    cluster_readiness_timed_out: bool | None = None
    cluster_readiness_final_state: str | None = None
    notebook_path: str | None = None
    job_id: int | None = None
    run_id: int | None = None
    job_life_cycle_state: str | None = None
    job_result_state: str | None = None
    job_timed_out: bool | None = None
    notebook_output: str | None = None
    output_matches_expected: bool | None = None
    cluster_terminated_confirmed: bool | None = None
    cluster_final_state: str | None = None
    job_deleted: bool | None = None
    notebook_deleted: bool | None = None
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

    @property
    def cleanup_confirmed(self) -> bool:
        """True unless a resource this run actually created is known NOT to
        have been cleaned up.

        None means "not applicable" (this run never got far enough to
        create that resource) and is never itself treated as a failure --
        only an explicit False (job_deleted=False, notebook_deleted=False,
        or cluster_terminated_confirmed=False) does. Mirrors the same
        "confirmed vs. merely not raised" distinction already established in
        cli.py's _finish() / compute.terminate_and_verify_cluster().
        """
        return (
            self.job_deleted is not False
            and self.notebook_deleted is not False
            and self.cluster_terminated_confirmed is not False
        )

    @property
    def succeeded(self) -> bool:
        """A successful notebook run must not, by itself, count as overall
        success if required cleanup of this run's own temporary resources
        failed. `status` above still preserves the actual execution
        result unchanged (never overwritten to "FAILED" just because
        cleanup fell short) -- this property is the separate, combined
        signal main()'s exit code is based on.
        """
        return self.status == "SUCCESS" and self.cleanup_confirmed


def write_report(report: ValidationReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
    logger.info("Wrote classic-compute validation report to %s", path)
    return path


def find_cluster_by_unique_tag(client: WorkspaceClient, tag_value: str) -> str | None:
    """Find a cluster carrying this run's own unique custom_tags value.

    Used only when compute.start_cluster_create() itself raises without ever
    returning a cluster_id (e.g. a transport-level timeout) -- this never
    assumes nothing was created, but also never guesses: a cluster is only
    ever treated as this run's own if its custom_tags carries the exact
    random per-run tag generated for this invocation. If no such cluster is
    found, the caller records that explicitly rather than treating any
    unrelated cluster as this run's responsibility.
    """
    for c in client.clusters.list():
        tags = getattr(c, "custom_tags", None) or {}
        if tags.get("lab09_run_id") == tag_value:
            return c.cluster_id
    return None


def run_validation(
    client: WorkspaceClient,
    *,
    node_type_hint: str | None,
    cluster_policy_name: str | None,
    autotermination_minutes: int,
    readiness_timeout_seconds: int,
    readiness_poll_interval_seconds: int,
    job_timeout_seconds: int,
    job_poll_interval_seconds: int,
    termination_timeout_seconds: int,
    termination_poll_interval_seconds: int,
    workspace_root_subpath: str = "lab09_classic_e2e",
) -> ValidationReport:
    """Run the full create -> RUNNING -> Job -> verify -> terminate -> cleanup
    sequence exactly once, uninterrupted. Never raises: every outcome,
    success or failure, is captured in the returned report, and cleanup is
    always attempted for whatever resources this run itself created.
    """
    report = ValidationReport()
    report.mark_started()

    run_tag = uuid.uuid4().hex[:12]
    cluster_name = f"lab09-classic-e2e-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{run_tag}"

    cluster_id: str | None = None
    job_id: int | None = None
    notebook_path: str | None = None

    try:
        report.host = client.config.host
        report.identity = client.current_user.me().user_name

        policy_id = compute.resolve_policy_id(client, cluster_policy_name)
        report.cluster_policy_name = cluster_policy_name
        report.cluster_policy_id = policy_id

        node_type_id = compute.resolve_node_type(client, node_type_hint)
        spark_version = compute.resolve_lts_spark_version(client)
        report.node_type_id = node_type_id
        report.spark_version = spark_version
        report.cluster_name = cluster_name

        spec = compute.ClusterSpec(
            spark_version=spark_version,
            node_type_id=node_type_id,
            autotermination_minutes=autotermination_minutes,
            num_workers=0,
            policy_id=policy_id,
            single_node=True,
            custom_tags={"purpose": "lab09-classic-e2e-validation", "lab09_run_id": run_tag},
        )

        try:
            cluster_id = compute.start_cluster_create(client, spec, cluster_name)
        except Exception as exc:  # noqa: BLE001 - a create-call failure must not be assumed fatal
            logger.warning(
                "Cluster create call raised (%s); searching for a cluster carrying this "
                "run's own unique tag before concluding nothing was created.",
                exc,
            )
            report.cluster_create_required_tag_search = True
            cluster_id = find_cluster_by_unique_tag(client, run_tag)
            if cluster_id is None:
                report.status = "FAILED"
                report.error = (
                    f"Cluster create call failed and no cluster carrying this run's own "
                    f"tag was found: {exc}"
                )
                return report
        report.cluster_id = cluster_id

        cluster_outcome = monitoring.poll_cluster_state(
            client,
            cluster_id,
            timeout_seconds=readiness_timeout_seconds,
            poll_interval_seconds=readiness_poll_interval_seconds,
        )
        report.cluster_readiness_timed_out = cluster_outcome.timed_out
        report.cluster_readiness_final_state = cluster_outcome.state
        report.cluster_reached_running = cluster_outcome.usable
        if not cluster_outcome.usable:
            report.status = "FAILED"
            report.error = (
                f"Cluster did not reach RUNNING (single, uninterrupted, automated attempt): "
                f"state={cluster_outcome.state} timed_out={cluster_outcome.timed_out}"
            )
            return report

        notebook_path = (
            f"/Workspace/Users/{report.identity}/{workspace_root_subpath}/"
            f"{cluster_name}/validate_notebook"
        )
        parent = str(Path(notebook_path).parent).replace("\\", "/")
        client.workspace.mkdirs(parent)
        client.workspace.import_(
            notebook_path,
            content=base64.b64encode(_notebook_source()).decode("ascii"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        report.notebook_path = notebook_path

        job_name = f"lab09-classic-e2e-{run_tag}"
        task = Task(
            task_key=TASK_KEY,
            notebook_task=NotebookTask(notebook_path=notebook_path),
            existing_cluster_id=cluster_id,
        )
        created_job = client.jobs.create(name=job_name, tasks=[task])
        job_id = created_job.job_id
        report.job_id = job_id

        run_id = jobs.run_job_now(client, job_id)
        report.run_id = run_id

        job_outcome = monitoring.poll_job_run(
            client,
            run_id,
            timeout_seconds=job_timeout_seconds,
            poll_interval_seconds=job_poll_interval_seconds,
        )
        report.job_life_cycle_state = job_outcome.life_cycle_state
        report.job_result_state = job_outcome.result_state
        report.job_timed_out = job_outcome.timed_out
        if not job_outcome.succeeded:
            report.status = "FAILED"
            report.error = (
                f"Job did not succeed: life_cycle_state={job_outcome.life_cycle_state} "
                f"result_state={job_outcome.result_state}"
            )
            return report

        output = jobs.get_run_output_json(client, run_id, TASK_KEY)
        actual = output.get("result")
        report.notebook_output = actual
        report.output_matches_expected = actual == EXPECTED_RESULT
        if actual != EXPECTED_RESULT:
            report.status = "FAILED"
            report.error = f"Notebook returned {actual!r}, expected {EXPECTED_RESULT!r}."
            return report

        report.status = "SUCCESS"
        return report

    except Exception as exc:  # noqa: BLE001 - orchestration must still clean up and report
        logger.exception("Classic-compute validation failed: %s", exc)
        report.status = "FAILED"
        report.error = str(exc)
        return report

    finally:
        # OUTER SAFETY: always attempt cleanup of whatever THIS run itself
        # created, regardless of where the run stopped -- mirroring
        # cli._finish()'s "cleanup is not conditional on success" design.
        if job_id is not None:
            try:
                client.jobs.delete(job_id=job_id)
                report.job_deleted = True
            except Exception as exc:  # noqa: BLE001 - cleanup must report, not raise
                logger.warning("Failed to delete temporary job %s: %s", job_id, exc)
                report.job_deleted = False
        if notebook_path is not None:
            try:
                client.workspace.delete(path=notebook_path)
                report.notebook_deleted = True
            except Exception as exc:  # noqa: BLE001 - cleanup must report, not raise
                logger.warning("Failed to delete temporary notebook %s: %s", notebook_path, exc)
                report.notebook_deleted = False
        if cluster_id is not None:
            outcome = compute.terminate_and_verify_cluster(
                client,
                cluster_id,
                timeout_seconds=termination_timeout_seconds,
                poll_interval_seconds=termination_poll_interval_seconds,
            )
            report.cluster_terminated_confirmed = outcome.confirmed
            report.cluster_final_state = outcome.state
            if not outcome.confirmed:
                logger.warning(
                    "Cluster %s termination not confirmed (last state=%s%s) -- "
                    "requires manual investigation.",
                    cluster_id,
                    outcome.state,
                    f", error={outcome.error}" if outcome.error else "",
                )
        report.mark_finished()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "One-shot, uninterrupted LAB 09 classic-compute demonstration: create a "
            "temporary cluster, wait for RUNNING, run a trivial notebook Job against it, "
            "verify its output, terminate the cluster, and delete the temporary Job/notebook "
            "-- all within a single invocation of this script."
        )
    )
    parser.add_argument(
        "--profile", required=True, help="Databricks CLI auth profile to use (never a shared PAT)."
    )
    parser.add_argument(
        "--confirm-host",
        required=True,
        help=(
            "The exact Databricks host you intend to target. The run aborts before creating "
            "anything if the resolved profile does not resolve to this host."
        ),
    )
    parser.add_argument(
        "--confirm-billable",
        action="store_true",
        help="Required acknowledgement that this creates a real, billable cloud cluster.",
    )
    parser.add_argument("--cluster-policy-name", default="Personal Compute")
    parser.add_argument("--node-type-hint", default="Standard_D4ds_v5")
    parser.add_argument("--autotermination-minutes", type=int, default=20)
    parser.add_argument("--readiness-timeout-seconds", type=int, default=1200)
    parser.add_argument("--readiness-poll-interval-seconds", type=int, default=15)
    parser.add_argument("--job-timeout-seconds", type=int, default=900)
    parser.add_argument("--job-poll-interval-seconds", type=int, default=10)
    parser.add_argument("--termination-timeout-seconds", type=int, default=600)
    parser.add_argument("--termination-poll-interval-seconds", type=int, default=10)
    parser.add_argument("--report-path", default="evidence/classic_e2e_report.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if not args.confirm_billable:
        parser.error(
            "Refusing to proceed without --confirm-billable: this creates a real, "
            "billable classic-compute cluster."
        )

    client = get_workspace_client(args.profile)

    actual_host = (client.config.host or "").rstrip("/").lower()
    expected_host = args.confirm_host.strip().rstrip("/").lower()
    if "://" not in expected_host:
        expected_host = "https://" + expected_host
    if actual_host != expected_host:
        parser.error(
            f"Profile {args.profile!r} resolved to a host that does not match --confirm-host "
            f"{expected_host!r}. Refusing to proceed without an exact match."
        )

    report = run_validation(
        client,
        node_type_hint=args.node_type_hint,
        cluster_policy_name=args.cluster_policy_name,
        autotermination_minutes=args.autotermination_minutes,
        readiness_timeout_seconds=args.readiness_timeout_seconds,
        readiness_poll_interval_seconds=args.readiness_poll_interval_seconds,
        job_timeout_seconds=args.job_timeout_seconds,
        job_poll_interval_seconds=args.job_poll_interval_seconds,
        termination_timeout_seconds=args.termination_timeout_seconds,
        termination_poll_interval_seconds=args.termination_poll_interval_seconds,
    )
    write_report(report, args.report_path)
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0 if report.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())

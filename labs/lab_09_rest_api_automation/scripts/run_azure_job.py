"""Trigger, monitor, and report on ONE run of LAB 09's Azure three-task Job.

Never creates or updates the Job itself -- see deploy_azure_job.py for that.
This only finds the persistent `lab09_taxi_reconciliation_job` by name,
triggers exactly one run (no retry loop), polls it to a terminal state,
retrieves each task's own output, and independently verifies the run's own
job-cluster instance actually reached TERMINATED afterward -- a run
finishing is not, by itself, treated as proof its compute is gone (the same
"confirmed vs. merely not raised" distinction already established in
compute.terminate_and_verify_cluster()).
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

from lab09 import jobs, monitoring
from lab09.client import get_workspace_client, load_config, normalize_host

logger = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = LAB_ROOT / "config" / "azure.yml"


@dataclass
class RunReport:
    job_id: int | None = None
    run_id: int | None = None
    life_cycle_state: str | None = None
    result_state: str | None = None
    timed_out: bool | None = None
    ingestion_output: dict[str, Any] | None = None
    ingestion_output_error: str | None = None
    reconciliation_output: dict[str, Any] | None = None
    reconciliation_output_error: str | None = None
    job_cluster_id: str | None = None
    job_cluster_terminated_confirmed: bool | None = None
    job_cluster_final_state: str | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def succeeded(self) -> bool:
        """Mirrors validate_classic_e2e.ValidationReport.succeeded's fix: a
        successful run must not count as overall success if its own
        job-cluster instance is known NOT to have terminated. None (no
        job-cluster instance found at all, e.g. the run failed before any
        task started) is never itself treated as a cleanup failure -- only
        an explicit False is.
        """
        return (
            self.result_state == "SUCCESS"
            and not self.timed_out
            and self.job_cluster_terminated_confirmed is not False
        )


def run_and_verify(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    job_timeout_seconds: int,
    job_poll_interval_seconds: int,
    termination_timeout_seconds: int,
    termination_poll_interval_seconds: int,
) -> RunReport:
    report = RunReport()
    job_cfg = cfg["job"]

    job = jobs.find_job_by_name(client, job_cfg["name"])
    if job is None:
        report.error = (
            f"Job {job_cfg['name']!r} not found -- deploy it first with deploy_azure_job.py."
        )
        return report
    report.job_id = job.job_id

    run_id = jobs.run_job_now(client, job.job_id)
    report.run_id = run_id

    outcome = monitoring.poll_job_run(
        client,
        run_id,
        timeout_seconds=job_timeout_seconds,
        poll_interval_seconds=job_poll_interval_seconds,
    )
    report.life_cycle_state = outcome.life_cycle_state
    report.result_state = outcome.result_state
    report.timed_out = outcome.timed_out

    run_details = client.jobs.get_run(run_id=run_id)
    task_runs = run_details.tasks or []
    cluster_ids = {
        t.task_key: getattr(t.cluster_instance, "cluster_id", None)
        for t in task_runs
        if getattr(t, "cluster_instance", None) is not None
    }
    report.job_cluster_id = cluster_ids.get(job_cfg["ingestion_task_key"]) or cluster_ids.get(
        job_cfg["reconciliation_task_key"]
    )

    if outcome.succeeded:
        try:
            report.ingestion_output = jobs.get_run_output_json(
                client, run_id, job_cfg["ingestion_task_key"]
            )
        except (
            Exception
        ) as exc:  # noqa: BLE001 - a retrieval failure must not mask the run's own result
            report.ingestion_output_error = str(exc)
        try:
            report.reconciliation_output = jobs.get_run_output_json(
                client, run_id, job_cfg["reconciliation_task_key"]
            )
        except Exception as exc:  # noqa: BLE001
            report.reconciliation_output_error = str(exc)

    if report.job_cluster_id:
        termination_outcome = monitoring.poll_cluster_termination(
            client,
            report.job_cluster_id,
            timeout_seconds=termination_timeout_seconds,
            poll_interval_seconds=termination_poll_interval_seconds,
        )
        report.job_cluster_terminated_confirmed = termination_outcome.confirmed
        report.job_cluster_final_state = termination_outcome.state
        if not termination_outcome.confirmed:
            logger.warning(
                "Job cluster %s termination not confirmed (state=%s%s) -- "
                "requires manual investigation.",
                report.job_cluster_id,
                termination_outcome.state,
                f", error={termination_outcome.error}" if termination_outcome.error else "",
            )

    return report


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Trigger exactly one run of the already-deployed "
            "lab09_taxi_reconciliation_job, monitor it to completion, collect "
            "task output, and verify its job-cluster instance terminated."
        )
    )
    parser.add_argument("--profile", default=None, help="Databricks CLI auth profile to use.")
    parser.add_argument(
        "--confirm-host",
        default=None,
        help=(
            "The exact Databricks host you intend to target. If given, the run aborts "
            "before triggering anything if the resolved client does not match exactly -- "
            "this triggers a real, billable run, so this is optional but strongly "
            "recommended whenever the caller has an independent expected-host value "
            "(see .github/workflows/lab09_azure_deployment.yml's AZURE_PROD_EXPECTED_HOST)."
        ),
    )
    parser.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH), help="Path to config/azure.yml."
    )
    parser.add_argument("--job-timeout-seconds", type=int, default=1800)
    parser.add_argument("--job-poll-interval-seconds", type=int, default=15)
    parser.add_argument("--termination-timeout-seconds", type=int, default=600)
    parser.add_argument("--termination-poll-interval-seconds", type=int, default=10)
    parser.add_argument("--report-path", default="evidence/azure_run_report.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    client = get_workspace_client(args.profile)

    if args.confirm_host is not None:
        actual_host = normalize_host(client.config.host)
        expected_host = normalize_host(args.confirm_host)
        if actual_host != expected_host:
            parser.error(
                f"Profile {args.profile!r} resolved to a host that does not match "
                f"--confirm-host {expected_host!r}. Refusing to trigger a run without "
                "an exact match."
            )

    cfg = load_config(args.config)

    report = run_and_verify(
        client,
        cfg,
        job_timeout_seconds=args.job_timeout_seconds,
        job_poll_interval_seconds=args.job_poll_interval_seconds,
        termination_timeout_seconds=args.termination_timeout_seconds,
        termination_poll_interval_seconds=args.termination_poll_interval_seconds,
    )

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0 if report.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())

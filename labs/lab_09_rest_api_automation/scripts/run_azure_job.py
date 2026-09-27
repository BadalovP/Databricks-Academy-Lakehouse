"""Trigger, monitor, and report on ONE run of LAB 09's Azure three-task Job.

Never creates or updates the Job itself -- see deploy_azure_job.py for that.
This only finds the persistent `lab09_taxi_reconciliation_job` by name,
triggers exactly one run (no retry loop), polls it to a terminal state,
retrieves each task's own output, and independently verifies the run's own
job-cluster instance actually reached TERMINATED afterward -- a run
finishing is not, by itself, treated as proof its compute is gone (the same
"confirmed vs. merely not raised" distinction already established in
compute.terminate_and_verify_cluster()).

`succeeded` is deliberately strict: this Job's ingestion and reconciliation
tasks are always expected to run on the shared job cluster (never
serverless), so a SUCCESS result with no observed job-cluster id is treated
as an anomaly in its own right (monitoring failed to identify the cluster
that must have existed), not as "nothing to clean up." Every one of
ingestion output, the pipeline task's own result, reconciliation output
(including its own reconciliation_passed field, not just "some JSON came
back"), and confirmed cluster termination must each independently hold --
`result_state` and `life_cycle_state` are never overwritten by any of this,
so the actual Databricks-reported status is always visible even when the
combined verdict is False.

A poll timeout is never assumed to mean the underlying Databricks run (or
its compute) has stopped: it means only that this script's own polling
loop gave up waiting. On a timeout, one further read-only status check is
made immediately and surfaced as an explicit warning -- the caller must
still investigate and, if necessary, escalate to a human rather than
silently walking away from what may be a still-running, still-billing job.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from lab09 import jobs, monitoring
from lab09.client import get_workspace_client, load_config, normalize_host

logger = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = LAB_ROOT / "config" / "azure.yml"


def _state_name(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "value", value) or None


@dataclass
class RunReport:
    job_id: int | None = None
    run_id: int | None = None
    life_cycle_state: str | None = None
    result_state: str | None = None
    timed_out: bool | None = None

    ingestion_task_life_cycle_state: str | None = None
    ingestion_task_result_state: str | None = None
    pipeline_task_life_cycle_state: str | None = None
    pipeline_task_result_state: str | None = None
    reconciliation_task_life_cycle_state: str | None = None
    reconciliation_task_result_state: str | None = None

    ingestion_output: dict[str, Any] | None = None
    ingestion_output_error: str | None = None
    reconciliation_output: dict[str, Any] | None = None
    reconciliation_output_error: str | None = None
    reconciliation_passed: bool | None = None

    job_cluster_id: str | None = None
    job_cluster_terminated_confirmed: bool | None = None
    job_cluster_final_state: str | None = None

    post_timeout_live_state: str | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def succeeded(self) -> bool:
        """Every one of these must independently hold -- see module
        docstring for the full reasoning behind each check.
        """
        if self.error is not None:
            return False
        if self.result_state != "SUCCESS" or self.timed_out:
            return False
        if self.ingestion_task_result_state != "SUCCESS":
            return False
        if self.pipeline_task_result_state != "SUCCESS":
            return False
        if self.reconciliation_task_result_state != "SUCCESS":
            return False
        if self.ingestion_output is None or self.ingestion_output_error is not None:
            return False
        if self.reconciliation_output is None or self.reconciliation_output_error is not None:
            return False
        if self.reconciliation_passed is not True:
            return False
        if self.job_cluster_id is None:
            # This Job's ingestion/reconciliation tasks always use the shared
            # job cluster -- a SUCCESS result with no observed cluster id
            # means this script's own monitoring failed to identify it, not
            # that there was nothing to verify.
            return False
        if self.job_cluster_terminated_confirmed is not True:
            return False
        return True


def _task_states(task_runs: list[Any], task_key: str) -> tuple[str | None, str | None]:
    for t in task_runs:
        if t.task_key == task_key:
            life_cycle = (
                _state_name(getattr(t.state, "life_cycle_state", None)) if t.state else None
            )
            result = _state_name(getattr(t.state, "result_state", None)) if t.state else None
            return life_cycle, result
    return None, None


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
    ingestion_key = job_cfg["ingestion_task_key"]
    pipeline_key = job_cfg["pipeline_task_key"]
    reconciliation_key = job_cfg["reconciliation_task_key"]

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

    # Fetched exactly once, with error handling, and reused below for both the
    # timeout-escalation check and the per-task inspection -- a lookup
    # failure here must never crash reporting, whether it happens as part of
    # a timeout escalation or an ordinary terminal-state run.
    run_details = None
    run_details_error: str | None = None
    try:
        run_details = client.jobs.get_run(run_id=run_id)
    except Exception as exc:  # noqa: BLE001 - a lookup failure must not crash reporting
        run_details_error = str(exc)

    if outcome.timed_out:
        # This script's own polling gave up -- that is never the same claim
        # as "the Databricks run has stopped". This lookup (already made
        # above) is surfaced loudly, so a human knows to investigate (and,
        # if necessary, manually cancel) a run that may still be active and
        # billing rather than this simply going unnoticed.
        if run_details is not None:
            live_state = _state_name(getattr(run_details.state, "life_cycle_state", None))
        else:
            live_state = f"<lookup failed: {run_details_error}>"
        report.post_timeout_live_state = live_state
        warning = (
            f"Polling timed out after {job_timeout_seconds}s, but run {run_id} was last "
            f"observed as life_cycle_state={live_state!r} -- this is NOT confirmation the "
            "run has stopped. It may still be active and billing. Investigate and cancel "
            "manually if necessary; do not assume this timeout ended it, and do not "
            "automatically retry or dispatch another run."
        )
        report.warnings.append(warning)
        logger.warning(warning)

    task_runs = run_details.tasks or [] if run_details is not None else []

    report.ingestion_task_life_cycle_state, report.ingestion_task_result_state = _task_states(
        task_runs, ingestion_key
    )
    report.pipeline_task_life_cycle_state, report.pipeline_task_result_state = _task_states(
        task_runs, pipeline_key
    )
    (
        report.reconciliation_task_life_cycle_state,
        report.reconciliation_task_result_state,
    ) = _task_states(task_runs, reconciliation_key)

    cluster_ids = {
        t.task_key: getattr(t.cluster_instance, "cluster_id", None)
        for t in task_runs
        if getattr(t, "cluster_instance", None) is not None
    }
    report.job_cluster_id = cluster_ids.get(ingestion_key) or cluster_ids.get(reconciliation_key)

    if report.ingestion_task_result_state == "SUCCESS":
        try:
            report.ingestion_output = jobs.get_run_output_json(client, run_id, ingestion_key)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - a retrieval failure must not mask the run's own result
            report.ingestion_output_error = str(exc)
            logger.warning("Failed to retrieve ingestion output for run %s: %s", run_id, exc)

    if report.reconciliation_task_result_state == "SUCCESS":
        try:
            report.reconciliation_output = jobs.get_run_output_json(
                client, run_id, reconciliation_key
            )
            report.reconciliation_passed = report.reconciliation_output.get("reconciliation_passed")
        except Exception as exc:  # noqa: BLE001
            report.reconciliation_output_error = str(exc)
            logger.warning("Failed to retrieve reconciliation output for run %s: %s", run_id, exc)

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
            warning = (
                f"Job cluster {report.job_cluster_id} termination not confirmed "
                f"(last state={termination_outcome.state}"
                f"{f', error={termination_outcome.error}' if termination_outcome.error else ''}) "
                "-- requires manual investigation. Do not assume it has stopped billing."
            )
            report.warnings.append(warning)
            logger.warning(warning)
    elif report.result_state == "SUCCESS" and not report.timed_out:
        warning = (
            "Run reported SUCCESS but no job-cluster instance was observed on the "
            f"{ingestion_key!r}/{reconciliation_key!r} tasks -- this Job always uses a "
            "shared job cluster for those tasks, so this is treated as a monitoring "
            "anomaly, not as evidence there was nothing to clean up."
        )
        report.warnings.append(warning)
        logger.warning(warning)

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
            "(see .github/workflows/lab09.yml's AZURE_PROD_EXPECTED_HOST)."
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
    for warning in report.warnings:
        print(f"WARNING: {warning}", file=sys.stderr)
    return 0 if report.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())

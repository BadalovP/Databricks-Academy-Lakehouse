"""argparse CLI for LAB 09.

python -m lab09.cli preflight [--probe-cluster-create]
python -m lab09.cli run-all
python -m lab09.cli cleanup [--reset-landing] [--reset-reference]
python -m lab09.cli status
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from . import (
    compute,
    jobs,
    landing,
    monitoring,
    pipelines,
    preflight,
    reporting,
    volumes,
    workspace,
)
from .client import get_workspace_client, load_config

logger = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parents[2]  # .../lab_09_rest_api_automation
DEFAULT_CONFIG_PATH = LAB_ROOT / "config" / "dev.yml"
PIPELINE_SOURCE_DIR = LAB_ROOT / "pipeline"
NOTEBOOK_PATH = LAB_ROOT / "notebooks" / "01_reconcile_counts.py"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lab09.cli")
    parser.add_argument("--profile", default=None, help="Databricks CLI auth profile to use.")
    parser.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH), help="Path to a LAB 09 YAML config file."
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_preflight = sub.add_parser("preflight", help="Run Phase 0 capability checks.")
    p_preflight.add_argument(
        "--probe-cluster-create",
        action="store_true",
        help="Also run the real tiny-cluster create/terminate capability test.",
    )

    sub.add_parser("run-all", help="Run the full Lab 9 live execution order end to end.")

    p_cleanup = sub.add_parser("cleanup", help="Delete only Lab 9-owned resources.")
    p_cleanup.add_argument(
        "--reset-landing",
        action="store_true",
        help="Delete all landed trips files under the Lab 9 volume so land_next_month() starts over.",
    )
    p_cleanup.add_argument(
        "--reset-reference",
        action="store_true",
        help="Also delete reference/taxi_zone_lookup.csv. Only takes effect with --reset-landing.",
    )

    sub.add_parser(
        "status", help="Print current job/pipeline/landing status without changing anything."
    )

    return parser


def cmd_preflight(client: WorkspaceClient, cfg: dict[str, Any], args: argparse.Namespace) -> int:
    report = preflight.run_preflight(client, cfg, probe_cluster_create=args.probe_cluster_create)
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0 if report.passed else 1


def cmd_cleanup(client: WorkspaceClient, cfg: dict[str, Any], args: argparse.Namespace) -> int:
    if not args.reset_landing:
        print("Nothing to do: pass --reset-landing to delete Lab 9-owned landing files.")
        return 0
    deleted = landing.reset_landing(client, cfg, reset_reference=args.reset_reference)
    print(f"Deleted {len(deleted)} file(s):")
    for path in deleted:
        print(f"  {path}")
    return 0


def cmd_status(client: WorkspaceClient, cfg: dict[str, Any], args: argparse.Namespace) -> int:
    landed = landing.list_landed_months(client, cfg)
    next_month = landing.next_missing_month(cfg["months"], landed)
    pipeline = pipelines.find_pipeline_by_name(client, cfg["pipeline"]["name"])
    job = jobs.find_job_by_name(client, cfg["job"]["name"])
    status = {
        "landed_months": sorted(landed),
        "next_missing_month": next_month,
        "pipeline_id": pipeline.pipeline_id if pipeline else None,
        "job_id": job.job_id if job else None,
    }
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


def _finish(
    client: WorkspaceClient, report: reporting.Report, cluster_id: str | None, cfg: dict[str, Any]
) -> int:
    """OUTER SAFETY: always terminate the temporary cluster and always write the report."""
    cluster_cleaned_up = True
    if cluster_id:
        try:
            if compute.cluster_exists_and_active(client, cluster_id):
                compute.terminate_cluster(client, cluster_id)
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask the original outcome
            logger.warning("Failed to terminate cluster %s during cleanup: %s", cluster_id, exc)
            cluster_cleaned_up = False
    report.cluster_cleaned_up = cluster_cleaned_up

    report.mark_finished()
    reporting.write_report(report, cfg["report"]["output_path"])

    return reporting.exit_code_for(report)


def cmd_run_all(client: WorkspaceClient, cfg: dict[str, Any], args: argparse.Namespace) -> int:
    report = reporting.Report()
    report.mark_started()
    cluster_id: str | None = None

    try:
        # 1. Authenticate + preflight (core checks only -- the optional real
        # tiny-cluster create/terminate probe is a separate, explicit,
        # opt-in `preflight --probe-cluster-create` command, not something
        # run-all triggers implicitly on every execution).
        preflight_report = preflight.run_preflight(client, cfg, probe_cluster_create=False)
        if not preflight_report.passed:
            failing = [c.name for c in preflight_report.checks if not c.passed]
            report.status = "FAILED"
            report.error = f"Preflight failed before any resources were touched: {failing}"
            return _finish(client, report, cluster_id, cfg)

        # 2. Create-or-get volume
        volumes.ensure_volume(client, cfg)

        # 3-5. Determine next month, download+validate, upload via Files API
        landing_result = landing.land_next_month(client, cfg)
        report.status = landing_result.status
        report.month = landing_result.month
        report.month_landed = landing_result.month_landed
        report.file_bytes = landing_result.file_bytes
        report.volume_path = landing_result.volume_path

        # 6. Ensure reference CSV exists (uploaded once, never re-downloaded)
        landing.ensure_reference_csv(client, cfg)

        # 7. Upload notebook and all pipeline source files
        workspace.upload_pipeline_sources(client, PIPELINE_SOURCE_DIR, cfg)
        notebook_path = workspace.upload_notebook(client, NOTEBOOK_PATH, cfg)
        pipeline_dir_ws = workspace.pipeline_source_dir(client, cfg)

        # 8. Create-or-get Lakeflow pipeline (serverless, classic fallback if rejected)
        pipeline_id, used_serverless = pipelines.ensure_pipeline(client, cfg, pipeline_dir_ws)
        report.pipeline_id = pipeline_id
        report.pipeline_serverless = used_serverless

        # 9. Start pipeline update
        update_id = pipelines.start_update(client, pipeline_id)
        report.update_id = update_id

        # Optional optimization: start the temporary notebook-job cluster now
        # so its provisioning overlaps the pipeline update's runtime, instead
        # of waiting for the pipeline to finish first.
        cluster_spec = compute.build_cluster_spec(client, cfg)
        cluster_id = compute.start_cluster_create(
            client, cluster_spec, cluster_name="lab09-temp-notebook-cluster"
        )
        report.cluster_id = cluster_id

        # 10. Poll pipeline explicitly until terminal
        monitoring_cfg = cfg["monitoring"]
        pipeline_outcome = monitoring.poll_pipeline_update(
            client,
            pipeline_id,
            update_id,
            timeout_seconds=monitoring_cfg["pipeline_timeout_seconds"],
            poll_interval_seconds=monitoring_cfg["pipeline_poll_interval_seconds"],
        )
        if not pipeline_outcome.succeeded:
            report.status = "FAILED"
            report.error = (
                f"Pipeline update did not complete successfully: "
                f"state={pipeline_outcome.state} timed_out={pipeline_outcome.timed_out}"
            )
            return _finish(client, report, cluster_id, cfg)

        # 11. Create/wait for temporary notebook cluster (creation started above)
        cluster_outcome = monitoring.poll_cluster_state(
            client,
            cluster_id,
            timeout_seconds=monitoring_cfg["cluster_timeout_seconds"],
            poll_interval_seconds=monitoring_cfg["cluster_poll_interval_seconds"],
        )
        if not cluster_outcome.usable:
            report.status = "FAILED"
            report.error = (
                f"Temporary cluster did not become usable: "
                f"state={cluster_outcome.state} timed_out={cluster_outcome.timed_out}"
            )
            return _finish(client, report, cluster_id, cfg)

        # 12. Find/create persistent Lab 9 Databricks Job
        job_id = jobs.ensure_job(client, cfg, notebook_path, cluster_id=cluster_id)
        report.job_id = job_id

        # 13. RESET the existing job to point at the CURRENT cluster ID
        jobs.reset_job_cluster(client, job_id, cfg, notebook_path, cluster_id=cluster_id)

        # 14. run_now()
        run_id = jobs.run_job_now(client, job_id)
        report.run_id = run_id

        # 15. poll job explicitly
        job_outcome = monitoring.poll_job_run(
            client,
            run_id,
            timeout_seconds=monitoring_cfg["job_timeout_seconds"],
            poll_interval_seconds=monitoring_cfg["job_poll_interval_seconds"],
        )
        if not job_outcome.succeeded:
            report.status = "FAILED"
            report.error = (
                f"Reconciliation job did not succeed: "
                f"life_cycle_state={job_outcome.life_cycle_state} "
                f"result_state={job_outcome.result_state}"
            )
            return _finish(client, report, cluster_id, cfg)

        # 16. read reconciliation output via get_run_output(task_run_id)
        output = jobs.get_run_output_json(client, run_id, cfg["job"]["task_key"])
        report.bronze_rows = output.get("bronze_rows")
        report.silver_valid_rows = output.get("silver_valid_rows")
        report.rejected_rows = output.get("rejected_rows")
        report.gold_rows = output.get("gold_rows")
        report.failed_rules = output.get("failed_rules", {})
        report.reconciliation_passed = output.get("reconciliation_passed")

        if report.status != "NO_NEW_DATA":
            report.status = "SUCCESS"

        # 17-19. terminate cluster, generate report, return exit code
        return _finish(client, report, cluster_id, cfg)

    except Exception as exc:  # noqa: BLE001 - orchestration must still clean up and report
        logger.exception("run-all failed: %s", exc)
        report.status = "FAILED"
        report.error = str(exc)
        return _finish(client, report, cluster_id, cfg)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    client = get_workspace_client(args.profile)

    if args.command == "preflight":
        return cmd_preflight(client, cfg, args)
    if args.command == "run-all":
        return cmd_run_all(client, cfg, args)
    if args.command == "cleanup":
        return cmd_cleanup(client, cfg, args)
    if args.command == "status":
        return cmd_status(client, cfg, args)

    parser.error(f"Unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    sys.exit(main())

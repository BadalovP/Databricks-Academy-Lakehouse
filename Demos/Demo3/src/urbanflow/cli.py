"""Command-line entry point for safe UrbanFlow validation and bounded publishing."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from urbanflow.automation import (
    select_ready_existing_cluster,
    verify_workspace,
    workspace_client,
)
from urbanflow.config import load_config
from urbanflow.gbfs_client import GBFSClient
from urbanflow.producer import (
    EventHubsPublisher,
    new_execution_id,
    publish_snapshot,
    run_bounded,
)
from urbanflow.reporting import reconcile_producer_and_bronze, write_json_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="urbanflow")
    parser.add_argument("--config", default="config/dev.yml")
    subcommands = parser.add_subparsers(dest="command", required=True)

    subcommands.add_parser("validate-config", help="Validate local YAML only.")
    subcommands.add_parser("validate-sources", help="Read the public APIs; no Azure resources.")

    status = subcommands.add_parser("workspace-status", help="Read-only Databricks identity check.")
    status.add_argument("--profile", required=True)

    compute = subcommands.add_parser(
        "compute-status", help="Read-only GP1/GP2 compatibility and readiness check."
    )
    compute.add_argument("--profile", required=True)
    compute.add_argument("--require-ready", action="store_true")

    produce = subcommands.add_parser("produce", help="Publish a small bounded Event Hubs sample.")
    produce.add_argument("--poll-count", type=int, default=1)
    produce.add_argument("--execution-id")
    produce.add_argument("--report-path", type=Path)
    produce.add_argument("--confirm-publish", action="store_true")

    reconcile = subcommands.add_parser(
        "reconcile-reports", help="Compare producer and Bronze JSON reports offline."
    )
    reconcile.add_argument("--producer-report", type=Path, required=True)
    reconcile.add_argument("--bronze-report", type=Path, required=True)
    reconcile.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    cfg = load_config(Path(args.config))

    if args.command == "validate-config":
        print(
            json.dumps(
                {
                    "project": cfg.project_name,
                    "environment": cfg.environment,
                    "expected_host": cfg.azure.expected_databricks_host,
                    "volume_root": cfg.azure.volume_root,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "validate-sources":
        client = GBFSClient(cfg.sources.gbfs_discovery_url, language=cfg.sources.gbfs_language)
        information = client.fetch_station_information()
        status = client.fetch_station_status()
        print(
            json.dumps(
                {
                    "station_information_count": len(information.stations),
                    "station_status_count": len(status.stations),
                    "ttl_seconds": status.ttl_seconds,
                    "version": status.version,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "workspace-status":
        client = workspace_client(args.profile, cfg.azure.expected_databricks_host)
        identity = verify_workspace(client, cfg.azure.expected_databricks_host)
        print(json.dumps(identity.__dict__, indent=2))
        return 0

    if args.command == "compute-status":
        client = workspace_client(args.profile, cfg.azure.expected_databricks_host)
        verify_workspace(client, cfg.azure.expected_databricks_host)
        selected, assessments = select_ready_existing_cluster(client, cfg.compute)
        print(
            json.dumps(
                {
                    "selected": asdict(selected) if selected else None,
                    "clusters": [asdict(assessment) for assessment in assessments],
                    "mutation_policy": {
                        "allow_start": cfg.compute.allow_start,
                        "allow_restart": cfg.compute.allow_restart,
                        "allow_resize": cfg.compute.allow_resize,
                        "allow_terminate": cfg.compute.allow_terminate,
                    },
                },
                indent=2,
            )
        )
        if args.require_ready and selected is None:
            raise SystemExit("No compatible shared cluster is already RUNNING.")
        return 0

    if args.command == "produce":
        if not args.confirm_publish:
            raise SystemExit("Refusing to publish without --confirm-publish.")
        client = GBFSClient(cfg.sources.gbfs_discovery_url, language=cfg.sources.gbfs_language)
        publisher = EventHubsPublisher.from_environment()
        if publisher.event_hub_name != cfg.azure.event_hub_name:
            raise SystemExit(
                "AZURE_EVENTHUB_NAME does not match the configured UrbanFlow Event Hub."
            )
        execution_id = args.execution_id or new_execution_id()
        if args.poll_count == 1:
            report = publish_snapshot(
                client,
                publisher,
                execution_id=execution_id,
                max_publish_events=cfg.streaming.max_publish_events,
            )
            report_data = report.as_dict()
            if args.report_path:
                write_json_report(report_data, args.report_path)
            print(json.dumps(report_data, indent=2))
            return 0
        if args.report_path:
            raise SystemExit("--report-path is supported only for the one-snapshot live test.")
        total = run_bounded(
            client,
            publisher,
            poll_count=args.poll_count,
            minimum_interval_seconds=cfg.streaming.minimum_poll_interval_seconds,
            max_publish_events=cfg.streaming.max_publish_events,
            execution_id=execution_id,
        )
        print(
            json.dumps(
                {
                    "execution_id": execution_id,
                    "published_events": total,
                    "poll_count": args.poll_count,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "reconcile-reports":
        producer_report = json.loads(args.producer_report.read_text(encoding="utf-8"))
        bronze_report = json.loads(args.bronze_report.read_text(encoding="utf-8"))
        report = reconcile_producer_and_bronze(producer_report, bronze_report)
        if args.output:
            write_json_report(report, args.output)
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "PASS" else 1

    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

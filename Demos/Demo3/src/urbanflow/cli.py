"""Command-line entry point for safe UrbanFlow validation and bounded publishing."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

from urbanflow.automation import verify_workspace, workspace_client
from urbanflow.config import load_config
from urbanflow.gbfs_client import GBFSClient
from urbanflow.producer import EventHubsPublisher, run_bounded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="urbanflow")
    parser.add_argument("--config", default="config/dev.yml")
    subcommands = parser.add_subparsers(dest="command", required=True)

    subcommands.add_parser("validate-config", help="Validate local YAML only.")
    subcommands.add_parser("validate-sources", help="Read the public APIs; no Azure resources.")

    status = subcommands.add_parser("workspace-status", help="Read-only Databricks identity check.")
    status.add_argument("--profile", required=True)

    produce = subcommands.add_parser("produce", help="Publish a small bounded Event Hubs sample.")
    produce.add_argument("--poll-count", type=int, default=1)
    produce.add_argument("--confirm-publish", action="store_true")
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

    if args.command == "produce":
        if not args.confirm_publish:
            raise SystemExit("Refusing to publish without --confirm-publish.")
        client = GBFSClient(cfg.sources.gbfs_discovery_url, language=cfg.sources.gbfs_language)
        publisher = EventHubsPublisher.from_environment()
        total = run_bounded(
            client,
            publisher,
            poll_count=args.poll_count,
            minimum_interval_seconds=cfg.streaming.minimum_poll_interval_seconds,
        )
        print(json.dumps({"published_events": total, "poll_count": args.poll_count}, indent=2))
        return 0

    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

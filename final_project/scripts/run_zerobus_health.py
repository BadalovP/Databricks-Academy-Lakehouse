"""Optional: send synthetic station-health readings to the Final Project Bronze table via Zerobus.

Dry run by default: it prints the plan (tables, batches, event IDs) and sends nothing. Sending
needs ``--send``, the confirmation phrase, the workspace's Zerobus endpoint, and the Final
Project's own service principal in URBANFLOW_FINAL_ZEROBUS_CLIENT_ID / _CLIENT_SECRET. Lab 11's
identity, schemas and tables are refused. The secret is held in memory only and never printed.

Example (dry run)::

    python scripts/run_zerobus_health.py --target personal_dev --run-id fp-health-001
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.zerobus import (  # noqa: E402
    BRONZE_TABLE,
    assert_final_project_target,
    build_batches,
    deliver,
    final_project_credentials,
    generate_health_events,
)

CONFIRMATION = "SEND_URBANFLOW_FINAL_ZEROBUS"
APPLICATION_NAME = "urbanflow-final-zerobus/0.1"
# Mirrors the bundle targets in databricks.yml.
TARGETS = {
    "personal_dev": ("https://dbc-1750318a-76a9.cloud.databricks.com", "workspace",
                     "urbanflow_final_dev"),
    "azure_prod": ("https://adb-7405618361895520.0.azuredatabricks.net", "dbr_dev",
                   "parvinbadalov_urbanflow_final_prod"),
}  # fmt: skip


def sample_station_ids(count: int) -> list[str]:
    path = PROJECT_ROOT / "fixtures" / "samples" / "station_information.sample.json"
    stations = json.loads(path.read_text(encoding="utf-8"))["data"]["stations"]
    return [station["station_id"] for station in stations[:count]]


def plan(args: argparse.Namespace) -> dict:
    workspace_url, catalog, schema = TARGETS[args.target]
    table = assert_final_project_target(catalog, schema, BRONZE_TABLE)
    events = generate_health_events(
        run_id=args.run_id,
        station_ids=sample_station_ids(args.stations),
        readings_per_station=args.readings,
    )
    clock = iter(range(1_760_000_000_000_000, 1_760_000_100_000_000, 1_000_000))
    batches = build_batches(
        events, replay_count=args.replay, batch_size=args.batch_size, clock=lambda: next(clock)
    )
    return {"workspace_url": workspace_url, "table": table, "events": events, "batches": batches}


def open_stream_factory(endpoint: str, workspace_url: str, table: str, credentials: tuple):
    def open_stream():
        from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        sdk = ZerobusSdk(endpoint, workspace_url, APPLICATION_NAME)
        options = StreamConfigurationOptions(
            flush_timeout_ms=60_000, server_lack_of_ack_timeout_ms=60_000
        )
        return sdk.create_stream(*credentials, TableProperties(table), options)

    return open_stream


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stations", type=int, default=4)
    parser.add_argument("--readings", type=int, default=3)
    parser.add_argument("--replay", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=6)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--zerobus-endpoint", default="")
    args = parser.parse_args(argv)

    run = plan(args)
    summary = {
        "target": args.target,
        "table": run["table"],
        "unique_events": len(run["events"]),
        "batches": [(b.batch_id, b.delivery, len(b.records)) for b in run["batches"]],
        "data": "synthetic",
    }
    if not args.send:
        print(json.dumps({"mode": "DRY_RUN", **summary}, indent=2))
        return 0
    if args.confirm != CONFIRMATION or not args.zerobus_endpoint.strip():
        parser.error(f"--send needs --confirm {CONFIRMATION} and --zerobus-endpoint.")
    credentials = final_project_credentials(os.environ)
    started = time.monotonic()
    acknowledged = deliver(
        open_stream_factory(
            args.zerobus_endpoint.strip(), run["workspace_url"], run["table"], credentials
        ),
        run["batches"],
    )
    summary.update(acknowledged=acknowledged, seconds=round(time.monotonic() - started, 3))
    print(json.dumps({"mode": "SENT", **summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

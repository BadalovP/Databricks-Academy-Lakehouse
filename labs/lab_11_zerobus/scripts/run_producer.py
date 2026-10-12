"""Send one bounded Lab 11 run to Zerobus Ingest, or show what would be sent.

    python scripts/run_producer.py --target personal_dev --run-id run-001 --dry-run
    python scripts/run_producer.py --target personal_dev --run-id run-001 --live --confirm-live personal_dev

A live run needs ZEROBUS_CLIENT_ID and ZEROBUS_CLIENT_SECRET in the environment. It sends at most
30 records, waits for the durability acknowledgment of every batch, and writes a sanitized
manifest of exactly what was sent to evidence/.
"""

from __future__ import annotations

import argparse
import json
import sys

from _common import setup_logging, utc_now, write_evidence

from lab11.config import Credentials, load_config
from lab11.ingestion import IngestionError, ZerobusSink
from lab11.producer import build_plan, fixed_clock, wall_clock_micros


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", required=True)
    parser.add_argument("--run-id", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", help="validate and print the plan; no network"
    )
    mode.add_argument("--live", action="store_true", help="send through Zerobus Ingest")
    parser.add_argument("--confirm-live", default="", help="must repeat the target name for --live")
    args = parser.parse_args(argv)

    setup_logging()
    config = load_config()
    target = config.target(args.target)

    if args.dry_run:
        plan = build_plan(config.producer, args.run_id, clock=fixed_clock(1_791_000_000_000_000))
        print(
            json.dumps(
                {
                    "mode": "dry-run (no network writes)",
                    "target_table": target.bronze_table,
                    "zerobus_endpoint": target.zerobus_endpoint,
                    **plan.summary(),
                    "first_record": plan.records[0],
                },
                indent=2,
            )
        )
        return 0

    if args.confirm_live != args.target:
        parser.error("--live requires --confirm-live with the same value as --target")
    credentials = Credentials.from_env()
    plan = build_plan(config.producer, args.run_id, clock=wall_clock_micros)
    started = utc_now()
    try:
        report = ZerobusSink(target, credentials, config.transport).send(plan.batches)
    except IngestionError as exc:
        write_evidence(
            f"producer_{args.target}_{args.run_id}_FAILED.json",
            {
                "target": args.target,
                "table": target.bronze_table,
                "started_at": started,
                "error": str(exc),
                "plan": plan.summary(),
            },
        )
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    path = write_evidence(
        f"producer_{args.target}_{args.run_id}.json",
        {
            "target": args.target,
            "workspace_url": target.workspace_url,
            "table": target.bronze_table,
            "started_at": started,
            "acknowledged_at": utc_now(),
            "plan": plan.summary(),
            "delivery": report.to_dict(),
            "records": plan.records,
        },
    )
    print(
        f"acknowledged {report.records_acknowledged} records in {len(report.batches)} batches; manifest: {path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

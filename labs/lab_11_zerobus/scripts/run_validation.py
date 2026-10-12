"""Verify one live run end to end and demonstrate idempotent Silver processing.

    python scripts/run_validation.py --target personal_dev --run-id run-001 [--warehouse-id ID]

Reads the producer manifest from evidence/, then (bounded, read-mostly):

1. polls Bronze until every acknowledged delivery is queryable, measuring time to table;
2. compares every Bronze row with the record that was sent;
3. runs the Silver MERGE twice - the second run must insert and update nothing;
4. checks Silver against the Python reference policy and runs sql/02_validation.sql;
5. confirms the schema holds only the two Lab 11 tables;

and writes the results to evidence/. The only write is the Silver MERGE.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from _common import EVIDENCE, setup_logging, utc_now, write_evidence
from setup_tables import pick_warehouse

from lab11.config import load_config
from lab11.idempotency import SILVER_COLUMNS, canonical_rows
from lab11.verification import (
    METRICS,
    StatementRunner,
    bronze_rows_sql,
    compare_metrics,
    compare_payloads,
    render_file,
    split_statements,
    workspace_client,
)


def _seconds_since(iso: str) -> float:
    return round(time.time() - datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp(), 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--warehouse-id")
    parser.add_argument("--max-wait-seconds", type=int, default=180)
    args = parser.parse_args(argv)
    setup_logging()
    target = load_config().target(args.target)
    manifest = json.loads(
        (EVIDENCE / f"producer_{args.target}_{args.run_id}.json").read_text(encoding="utf-8")
    )
    sent = manifest["records"]
    unique_ids = {r["event_id"] for r in sent}
    expected = {
        "bronze_rows": len(sent),
        "unique_events": len(unique_ids),
        "duplicate_deliveries": len(sent) - len(unique_ids),
        "events_with_duplicates": sum(
            1 for e in unique_ids if sum(r["event_id"] == e for r in sent) > 1
        ),
        **{
            m: 0
            for m in METRICS
            if m
            not in (
                "bronze_rows",
                "unique_events",
                "duplicate_deliveries",
                "events_with_duplicates",
                "silver_rows",
            )
        },
        "silver_rows": len(unique_ids),
    }

    w = workspace_client(target)
    runner = StatementRunner(w, pick_warehouse(w, args.warehouse_id))
    result: dict[str, object] = {
        "target": args.target,
        "table": target.bronze_table,
        "run_id": args.run_id,
        "started_at": utc_now(),
    }

    # 1. Time to queryable rows (bounded polling).
    count_sql = f"SELECT COUNT(*) AS n FROM {target.bronze_table} WHERE run_id = '{args.run_id}'"
    polls, deadline, landed = 0, time.monotonic() + args.max_wait_seconds, 0
    while True:
        polls += 1
        landed = int(runner.run(count_sql)[0]["n"])
        if landed >= len(sent) or time.monotonic() > deadline:
            break
        time.sleep(5)
    result["materialization"] = {
        "rows_queryable": landed,
        "rows_acknowledged": len(sent),
        "polls": polls,
        "seconds_from_last_ack_to_check": _seconds_since(manifest["acknowledged_at"]),
        "note": "upper bound: includes the time between the producer finishing and this check starting",
    }

    # 2. Payload comparison.
    bronze = runner.run(bronze_rows_sql(target, args.run_id))
    result["payload_problems"] = compare_payloads(sent, bronze)

    # 3. Silver MERGE twice.
    merge_sql = split_statements(render_file("01_deduplication.sql", target))[0]
    silver_cols = ", ".join(
        f"unix_micros({c}) AS {c}" if c in ("event_timestamp", "first_produced_at") else c
        for c in SILVER_COLUMNS
    )
    silver_sql = f"SELECT {silver_cols} FROM {target.silver_table} WHERE run_id = '{args.run_id}' ORDER BY event_id"
    merges = []
    snapshots = []
    for _ in range(2):
        merges.append(runner.run(merge_sql)[0])
        snapshots.append(runner.run(silver_sql))
    result["merge_runs"] = merges
    result["second_merge_wrote_nothing"] = int(merges[1].get("num_affected_rows", -1)) == 0
    result["silver_identical_after_rerun"] = snapshots[0] == snapshots[1]

    # 4. Silver vs the reference policy, and the reconciliation SQL.
    reference = canonical_rows(bronze)
    observed_silver = {row["event_id"]: row for row in snapshots[1]}
    result["silver_matches_reference"] = observed_silver == reference
    metrics = runner.run(
        split_statements(render_file("02_validation.sql", target, args.run_id))[0]
    )[0]
    result["metrics"] = metrics
    result["expected_metrics"] = expected
    result["metric_mismatches"] = compare_metrics(expected, metrics)

    # 5. No other objects in the schema.
    tables = sorted(row["tableName"] for row in runner.run(f"SHOW TABLES IN {target.schema_name}"))
    result["tables_in_schema"] = tables
    result["only_lab11_tables"] = tables == sorted([target.bronze, target.silver])

    result["finished_at"] = utc_now()
    result["status"] = (
        "PASS"
        if (
            landed == len(sent)
            and not result["payload_problems"]
            and result["second_merge_wrote_nothing"]
            and result["silver_identical_after_rerun"]
            and result["silver_matches_reference"]
            and not result["metric_mismatches"]
            and result["only_lab11_tables"]
        )
        else "FAIL"
    )
    path = write_evidence(f"validation_{args.target}_{args.run_id}.json", result)
    print(
        json.dumps(
            {
                k: result[k]
                for k in ("status", "materialization", "merge_runs", "metrics", "metric_mismatches")
            },
            indent=2,
            default=str,
        )
    )
    print("evidence:", path)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

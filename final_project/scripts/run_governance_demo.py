"""Bounded, reversible Unity Catalog governance demonstration on ISOLATED copies.

Nothing here changes a Final Project data table. Every object lives in a dedicated schema,
`<target schema>_governance_demo`, that this script creates and - with `--cleanup` - drops again.
The copies hold public data only: the 40-station development reference sample and up to 1,000
trips of the Job's default historical execution, both read from the target's own schema.

Why a mapping table instead of account groups: creating account groups would broaden access
beyond this project. A row filter that checks `current_user()` against a mapping table in the
demo schema shows BOTH outcomes - filtered and allowed - for one principal, by changing only the
mapping row.

Dry run by default: it prints every statement and connects to nothing. Executing needs
`--execute`, the confirmation phrase, a SQL warehouse ID and a CLI profile:

    python scripts/run_governance_demo.py --target personal_dev
    python scripts/run_governance_demo.py --target personal_dev --execute \\
        --confirm RUN_URBANFLOW_FINAL_GOVERNANCE_DEMO --warehouse-id <id> --profile <profile> \\
        --evidence evidence/governance_demo.json --cleanup

Requires `databricks-sdk` only when executing. Not run in a workspace yet.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import build_dashboard  # noqa: E402

CONFIRMATION = "RUN_URBANFLOW_FINAL_GOVERNANCE_DEMO"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def statements(target: str) -> dict[str, object]:
    """Every statement of the demonstration for one bundle target."""
    names = build_dashboard.targets()[target]
    catalog, schema = names["catalog"], names["schema"]
    if "urbanflow_final" not in schema:
        raise ValueError(f"{schema} is not a Final Project schema.")
    month = build_dashboard.execution_ids()["historical_execution_id"]
    if not all(_IDENTIFIER.fullmatch(value) for value in (catalog, schema)):
        raise ValueError("catalog and schema must be plain identifiers.")
    if not re.fullmatch(r"[A-Za-z0-9._+-]+", month):
        raise ValueError(f"execution id {month!r} is not a safe literal.")
    source, demo_schema = f"{catalog}.{schema}", f"{schema}_governance_demo"
    demo = f"{catalog}.{demo_schema}"
    setup = [
        f"CREATE SCHEMA IF NOT EXISTS {demo} COMMENT 'UrbanFlow 2.0: isolated, disposable "
        "governance demonstration. Copies of public data only; dropped after evidence.'",
        f"CREATE OR REPLACE TABLE {demo}.station_reference AS "
        "SELECT station_id, station_short_name, station_name, latitude, longitude, capacity, "
        "CASE WHEN latitude >= 40.75 THEN 'NORTH' ELSE 'SOUTH' END AS dispatch_zone "
        f"FROM {source}.dim_station_development_sample",
        f"CREATE OR REPLACE TABLE {demo}.trips_slice AS "
        "SELECT ride_id, rideable_type, started_at, ended_at, start_station_id, member_casual "
        f"FROM {source}.silver_historical_trips WHERE execution_id = '{month}' "
        "ORDER BY ride_id LIMIT 1000",
        f"CREATE OR REPLACE TABLE {demo}.zone_access (user_email STRING, dispatch_zone STRING)",
        f"CREATE OR REPLACE FUNCTION {demo}.zone_row_filter(zone STRING) RETURNS BOOLEAN "
        "COMMENT 'A dispatcher sees only zones mapped to them in zone_access.' "
        f"RETURN EXISTS (SELECT 1 FROM {demo}.zone_access a WHERE a.user_email = current_user() "
        "AND (a.dispatch_zone = zone OR a.dispatch_zone = 'ALL'))",
        f"CREATE OR REPLACE FUNCTION {demo}.coordinate_mask(value DOUBLE) RETURNS DOUBLE "
        "COMMENT 'Two decimal places (about 1 km) unless in urbanflow_admins.' "
        "RETURN CASE WHEN is_account_group_member('urbanflow_admins') THEN value "
        "ELSE ROUND(value, 2) END",
        f"CREATE OR REPLACE FUNCTION {demo}.ride_id_mask(value STRING) RETURNS STRING "
        "COMMENT 'Stable one-way SHA-256 hash unless in urbanflow_admins.' "
        "RETURN CASE WHEN is_account_group_member('urbanflow_admins') THEN value "
        "ELSE SHA2(value, 256) END",
    ]
    apply = [
        f"ALTER TABLE {demo}.station_reference SET ROW FILTER {demo}.zone_row_filter "
        "ON (dispatch_zone)",
        f"ALTER TABLE {demo}.station_reference ALTER COLUMN latitude SET MASK "
        f"{demo}.coordinate_mask",
        f"ALTER TABLE {demo}.station_reference ALTER COLUMN longitude SET MASK "
        f"{demo}.coordinate_mask",
        f"ALTER TABLE {demo}.trips_slice ALTER COLUMN ride_id SET MASK {demo}.ride_id_mask",
    ]
    rollback = [
        f"ALTER TABLE {demo}.station_reference DROP ROW FILTER",
        f"ALTER TABLE {demo}.station_reference ALTER COLUMN latitude DROP MASK",
        f"ALTER TABLE {demo}.station_reference ALTER COLUMN longitude DROP MASK",
        f"ALTER TABLE {demo}.trips_slice ALTER COLUMN ride_id DROP MASK",
    ]
    inspect = {
        "column_masks": "SELECT table_name, column_name, mask_name FROM "
        f"{catalog}.information_schema.column_masks WHERE table_schema = '{demo_schema}' "
        "ORDER BY 1, 2",
        "row_filters": "SELECT table_name, filter_name, target_columns FROM "
        f"{catalog}.information_schema.row_filters WHERE table_schema = '{demo_schema}' "
        "ORDER BY 1",
        "data_schema_masks_and_filters": "SELECT "
        f"(SELECT count(*) FROM {catalog}.information_schema.column_masks "
        f"WHERE table_schema = '{schema}') AS masks, "
        f"(SELECT count(*) FROM {catalog}.information_schema.row_filters "
        f"WHERE table_schema = '{schema}') AS filters",
    }
    return {
        "catalog": catalog,
        "demo": demo,
        "demo_schema": demo_schema,
        "setup": setup,
        "apply": apply,
        "rollback": rollback,
        "inspect": inspect,
        "cleanup": f"DROP SCHEMA {demo} CASCADE",
        "station_view": f"SELECT dispatch_zone, count(*) AS stations, min(latitude) AS min_lat, "
        f"max(latitude) AS max_lat FROM {demo}.station_reference GROUP BY 1 ORDER BY 1",
        "trip_view": f"SELECT ride_id FROM {demo}.trips_slice ORDER BY started_at, ride_id LIMIT 2",
    }


def run(client, warehouse_id: str, catalog: str, sql: str) -> list[dict]:
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    response = client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        catalog=catalog,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        message = response.status.error and response.status.error.message
        raise RuntimeError(f"{response.status.state}: {message}\n{sql}")
    columns = [c.name for c in response.manifest.schema.columns] if response.manifest else []
    rows = (response.result.data_array or []) if response.result else []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def execute(plan: dict, args: argparse.Namespace) -> dict:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=args.profile)

    def sql(statement: str) -> list[dict]:
        return run(client, args.warehouse_id, plan["catalog"], statement)

    me = sql("SELECT current_user() AS u")[0]["u"]
    ev: dict = {"principal": "the executing user", "statements": {}}

    def snap(label: str) -> None:
        ev[label] = {name: sql(query) for name, query in plan["inspect"].items()}

    snap("before")
    try:
        for statement in plan["setup"]:
            sql(statement)
        ev["raw_unpoliced"] = {
            "stations": sql(plan["station_view"]),
            "trips": sql(plan["trip_view"]),
        }
        for statement in plan["apply"]:
            sql(statement)
        snap("after_apply")
        ev["filtered_no_mapping"] = sql(plan["station_view"])
        sql(f"INSERT INTO {plan['demo']}.zone_access VALUES ('{me}', 'SOUTH')")
        ev["filtered_south_only"] = sql(plan["station_view"])
        sql(f"INSERT INTO {plan['demo']}.zone_access VALUES ('{me}', 'ALL')")
        ev["allowed_all_zones_coordinates_masked"] = sql(plan["station_view"])
        ev["ride_id_masked"] = sql(plan["trip_view"])
        for statement in plan["rollback"]:
            sql(statement)
        snap("after_rollback")
    except Exception as error:  # record, roll back what may have been applied, then clean up
        ev["error"] = f"{type(error).__name__}: {str(error)[:500]}"
        ev["rollback_after_error"] = []
        for statement in plan["rollback"]:
            try:
                sql(statement)
                ev["rollback_after_error"].append("ok")
            except Exception as rollback_error:  # e.g. the policy was never applied
                ev["rollback_after_error"].append(type(rollback_error).__name__)
    finally:
        if args.cleanup:
            try:
                sql(plan["cleanup"])
                ev["demo_schema_exists_after_cleanup"] = bool(
                    sql(
                        f"SELECT 1 FROM {plan['catalog']}.information_schema.schemata "
                        f"WHERE schema_name = '{plan['demo_schema']}'"
                    )
                )
            except Exception as cleanup_error:
                ev["cleanup_error"] = f"{type(cleanup_error).__name__}: {str(cleanup_error)[:300]}"
        ev["statements"] = {key: plan[key] for key in ("setup", "apply", "rollback")}
    return ev


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", choices=sorted(build_dashboard.targets()), required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--warehouse-id", default="")
    parser.add_argument("--profile", default="")
    parser.add_argument("--evidence", default="")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args(argv)
    plan = statements(args.target)
    if not args.execute:
        print(json.dumps({"mode": "DRY_RUN", **plan}, indent=2))
        return 0
    if args.confirm != CONFIRMATION or not (args.warehouse_id and args.profile and args.evidence):
        parser.error(f"--execute needs --confirm {CONFIRMATION}, --warehouse-id, --profile and "
                     "--evidence.")  # fmt: skip
    evidence = execute(plan, args)
    Path(args.evidence).write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in evidence.items() if k != "statements"}, indent=1)[:6000])
    return 1 if "error" in evidence or "cleanup_error" in evidence else 0


if __name__ == "__main__":
    raise SystemExit(main())

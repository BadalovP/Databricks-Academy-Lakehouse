"""Bounded, reversible Unity Catalog governance demonstration on ISOLATED copies.

Nothing here touches a validated UrbanFlow table. Every object lives in a dedicated schema,
`parvinbadalov_urbanflow_governance_demo`, that this script creates and - with `--cleanup` -
drops again. The copies hold public data only: the 40-station reference sample and 1,000 trips
from the real January 2024 archive.

Why a mapping table instead of account groups: the groups named in `sql/20` do not exist, and
creating account groups would broaden access beyond this project. A row filter that checks
`current_user()` against a Demo3-owned mapping table shows BOTH outcomes - filtered and allowed -
for one principal, by changing only the mapping row.

    python scripts/run_governance_demo.py --evidence evidence/2026-10-03_governance_demo.json --cleanup

Requires `databricks-sdk` and a `dev` CLI profile. Uses the shared SQL warehouse only.
"""

from __future__ import annotations

import argparse
import json
import time

WAREHOUSE = "3ed106620db591d9"
CATALOG = "dbr_dev"
SOURCE = "dbr_dev.parvinbadalov_urbanflow"
DEMO = "dbr_dev.parvinbadalov_urbanflow_governance_demo"
MONTH = "urbanflow-hist-202401-full-gh-37140618864-1"

SETUP = [
    f"CREATE SCHEMA IF NOT EXISTS {DEMO} COMMENT 'UrbanFlow Demo3: isolated, disposable "
    "governance demonstration. Copies of public data only; dropped after evidence.'",
    f"CREATE OR REPLACE TABLE {DEMO}.station_reference AS "
    "SELECT station_id, station_short_name, station_name, latitude, longitude, capacity, "
    "CASE WHEN latitude >= 40.75 THEN 'NORTH' ELSE 'SOUTH' END AS dispatch_zone "
    f"FROM {SOURCE}.dim_station_development_sample",
    f"CREATE OR REPLACE TABLE {DEMO}.trips_slice AS "
    "SELECT ride_id, rideable_type, started_at, ended_at, start_station_id, member_casual "
    f"FROM {SOURCE}.silver_historical_trips WHERE execution_id = '{MONTH}' "
    "ORDER BY ride_id LIMIT 1000",
    f"CREATE OR REPLACE TABLE {DEMO}.zone_access (user_email STRING, dispatch_zone STRING)",
    f"CREATE OR REPLACE FUNCTION {DEMO}.zone_row_filter(zone STRING) RETURNS BOOLEAN "
    "COMMENT 'A dispatcher sees only zones mapped to them in zone_access.' "
    f"RETURN EXISTS (SELECT 1 FROM {DEMO}.zone_access a "
    "WHERE a.user_email = current_user() AND (a.dispatch_zone = zone OR a.dispatch_zone = 'ALL'))",
    f"CREATE OR REPLACE FUNCTION {DEMO}.coordinate_mask(value DOUBLE) RETURNS DOUBLE "
    "COMMENT 'Two decimal places (about 1 km) unless in urbanflow_admins.' "
    "RETURN CASE WHEN is_account_group_member('urbanflow_admins') THEN value "
    "ELSE ROUND(value, 2) END",
    f"CREATE OR REPLACE FUNCTION {DEMO}.ride_id_mask(value STRING) RETURNS STRING "
    "COMMENT 'Stable one-way SHA-256 hash unless in urbanflow_admins.' "
    "RETURN CASE WHEN is_account_group_member('urbanflow_admins') THEN value "
    "ELSE SHA2(value, 256) END",
]

APPLY = [
    f"ALTER TABLE {DEMO}.station_reference SET ROW FILTER {DEMO}.zone_row_filter "
    "ON (dispatch_zone)",
    f"ALTER TABLE {DEMO}.station_reference ALTER COLUMN latitude SET MASK {DEMO}.coordinate_mask",
    f"ALTER TABLE {DEMO}.station_reference ALTER COLUMN longitude SET MASK {DEMO}.coordinate_mask",
    f"ALTER TABLE {DEMO}.trips_slice ALTER COLUMN ride_id SET MASK {DEMO}.ride_id_mask",
]

ROLLBACK = [
    f"ALTER TABLE {DEMO}.station_reference DROP ROW FILTER",
    f"ALTER TABLE {DEMO}.station_reference ALTER COLUMN latitude DROP MASK",
    f"ALTER TABLE {DEMO}.station_reference ALTER COLUMN longitude DROP MASK",
    f"ALTER TABLE {DEMO}.trips_slice ALTER COLUMN ride_id DROP MASK",
]

INSPECT = {
    "column_masks": "SELECT table_name, column_name, mask_name FROM "
    "dbr_dev.information_schema.column_masks "
    "WHERE table_schema = 'parvinbadalov_urbanflow_governance_demo' ORDER BY 1, 2",
    "row_filters": "SELECT table_name, filter_name, target_columns FROM "
    "dbr_dev.information_schema.row_filters "
    "WHERE table_schema = 'parvinbadalov_urbanflow_governance_demo' ORDER BY 1",
    "validated_schema_masks_and_filters": "SELECT "
    "(SELECT count(*) FROM dbr_dev.information_schema.column_masks "
    "WHERE table_schema = 'parvinbadalov_urbanflow') AS masks, "
    "(SELECT count(*) FROM dbr_dev.information_schema.row_filters "
    "WHERE table_schema = 'parvinbadalov_urbanflow') AS filters",
    "catalog_grants_to_account_users": "SELECT grantee, privilege_type FROM "
    "dbr_dev.information_schema.catalog_privileges WHERE grantee = 'account users' ORDER BY 2",
}

STATION_VIEW = (
    f"SELECT dispatch_zone, count(*) AS stations, min(latitude) AS min_lat, "
    f"max(latitude) AS max_lat FROM {DEMO}.station_reference GROUP BY 1 ORDER BY 1"
)
TRIP_VIEW = f"SELECT ride_id FROM {DEMO}.trips_slice ORDER BY started_at, ride_id LIMIT 2"


def run(client, sql: str) -> list[dict]:
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    response = client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=WAREHOUSE,
        catalog=CATALOG,
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


def main() -> int:
    from databricks.sdk import WorkspaceClient

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    client = WorkspaceClient(profile="dev")
    me = run(client, "SELECT current_user() AS u")[0]["u"]
    ev: dict = {"principal": "the bundle's workspace_user", "statements": {}}

    def snap(label: str) -> None:
        ev[label] = {name: run(client, sql) for name, sql in INSPECT.items()}

    snap("before")
    for sql in SETUP:
        run(client, sql)
    ev["statements"]["setup"] = SETUP
    ev["raw_unpoliced"] = {"stations": run(client, STATION_VIEW), "trips": run(client, TRIP_VIEW)}
    for sql in APPLY:
        run(client, sql)
    ev["statements"]["apply"] = APPLY
    snap("after_apply")

    ev["filtered_no_mapping"] = run(client, STATION_VIEW)
    run(client, f"INSERT INTO {DEMO}.zone_access VALUES ('{me}', 'SOUTH')")
    ev["filtered_south_only"] = run(client, STATION_VIEW)
    run(client, f"INSERT INTO {DEMO}.zone_access VALUES ('{me}', 'ALL')")
    ev["allowed_all_zones_coordinates_masked"] = run(client, STATION_VIEW)
    ev["ride_id_masked"] = run(client, TRIP_VIEW)

    for sql in ROLLBACK:
        run(client, sql)
    ev["statements"]["rollback"] = ROLLBACK
    snap("after_rollback")
    ev["after_rollback_view"] = {
        "stations": run(client, STATION_VIEW),
        "trips": run(client, TRIP_VIEW),
    }
    if args.cleanup:
        drop = f"DROP SCHEMA {DEMO} CASCADE"
        run(client, drop)
        ev["statements"]["cleanup"] = [drop]
        ev["demo_schema_exists_after_cleanup"] = bool(
            run(
                client,
                "SELECT 1 FROM dbr_dev.information_schema.schemata "
                "WHERE schema_name = 'parvinbadalov_urbanflow_governance_demo'",
            )
        )
    with open(args.evidence, "w", encoding="utf-8") as handle:
        json.dump(ev, handle, indent=2)
    print(json.dumps({k: v for k, v in ev.items() if k != "statements"}, indent=1)[:6000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

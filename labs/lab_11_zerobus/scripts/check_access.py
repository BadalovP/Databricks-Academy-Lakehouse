"""Read-only preflight for one Lab 11 target. Prints facts, never secrets, and changes nothing.

    python scripts/check_access.py --target personal_dev

Checks: caller identity, endpoint DNS, schema and tables (and whether Bronze matches the
contract), the service principal's effective privileges on Bronze when ZEROBUS_CLIENT_ID is set,
and the SQL warehouses the caller can see.
"""

from __future__ import annotations

import argparse
import os
import socket
from urllib.parse import urlparse

from _common import setup_logging

from lab11.config import load_config
from lab11.events import BRONZE_COLUMNS
from lab11.verification import workspace_client

REQUIRED = {"USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", required=True)
    args = parser.parse_args(argv)
    setup_logging()
    target = load_config().target(args.target)
    w = workspace_client(target)
    ok = True

    me = w.current_user.me()
    print(f"caller: {me.user_name} | target: {target.name} ({target.description})")
    host = urlparse(target.zerobus_endpoint).hostname
    try:
        print(f"zerobus endpoint: {target.zerobus_endpoint} -> {socket.gethostbyname(host)}")
    except OSError as exc:
        ok = False
        print(f"zerobus endpoint: {target.zerobus_endpoint} DNS FAILED ({exc})")

    try:
        w.schemas.get(target.schema_name)
        print(f"schema {target.schema_name}: exists")
    except Exception:
        print(f"schema {target.schema_name}: missing (scripts/setup_tables.py creates it)")
    for table in (target.bronze_table, target.silver_table):
        try:
            info = w.tables.get(table)
        except Exception:
            print(f"table {table}: missing")
            continue
        columns = {
            c.name: str(c.type_name.value if hasattr(c.type_name, "value") else c.type_name)
            for c in info.columns
        }
        kind = str(info.table_type.value if hasattr(info.table_type, "value") else info.table_type)
        fmt = str(
            info.data_source_format.value
            if hasattr(info.data_source_format, "value")
            else info.data_source_format
        )
        print(f"table {table}: {kind} {fmt}, columns {list(columns)}")
        matches = list(columns) == list(BRONZE_COLUMNS) and kind == "MANAGED" and fmt == "DELTA"
        if table == target.bronze_table and not matches:
            ok = False
            print("  Bronze does NOT match the contract (managed Delta with the expected columns)")

    client_id = os.environ.get("ZEROBUS_CLIENT_ID", "").strip()
    print(
        "ZEROBUS_CLIENT_ID set:",
        bool(client_id),
        "| ZEROBUS_CLIENT_SECRET set:",
        bool(os.environ.get("ZEROBUS_CLIENT_SECRET")),
    )
    if client_id:
        try:
            granted = set()
            for securable, name in (
                ("catalog", target.catalog),
                ("schema", target.schema_name),
                ("table", target.bronze_table),
            ):
                result = w.grants.get_effective(
                    securable_type=securable, full_name=name, principal=client_id
                )
                for assignment in result.privilege_assignments or []:
                    granted |= {
                        str(p.privilege.value if hasattr(p.privilege, "value") else p.privilege)
                        for p in assignment.privileges or []
                    }
            missing = set() if "ALL_PRIVILEGES" in granted else REQUIRED - granted
            print(
                f"service principal {client_id[:8]}... privileges: {sorted(granted)}; missing: {sorted(missing) or 'none'}"
            )
            ok &= not missing
        except Exception as exc:
            ok = False
            print(f"service principal privileges: could not read ({exc})")

    warehouses = list(w.warehouses.list())
    print(
        "warehouses:",
        [(wh.name, wh.id, str(wh.state.value if wh.state else None)) for wh in warehouses]
        or "none visible",
    )
    print("PREFLIGHT", "OK" if ok else "INCOMPLETE")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

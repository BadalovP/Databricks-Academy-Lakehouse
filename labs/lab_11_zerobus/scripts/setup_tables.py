"""Create the Lab 11 schema, Bronze and Silver tables if they do not exist (never drops anything).

python scripts/setup_tables.py --target personal_dev --confirm personal_dev [--warehouse-id ID]
"""

from __future__ import annotations

import argparse

from _common import setup_logging

from lab11.config import load_config
from lab11.verification import StatementRunner, render_file, split_statements, workspace_client


def pick_warehouse(w, warehouse_id: str | None) -> str:
    if warehouse_id:
        return warehouse_id
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise SystemExit("No SQL warehouse visible to the caller; pass --warehouse-id")
    serverless = [wh for wh in warehouses if wh.enable_serverless_compute]
    return (serverless or warehouses)[0].id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", required=True)
    parser.add_argument("--confirm", default="", help="must repeat the target name")
    parser.add_argument("--warehouse-id")
    args = parser.parse_args(argv)
    if args.confirm != args.target:
        parser.error("--confirm must repeat the --target value")
    setup_logging()
    target = load_config().target(args.target)
    w = workspace_client(target)
    runner = StatementRunner(w, pick_warehouse(w, args.warehouse_id))
    for statement in split_statements(render_file("00_create_tables.sql", target)):
        runner.run(statement)
        print("ok:", " ".join(statement.split())[:90])
    tables = runner.run(f"SHOW TABLES IN {target.schema_name}")
    print("tables now in schema:", sorted(row["tableName"] for row in tables))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

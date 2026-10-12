"""Verification: render the SQL assets, run them, and reconcile results against the delivery plan.

Live queries go through the Databricks SQL Statement Execution API on an existing SQL warehouse,
using the caller's own identity (a CLI profile or Azure CLI) - not the ingesting service
principal. Nothing here writes to Bronze.
"""

from __future__ import annotations

import math
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping

from lab11.config import LAB_ROOT, Target, run_identifier
from lab11.events import BRONZE_COLUMNS
from lab11.producer import DeliveryPlan

SQL_DIR = LAB_ROOT / "sql"
_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")

METRICS = (
    "bronze_rows",
    "unique_events",
    "duplicate_deliveries",
    "events_with_duplicates",
    "conflicting_events",
    "null_required_fields",
    "silver_rows",
    "silver_duplicate_keys",
    "missing_in_silver",
    "unexpected_in_silver",
    "wrong_delivery_counts",
)


def render(sql: str, target: Target, run_id: str | None = None) -> str:
    values = {
        "catalog": target.catalog,
        "schema": target.schema,
        "bronze": target.bronze,
        "silver": target.silver,
    }
    if run_id is not None:
        values["run_id"] = run_identifier(run_id)

    def substitute(match: re.Match) -> str:
        name = match.group(1)
        if name not in values:
            raise KeyError(f"No value for placeholder ${{{name}}}")
        return values[name]

    return _PLACEHOLDER.sub(substitute, sql)


def render_file(name: str, target: Target, run_id: str | None = None) -> str:
    return render((SQL_DIR / name).read_text(encoding="utf-8"), target, run_id)


def split_statements(sql: str) -> list[str]:
    """Statements of a SQL file, without comment lines (the assets contain no ';' in literals)."""
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in body.split(";") if statement.strip()]


def bronze_rows_sql(target: Target, run_id: str) -> str:
    """Bronze rows of one run with timestamps as epoch microseconds, for payload comparison."""
    columns = ", ".join(
        f"unix_micros({name}) AS {name}" if kind == "TIMESTAMP" else name
        for name, kind in BRONZE_COLUMNS.items()
    )
    return (
        f"SELECT {columns} FROM {target.bronze_table} "
        f"WHERE run_id = '{run_identifier(run_id)}' ORDER BY event_id, batch_id"
    )


def expected_metrics(plan: DeliveryPlan) -> dict[str, int]:
    replayed = plan.replayed_event_ids
    unique = len(plan.events)
    return {
        "bronze_rows": len(plan.records),
        "unique_events": unique,
        "duplicate_deliveries": len(plan.records) - unique,
        "events_with_duplicates": len(set(replayed)),
        "conflicting_events": 0,
        "null_required_fields": 0,
        "silver_rows": unique,
        "silver_duplicate_keys": 0,
        "missing_in_silver": 0,
        "unexpected_in_silver": 0,
        "wrong_delivery_counts": 0,
    }


def compare_metrics(expected: Mapping[str, int], observed: Mapping[str, object]) -> list[str]:
    return [
        f"{name}: expected {expected[name]}, observed {observed.get(name)}"
        for name in expected
        if observed.get(name) is None or int(observed[name]) != expected[name]
    ]


def compare_payloads(
    sent: Iterable[Mapping[str, object]], landed: Iterable[Mapping[str, object]]
) -> list[str]:
    """Every sent record must land exactly once per delivery, with identical values."""

    def key(row: Mapping[str, object]) -> tuple[str, str]:
        return str(row["event_id"]), str(row["batch_id"])

    sent_by_key = {key(r): r for r in sent}
    landed_rows = list(landed)
    problems = []
    counts = Counter(key(r) for r in landed_rows)
    for k, n in counts.items():
        if n > 1:
            problems.append(f"delivery {k} landed {n} times")
    for k in sent_by_key.keys() - counts.keys():
        problems.append(f"delivery {k} was acknowledged but is not in Bronze")
    for k in counts.keys() - sent_by_key.keys():
        problems.append(f"unexpected Bronze row {k}")
    for row in landed_rows:
        expected = sent_by_key.get(key(row))
        if expected is None:
            continue
        for column, kind in BRONZE_COLUMNS.items():
            a, b = expected[column], row[column]
            same = math.isclose(float(a), float(b), abs_tol=1e-9) if kind == "DOUBLE" else a == b
            if not same:
                problems.append(f"{key(row)} {column}: sent {a!r}, landed {b!r}")
    return problems


# ---------------------------------------------------------------------------------------------
# Live statement execution (optional dependency: databricks-sdk)
# ---------------------------------------------------------------------------------------------

_CASTS: dict[str, Callable[[str], object]] = {
    "LONG": int,
    "INT": int,
    "SHORT": int,
    "BYTE": int,
    "DOUBLE": float,
    "FLOAT": float,
    "BOOLEAN": lambda v: v.lower() == "true",
}


def workspace_client(target: Target):
    from databricks.sdk import WorkspaceClient

    if target.cli_profile:
        return WorkspaceClient(profile=target.cli_profile)
    return WorkspaceClient(host=target.workspace_url, auth_type=target.auth_type or "azure-cli")


class StatementRunner:
    """Runs one SQL statement at a time on an existing warehouse and returns typed rows."""

    def __init__(
        self,
        client,
        warehouse_id: str,
        *,
        poll_seconds: float = 3.0,
        timeout_seconds: float = 300.0,
    ):
        self._client = client
        self._warehouse_id = warehouse_id
        self._poll = poll_seconds
        self._timeout = timeout_seconds

    def run(self, statement: str) -> list[dict[str, object]]:
        from databricks.sdk.service.sql import StatementState

        api = self._client.statement_execution
        response = api.execute_statement(
            statement=statement, warehouse_id=self._warehouse_id, wait_timeout="30s"
        )
        deadline = time.monotonic() + self._timeout
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                api.cancel_execution(response.statement_id)
                raise TimeoutError(f"statement {response.statement_id} did not finish in time")
            time.sleep(self._poll)
            response = api.get_statement(response.statement_id)
        if response.status.state != StatementState.SUCCEEDED:
            error = response.status.error
            raise RuntimeError(
                f"statement failed: {error.message if error else response.status.state}"
            )
        columns = (
            response.manifest.schema.columns
            if response.manifest and response.manifest.schema
            else []
        )
        data = (response.result.data_array if response.result else None) or []
        rows = []
        for values in data:
            row = {}
            for column, value in zip(columns, values, strict=True):
                kind = str(
                    column.type_name.value
                    if hasattr(column.type_name, "value")
                    else column.type_name
                )
                row[column.name] = None if value is None else _CASTS.get(kind, str)(value)
            rows.append(row)
        return rows

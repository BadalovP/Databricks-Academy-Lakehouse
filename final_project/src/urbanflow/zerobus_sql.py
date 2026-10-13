"""SQL for the optional Zerobus station-health path, one pure function per statement.

Flow (``notebooks/12_zerobus_station_health.py`` and ``scripts/run_zerobus_health.py``):

1. ``create_tables`` creates the Final-Project-owned Bronze and Silver tables (never Lab 11's).
2. The producer script sends synthetic health readings to Bronze through Zerobus Ingest; a replay
   batch re-sends some events with their original ``event_id``, as a retrying producer would.
3. ``merge_silver`` keeps the earliest delivery per ``event_id`` and counts every delivery. A row
   is rewritten only when a value changes, so rerunning it on unchanged Bronze writes nothing.
4. ``validation`` proves, for one run, that Silver holds each event exactly once and that every
   Bronze delivery is accounted for.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .zerobus import BRONZE_TABLE, SILVER_TABLE, assert_final_project_target

HEALTH_COLUMNS = ("station_id", "device_id", "sequence_number", "battery_pct", "vibration_rms",
                  "temperature_c", "run_id")  # fmt: skip
SILVER_COLUMNS = (*HEALTH_COLUMNS, "event_timestamp", "first_batch_id", "first_produced_at",
                  "delivery_count")  # fmt: skip
_RUN_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


@dataclass(frozen=True)
class HealthTables:
    catalog: str
    schema: str

    def __post_init__(self) -> None:
        assert_final_project_target(self.catalog, self.schema, BRONZE_TABLE)

    @property
    def bronze(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{BRONZE_TABLE}`"

    @property
    def silver(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{SILVER_TABLE}`"


def run_id_literal(run_id: str) -> str:
    if not _RUN_ID.fullmatch(run_id or ""):
        raise ValueError(f"run_id must be 1-64 letters, digits, '-' or '_': {run_id!r}")
    return f"'{run_id}'"


def create_tables(tables: HealthTables) -> list[str]:
    """Bronze columns match the producer's record fields exactly, as Zerobus requires."""
    return [
        f"""CREATE TABLE IF NOT EXISTS {tables.bronze} (
  event_id STRING, station_id STRING, device_id STRING, sequence_number BIGINT,
  event_timestamp_micros BIGINT, battery_pct DOUBLE, vibration_rms DOUBLE, temperature_c DOUBLE,
  run_id STRING, batch_id STRING, produced_at_micros BIGINT, synthetic BOOLEAN
) TBLPROPERTIES (project = 'urbanflow_final_project', data = 'synthetic')""",
        f"""CREATE TABLE IF NOT EXISTS {tables.silver} (
  event_id STRING NOT NULL, station_id STRING, device_id STRING, sequence_number BIGINT,
  battery_pct DOUBLE, vibration_rms DOUBLE, temperature_c DOUBLE, run_id STRING,
  event_timestamp TIMESTAMP, first_batch_id STRING, first_produced_at TIMESTAMP,
  delivery_count BIGINT
) TBLPROPERTIES (project = 'urbanflow_final_project', data = 'synthetic')""",
    ]


def merge_silver(tables: HealthTables) -> str:
    """Earliest delivery per event_id wins; unchanged rows are never rewritten."""
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in SILVER_COLUMNS)
    updates = ", ".join(f"{c} = s.{c}" for c in SILVER_COLUMNS)
    columns = ", ".join(("event_id", *SILVER_COLUMNS))
    return f"""MERGE INTO {tables.silver} t
USING (
  SELECT event_id, {", ".join(HEALTH_COLUMNS)},
         timestamp_micros(event_timestamp_micros) AS event_timestamp,
         batch_id AS first_batch_id, timestamp_micros(produced_at_micros) AS first_produced_at,
         COUNT(*) OVER (PARTITION BY event_id) AS delivery_count
  FROM {tables.bronze}
  WHERE event_id IS NOT NULL AND synthetic = true
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY produced_at_micros, batch_id) = 1
) s ON t.event_id = s.event_id
WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({", ".join(f"s.{c}" for c in columns.split(", "))})"""


def validation(tables: HealthTables, *, run_id: str) -> str:
    """Per-run counts; Silver rows must equal distinct Bronze events and account for every row."""
    run = run_id_literal(run_id)
    return f"""SELECT
  (SELECT COUNT(*) FROM {tables.bronze} WHERE run_id = {run}) AS bronze_rows,
  (SELECT COUNT(DISTINCT event_id) FROM {tables.bronze} WHERE run_id = {run}) AS distinct_events,
  (SELECT COUNT(*) FROM {tables.silver} WHERE run_id = {run}) AS silver_rows,
  (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM {tables.silver} WHERE run_id = {run})
    AS duplicate_silver_keys,
  (SELECT COALESCE(SUM(delivery_count), 0) FROM {tables.silver} WHERE run_id = {run})
    AS silver_delivery_total"""


def validation_problems(result: dict[str, int], *, expected_events: int | None = None) -> list[str]:
    """Empty when the run is complete and exactly-once in Silver."""
    problems = []
    if result["silver_rows"] != result["distinct_events"]:
        problems.append("Silver rows differ from distinct Bronze events")
    if result["duplicate_silver_keys"]:
        problems.append("Silver has duplicate event_id values")
    if result["silver_delivery_total"] != result["bronze_rows"]:
        problems.append("delivery counts do not account for every Bronze row")
    if expected_events is not None and result["distinct_events"] != expected_events:
        problems.append(f"expected {expected_events} events, found {result['distinct_events']}")
    if not result["bronze_rows"]:
        problems.append("no Bronze rows for this run")
    return problems

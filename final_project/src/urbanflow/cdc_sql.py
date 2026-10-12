"""SQL for the station-maintenance CDC pipeline, one pure function per statement.

Flow (``notebooks/11_federation_cdc.py``):

1. A full snapshot of the operational maintenance system arrives - from a read-only Lakehouse
   Federation query or a committed fixture snapshot - as a temporary view.
2. ``snapshot_differences`` compares the snapshot with the current source in both directions.
   Only when they differ does ``merge_snapshot`` MERGE it into ``station_maintenance_source``, a
   Delta table with Change Data Feed. Unchanged rows are not rewritten, so only real changes become
   CDF events, and an identical snapshot creates no source commit at all (a Delta MERGE that
   changes nothing still records a commit, which would move the checkpoint over an empty version).
3. The CDC step reads ``table_changes`` for exactly the versions after the checkpoint, MERGEs every
   event into the audit history and the newest final state per key into the Silver target.
4. ``reconciliation`` compares target and source at the processed version, values included, in
   both directions. Only after it passes does ``advance_checkpoint`` move the checkpoint - and
   only forward.

Every identifier is validated and backtick-quoted; no statement drops, truncates or replaces.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
PIPELINE_KEY = "station_maintenance"
COLUMNS = ("maintenance_id", "station_id", "status", "severity", "updated_at")
SNAPSHOT_VIEW = "urbanflow_final_maintenance_snapshot"
CHANGES_VIEW = "urbanflow_final_maintenance_changes"
LATEST_VIEW = "urbanflow_final_latest_maintenance_change"


def _identifier(value: str, label: str) -> str:
    if not _IDENTIFIER.fullmatch(value or ""):
        raise ValueError(f"{label} must be a plain identifier: {value!r}")
    return value


def _version(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer: {value!r}")
    return value


@dataclass(frozen=True)
class CdcTables:
    catalog: str
    schema: str

    def __post_init__(self) -> None:
        _identifier(self.catalog, "catalog")
        _identifier(self.schema, "schema")

    def _name(self, table: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table}`"

    @property
    def source(self) -> str:
        return self._name("station_maintenance_source")

    @property
    def source_plain(self) -> str:
        """Unquoted name, as ``table_changes`` expects a string literal."""
        return f"{self.catalog}.{self.schema}.station_maintenance_source"

    @property
    def target(self) -> str:
        return self._name("silver_station_maintenance")

    @property
    def history(self) -> str:
        return self._name("maintenance_cdc_history")

    @property
    def checkpoint(self) -> str:
        return self._name("maintenance_cdc_checkpoint")


def create_tables(tables: CdcTables) -> list[str]:
    """Idempotent DDL; the source enables Change Data Feed from its first version."""
    return [
        f"""CREATE TABLE IF NOT EXISTS {tables.source} (
  maintenance_id STRING NOT NULL, station_id STRING NOT NULL, status STRING,
  severity STRING, updated_at TIMESTAMP
) TBLPROPERTIES (delta.enableChangeDataFeed = true, project = 'urbanflow_final_project')""",
        f"""CREATE TABLE IF NOT EXISTS {tables.target} (
  maintenance_id STRING NOT NULL, station_id STRING NOT NULL, status STRING,
  severity STRING, updated_at TIMESTAMP,
  source_commit_version BIGINT, source_commit_timestamp TIMESTAMP
) TBLPROPERTIES (project = 'urbanflow_final_project')""",
        f"""CREATE TABLE IF NOT EXISTS {tables.history} (
  change_id STRING NOT NULL, maintenance_id STRING, station_id STRING, status STRING,
  severity STRING, updated_at TIMESTAMP, change_type STRING, commit_version BIGINT,
  commit_timestamp TIMESTAMP, recorded_at TIMESTAMP
) TBLPROPERTIES (project = 'urbanflow_final_project')""",
        f"""CREATE TABLE IF NOT EXISTS {tables.checkpoint} (
  pipeline_key STRING NOT NULL, processed_version BIGINT NOT NULL, processed_at TIMESTAMP NOT NULL
) TBLPROPERTIES (project = 'urbanflow_final_project')""",
    ]


def merge_snapshot(tables: CdcTables) -> str:
    """Make the source equal the snapshot; unchanged rows are left alone (no empty CDF events)."""
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in COLUMNS[1:])
    return f"""MERGE INTO {tables.source} t
USING {SNAPSHOT_VIEW} s ON t.maintenance_id = s.maintenance_id
WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET
  station_id = s.station_id, status = s.status, severity = s.severity, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT ({", ".join(COLUMNS)}) VALUES ({", ".join(f"s.{c}" for c in COLUMNS)})
WHEN NOT MATCHED BY SOURCE THEN DELETE"""


def _two_way_difference(snapshot: str, source: str) -> str:
    """Rows present on one side and not the other, counted both ways over every business column.

    `EXCEPT ALL` compares whole rows, treats NULL as equal to NULL, and respects multiplicity, so
    a changed value, an added row and a removed row all count as differences.
    """
    columns = ", ".join(COLUMNS)
    return f"""SELECT
  (SELECT COUNT(*) FROM (SELECT {columns} FROM {snapshot} EXCEPT ALL SELECT {columns} FROM {source}))
    AS snapshot_rows_not_in_source,
  (SELECT COUNT(*) FROM (SELECT {columns} FROM {source} EXCEPT ALL SELECT {columns} FROM {snapshot}))
    AS source_rows_not_in_snapshot"""


def snapshot_differences(tables: CdcTables) -> str:
    """Differences between the incoming snapshot view and the source's current version."""
    return _two_way_difference(SNAPSHOT_VIEW, tables.source)


def snapshot_requires_merge(differences: dict[str, int]) -> bool:
    """True unless the snapshot and the source are identical in both directions."""
    return bool(
        int(differences["snapshot_rows_not_in_source"])
        or int(differences["source_rows_not_in_snapshot"])
    )


def checkpoint_version(tables: CdcTables) -> str:
    return (
        f"SELECT MAX(processed_version) AS processed_version FROM {tables.checkpoint} "
        f"WHERE pipeline_key = '{PIPELINE_KEY}'"
    )


def changes_view(tables: CdcTables, *, start_version: int, end_version: int) -> str:
    """Exactly the CDF rows of versions start..end, with a deterministic event identity."""
    start = _version(start_version, "start_version")
    end = _version(end_version, "end_version")
    if end < start:
        raise ValueError("end_version must not precede start_version.")
    return f"""CREATE OR REPLACE TEMP VIEW {CHANGES_VIEW} AS
SELECT *, sha2(concat_ws('|', maintenance_id, CAST(_commit_version AS STRING), _change_type), 256)
  AS change_id
FROM table_changes('{tables.source_plain}', {start}, {end})"""


def merge_history(tables: CdcTables) -> str:
    """Every event, pre-images included, exactly once (keyed by change_id) - safe to replay."""
    return f"""MERGE INTO {tables.history} t
USING (
  SELECT change_id, maintenance_id, station_id, status, severity, updated_at,
         _change_type AS change_type, _commit_version AS commit_version,
         _commit_timestamp AS commit_timestamp, current_timestamp() AS recorded_at
  FROM {CHANGES_VIEW}
) s ON t.change_id = s.change_id
WHEN NOT MATCHED THEN INSERT *"""


def latest_changes_view() -> str:
    """The newest final-state event per key; update pre-images are audit-only."""
    return f"""CREATE OR REPLACE TEMP VIEW {LATEST_VIEW} AS
SELECT * EXCEPT (rank) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY maintenance_id ORDER BY _commit_version DESC,
      CASE _change_type WHEN 'delete' THEN 3 WHEN 'update_postimage' THEN 2 ELSE 1 END DESC
  ) AS rank
  FROM {CHANGES_VIEW} WHERE _change_type IN ('insert', 'update_postimage', 'delete')
) WHERE rank = 1"""


def merge_target(tables: CdcTables) -> str:
    """SCD Type 1 current state: deletes remove, changes update, unchanged rows stay untouched."""
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in COLUMNS[1:])
    return f"""MERGE INTO {tables.target} t
USING {LATEST_VIEW} s ON t.maintenance_id = s.maintenance_id
WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET
  station_id = s.station_id, status = s.status, severity = s.severity, updated_at = s.updated_at,
  source_commit_version = s._commit_version, source_commit_timestamp = s._commit_timestamp
WHEN NOT MATCHED AND s._change_type <> 'delete' THEN INSERT (
  {", ".join(COLUMNS)}, source_commit_version, source_commit_timestamp
) VALUES ({", ".join(f"s.{c}" for c in COLUMNS)}, s._commit_version, s._commit_timestamp)"""


def reconciliation(tables: CdcTables, *, processed_version: int, check_history: bool = True) -> str:
    """Target vs source at the processed version, values included, in both directions.

    ``check_history`` also proves every event of this run reached the audit history; a run with
    no new versions has no changes view, so it skips only that one check.
    """
    version = _version(processed_version, "processed_version")
    history_check = (
        f"(SELECT COUNT(*) FROM {CHANGES_VIEW} c LEFT ANTI JOIN {tables.history} h "
        "ON c.change_id = h.change_id)"
        if check_history
        else "0"
    )
    columns = ", ".join(COLUMNS)
    source = f"(SELECT {columns} FROM {tables.source} VERSION AS OF {version})"
    target = f"(SELECT {columns} FROM {tables.target})"
    return f"""SELECT
  (SELECT COUNT(*) FROM {tables.source} VERSION AS OF {version}) AS source_count,
  (SELECT COUNT(*) FROM {tables.target}) AS target_count,
  (SELECT COUNT(*) FROM ({source} EXCEPT ALL {target})) AS missing_or_different_in_target,
  (SELECT COUNT(*) FROM ({target} EXCEPT ALL {source})) AS unexpected_or_different_in_target,
  (SELECT COUNT(*) - COUNT(DISTINCT maintenance_id) FROM {tables.target}) AS duplicate_target_keys,
  {history_check} AS events_missing_from_history"""


def advance_checkpoint(tables: CdcTables, *, processed_version: int) -> str:
    """Move the checkpoint forward only - a stale or replayed run can never move it back."""
    version = _version(processed_version, "processed_version")
    return f"""MERGE INTO {tables.checkpoint} t
USING (SELECT '{PIPELINE_KEY}' AS pipeline_key, CAST({version} AS BIGINT) AS processed_version,
              current_timestamp() AS processed_at) s
ON t.pipeline_key = s.pipeline_key
WHEN MATCHED AND t.processed_version < s.processed_version THEN UPDATE SET
  processed_version = s.processed_version, processed_at = s.processed_at
WHEN NOT MATCHED THEN INSERT *"""


# Casts applied to any snapshot so federated and fixture snapshots have identical types.
SNAPSHOT_CASTS = (
    "CAST(maintenance_id AS STRING) AS maintenance_id",
    "CAST(station_id AS STRING) AS station_id",
    "CAST(status AS STRING) AS status",
    "CAST(severity AS STRING) AS severity",
    "CAST(updated_at AS TIMESTAMP) AS updated_at",
)


def snapshot_contract() -> str:
    """Row count, duplicate keys and null keys of the snapshot view (both must be 0)."""
    return (
        "SELECT COUNT(*) AS n, COUNT(*) - COUNT(DISTINCT maintenance_id) AS duplicate_keys, "
        "COUNT_IF(maintenance_id IS NULL OR station_id IS NULL) AS null_keys "
        f"FROM {SNAPSHOT_VIEW}"
    )

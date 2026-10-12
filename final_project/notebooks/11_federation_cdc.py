# Databricks notebook source
"""Read-only Federation snapshot followed by local Delta Change Data Feed processing.

This notebook never mutates the external database. It is disabled by default and fails closed
unless an approved foreign catalog/table is supplied. Database-native CDC is not used: changes are
created by MERGEing successive read-only snapshots into a Final Project-owned Delta table with CDF.
"""

# COMMAND ----------

import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.federation import FederationSource, select_statement, validate_maintenance_columns
from urbanflow.rag.runtime import assert_workspace, flag, identifier


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)  # noqa: F821
    return dbutils.widgets.get(name)  # noqa: F821


expected_host = widget("expected_host", "")
catalog = identifier(widget("catalog", ""), "catalog")
schema = identifier(widget("schema", ""), "schema")
enabled = flag(widget("run_federation_cdc", "false"))
foreign_catalog = widget("federation_catalog", "").strip()
foreign_table = widget("federation_table", "public.station_maintenance").strip()

assert_workspace(expected_host)
if not enabled:
    dbutils.notebook.exit("SKIPPED: run_federation_cdc=false")  # noqa: F821
if not foreign_catalog:
    raise ValueError("federation_catalog is required when run_federation_cdc=true.")

source = FederationSource(foreign_catalog, foreign_table)
foreign = spark.sql(select_statement(source))  # noqa: F821
validate_maintenance_columns(set(foreign.columns))

# The external query above is SELECT-only. Every object below belongs to this Final Project schema.
snapshot_table = f"`{catalog}`.`{schema}`.`maintenance_federated_snapshot`"
target_table = f"`{catalog}`.`{schema}`.`silver_station_maintenance`"
history_table = f"`{catalog}`.`{schema}`.`maintenance_cdc_history`"
checkpoint_table = f"`{catalog}`.`{schema}`.`maintenance_cdc_checkpoint`"

foreign.selectExpr(
    "CAST(maintenance_id AS STRING) AS maintenance_id",
    "CAST(station_id AS STRING) AS station_id",
    "CAST(status AS STRING) AS status",
    "CAST(severity AS STRING) AS severity",
    "CAST(updated_at AS TIMESTAMP) AS updated_at",
).createOrReplaceTempView("urbanflow_final_foreign_maintenance")

duplicate_keys = spark.sql(  # noqa: F821
    "SELECT COUNT(*) - COUNT(DISTINCT maintenance_id) AS duplicate_keys "
    "FROM urbanflow_final_foreign_maintenance"
).first()[0]
if duplicate_keys:
    raise ValueError(f"Federated maintenance source contains {duplicate_keys} duplicate keys.")

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {snapshot_table} (
      maintenance_id STRING NOT NULL,
      station_id STRING NOT NULL,
      status STRING,
      severity STRING,
      updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
      delta.enableChangeDataFeed = true,
      project = 'urbanflow_final_project',
      source_mode = 'read_only_federation_snapshot'
    )
    """
)

spark.sql(  # noqa: F821
    f"""
    MERGE INTO {snapshot_table} target
    USING urbanflow_final_foreign_maintenance source
      ON target.maintenance_id = source.maintenance_id
    WHEN MATCHED AND NOT (
      target.station_id <=> source.station_id AND
      target.status <=> source.status AND
      target.severity <=> source.severity AND
      target.updated_at <=> source.updated_at
    ) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)

current_version = int(
    spark.sql(f"DESCRIBE HISTORY {snapshot_table}")  # noqa: F821
    .selectExpr("MAX(version) AS version")
    .first()[0]
)

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {checkpoint_table} (
      pipeline_key STRING NOT NULL,
      processed_version BIGINT NOT NULL,
      processed_at TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES (project = 'urbanflow_final_project')
    """
)

checkpoint_row = spark.sql(  # noqa: F821
    f"SELECT MAX(processed_version) FROM {checkpoint_table} "
    "WHERE pipeline_key = 'station_maintenance'"
).first()[0]
checkpoint = int(checkpoint_row) if checkpoint_row is not None else -1

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      maintenance_id STRING NOT NULL,
      station_id STRING NOT NULL,
      status STRING,
      severity STRING,
      updated_at TIMESTAMP,
      source_commit_version BIGINT,
      source_commit_timestamp TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (project = 'urbanflow_final_project')
    """
)

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {history_table} (
      change_id STRING NOT NULL,
      maintenance_id STRING,
      station_id STRING,
      status STRING,
      severity STRING,
      updated_at TIMESTAMP,
      change_type STRING,
      commit_version BIGINT,
      commit_timestamp TIMESTAMP,
      recorded_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (project = 'urbanflow_final_project')
    """
)

# COMMAND ----------

if current_version > checkpoint:
    changes = spark.sql(  # noqa: F821
        f"""
        SELECT *,
          sha2(concat_ws('|', maintenance_id, CAST(_commit_version AS STRING), _change_type), 256)
            AS change_id
        FROM table_changes('{catalog}.{schema}.maintenance_federated_snapshot', {checkpoint + 1})
        WHERE _commit_version <= {current_version}
        """
    )
    changes.createOrReplaceTempView("urbanflow_final_maintenance_changes")

    spark.sql(  # noqa: F821
        f"""
        MERGE INTO {history_table} target
        USING (
          SELECT change_id, maintenance_id, station_id, status, severity, updated_at,
                 _change_type AS change_type, _commit_version AS commit_version,
                 _commit_timestamp AS commit_timestamp, current_timestamp() AS recorded_at
          FROM urbanflow_final_maintenance_changes
        ) source
        ON target.change_id = source.change_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    spark.sql(  # noqa: F821
        """
        CREATE OR REPLACE TEMP VIEW urbanflow_final_latest_maintenance_change AS
        SELECT * EXCEPT (rank)
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY maintenance_id
            ORDER BY _commit_version DESC,
              CASE _change_type WHEN 'delete' THEN 3 WHEN 'update_postimage' THEN 2 ELSE 1 END DESC
          ) AS rank
          FROM urbanflow_final_maintenance_changes
          WHERE _change_type IN ('insert', 'update_postimage', 'delete')
        )
        WHERE rank = 1
        """
    )

    spark.sql(  # noqa: F821
        f"""
        MERGE INTO {target_table} target
        USING urbanflow_final_latest_maintenance_change source
          ON target.maintenance_id = source.maintenance_id
        WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
          station_id = source.station_id,
          status = source.status,
          severity = source.severity,
          updated_at = source.updated_at,
          source_commit_version = source._commit_version,
          source_commit_timestamp = source._commit_timestamp
        WHEN NOT MATCHED AND source._change_type <> 'delete' THEN INSERT (
          maintenance_id, station_id, status, severity, updated_at,
          source_commit_version, source_commit_timestamp
        ) VALUES (
          source.maintenance_id, source.station_id, source.status, source.severity,
          source.updated_at, source._commit_version, source._commit_timestamp
        )
        """
    )

    spark.sql(  # noqa: F821
        f"""
        MERGE INTO {checkpoint_table} target
        USING (SELECT 'station_maintenance' AS pipeline_key, {current_version} AS processed_version,
                      current_timestamp() AS processed_at) source
        ON target.pipeline_key = source.pipeline_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

# COMMAND ----------

reconciliation = (
    spark.sql(  # noqa: F821
        f"""
    SELECT
      (SELECT COUNT(*) FROM urbanflow_final_foreign_maintenance) AS source_count,
      (SELECT COUNT(*) FROM {target_table}) AS target_count,
      (SELECT COUNT(*) FROM urbanflow_final_foreign_maintenance source
       LEFT ANTI JOIN {target_table} target USING (maintenance_id)) AS missing_in_target,
      (SELECT COUNT(*) FROM {target_table} target
       LEFT ANTI JOIN urbanflow_final_foreign_maintenance source USING (maintenance_id))
        AS unexpected_in_target
    """
    )
    .first()
    .asDict()
)
if reconciliation["source_count"] != reconciliation["target_count"] or any(
    reconciliation[name] for name in ("missing_in_target", "unexpected_in_target")
):
    raise AssertionError(f"Federation/CDC reconciliation failed: {reconciliation}")

print(
    {
        "status": "PASS",
        "source_mode": "read_only_lakehouse_federation",
        "cdc_mode": "delta_change_data_feed_not_database_native_cdc",
        "checkpoint_before": checkpoint,
        "checkpoint_after": current_version,
        **reconciliation,
    }
)

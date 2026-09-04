# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

# Purpose: select the project catalog and schema.

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

display(
    spark.sql("""
        SELECT
            current_catalog() AS current_catalog,
            current_schema() AS current_schema,
            session_user() AS current_user
    """)
)

# COMMAND ----------

# Purpose: confirm that the Gold reconciliation audit table exists
# and contains the status columns required by the alert.

audit_table = (
    f"{catalog}.{schema}.gold_reconciliation_audit"
)

if not spark.catalog.tableExists(audit_table):
    raise RuntimeError(
        f"Required audit table does not exist: {audit_table}. "
        "Run the Gold validation notebook first."
    )

audit_df = spark.table(audit_table)
audit_columns = set(audit_df.columns)

required_columns = {
    "overall_status",
    "integrity_status",
}

missing_columns = sorted(
    required_columns - audit_columns
)

if missing_columns:
    raise RuntimeError(
        "The audit table is missing required columns: "
        + ", ".join(missing_columns)
    )

print("Audit table columns:")
print(", ".join(audit_df.columns))

print("Current Gold reconciliation result:")
display(audit_df.limit(1))

# COMMAND ----------

# Purpose: convert the pipeline validation result into a numeric
# value that a Databricks SQL alert can evaluate.

spark.sql(f"""
    CREATE OR REPLACE VIEW
        `{catalog}`.`{schema}`.`olist_pipeline_alert_status`
    COMMENT
        'Current Olist pipeline validation status for SQL alerts'
    AS

    WITH current_audit AS (
        SELECT
            overall_status,
            integrity_status

        FROM
            `{catalog}`.`{schema}`.`gold_reconciliation_audit`

        LIMIT 1
    )

    SELECT
        current_timestamp() AS alert_checked_at,

        COALESCE(
            MAX(overall_status),
            'MISSING'
        ) AS overall_status,

        COALESCE(
            MAX(integrity_status),
            'MISSING'
        ) AS integrity_status,

        CASE
            WHEN COUNT(*) = 0 THEN 1

            WHEN UPPER(
                COALESCE(MAX(overall_status), 'MISSING')
            ) <> 'PASS'
            THEN 1

            WHEN UPPER(
                COALESCE(MAX(integrity_status), 'MISSING')
            ) <> 'PASS'
            THEN 1

            ELSE 0
        END AS alert_value,

        CASE
            WHEN COUNT(*) = 0
            THEN 'No pipeline validation result was found'

            WHEN UPPER(
                COALESCE(MAX(overall_status), 'MISSING')
            ) <> 'PASS'
            THEN 'Gold reconciliation validation failed'

            WHEN UPPER(
                COALESCE(MAX(integrity_status), 'MISSING')
            ) <> 'PASS'
            THEN 'Gold referential integrity validation failed'

            ELSE 'Pipeline validation passed'
        END AS alert_message

    FROM current_audit
""")

print("Alert-status view created")

# COMMAND ----------

# Purpose: convert the latest pipeline validation result into a
# numeric alert value that Databricks SQL Alerts can evaluate.

spark.sql(f"""
    CREATE OR REPLACE VIEW
        `{catalog}`.`{schema}`.`olist_pipeline_alert_status`
    COMMENT
        'Latest Olist pipeline validation status for SQL alerts'
    AS

    WITH latest_audit AS (
        SELECT
            overall_status,
            integrity_status,
            _validated_at

        FROM
            `{catalog}`.`{schema}`.`gold_reconciliation_audit`

        QUALIFY
            ROW_NUMBER() OVER (
                ORDER BY _validated_at DESC
            ) = 1
    )

    SELECT
        current_timestamp() AS alert_checked_at,
        MAX(_validated_at) AS latest_validation_at,

        COALESCE(
            MAX(overall_status),
            'MISSING'
        ) AS overall_status,

        COALESCE(
            MAX(integrity_status),
            'MISSING'
        ) AS integrity_status,

        CASE
            WHEN COUNT(*) = 0 THEN 1

            WHEN UPPER(
                COALESCE(MAX(overall_status), 'MISSING')
            ) <> 'PASS'
            THEN 1

            WHEN UPPER(
                COALESCE(MAX(integrity_status), 'MISSING')
            ) <> 'PASS'
            THEN 1

            ELSE 0
        END AS alert_value,

        CASE
            WHEN COUNT(*) = 0
            THEN 'No pipeline validation result was found'

            WHEN UPPER(
                COALESCE(MAX(overall_status), 'MISSING')
            ) <> 'PASS'
            THEN 'Gold reconciliation validation failed'

            WHEN UPPER(
                COALESCE(MAX(integrity_status), 'MISSING')
            ) <> 'PASS'
            THEN 'Gold referential integrity validation failed'

            ELSE 'Pipeline validation passed'
        END AS alert_message

    FROM latest_audit
""")

print("Alert-status view created")

# COMMAND ----------

# Purpose: verify that the current pipeline result does not trigger an alert.

alert_validation_df = spark.table(
    f"{catalog}.{schema}.olist_pipeline_alert_status"
)

display(alert_validation_df)

alert_result = alert_validation_df.first()

if alert_result is None:
    raise RuntimeError(
        "The alert-status view returned no result."
    )

if int(alert_result["alert_value"]) != 0:
    raise RuntimeError(
        "ALERT: "
        + alert_result["alert_message"]
    )

print(
    "PASS: pipeline validation is healthy; "
    "no alert should be triggered."
)

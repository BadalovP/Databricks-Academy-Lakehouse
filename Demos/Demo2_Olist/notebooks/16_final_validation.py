# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./00_setup

# COMMAND ----------

# Purpose: select and verify the catalog, schema, and current user.

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

execution_context_df = spark.sql("""
    SELECT
        current_catalog() AS current_catalog,
        current_schema() AS current_schema,
        session_user() AS current_user,
        current_timestamp() AS validation_started_at
""")

display(execution_context_df)

# COMMAND ----------

# Purpose: verify that all required Gold, governance, dashboard,
# audit, and alert objects exist before final validation.

required_objects = [
    ("TABLE", "gold_dim_customer"),
    ("TABLE", "gold_dim_product"),
    ("TABLE", "gold_dim_seller"),
    ("TABLE", "gold_dim_date"),
    ("TABLE", "gold_fact_order_items"),
    ("TABLE", "gold_reconciliation_audit"),
    ("TABLE", "governance_customer_state_access"),
    ("VIEW", "secure_gold_dim_customer"),
    ("VIEW", "secure_gold_order_items_dashboard"),
    ("VIEW", "olist_pipeline_alert_status"),
]

object_check_rows = []

for object_type, object_name in required_objects:
    full_name = f"{catalog}.{schema}.{object_name}"

    object_check_rows.append(
        (
            object_type,
            object_name,
            full_name,
            spark.catalog.tableExists(full_name),
        )
    )

object_check_df = spark.createDataFrame(
    object_check_rows,
    [
        "object_type",
        "object_name",
        "full_name",
        "object_exists",
    ],
)

display(object_check_df)

missing_objects = [
    row["full_name"]
    for row in object_check_df.filter(
        "object_exists = false"
    ).collect()
]

if missing_objects:
    raise RuntimeError(
        "Final validation cannot continue. "
        "Missing objects: "
        + ", ".join(missing_objects)
    )

print("PASS: all required project objects exist")

# COMMAND ----------

# Purpose: read the latest Gold reconciliation audit result
# and confirm that reconciliation and referential integrity passed.

from pyspark.sql import functions as F

gold_audit_table = (
    f"{catalog}.{schema}.gold_reconciliation_audit"
)

latest_gold_audit_df = (
    spark.table(gold_audit_table)
    .orderBy(F.col("_validated_at").desc())
    .limit(1)
)

latest_gold_audit_row = latest_gold_audit_df.first()

if latest_gold_audit_row is None:
    raise RuntimeError(
        "The Gold reconciliation audit table is empty."
    )

display(
    latest_gold_audit_df.select(
        "silver_rows",
        "gold_rows",
        "silver_distinct_orders",
        "gold_distinct_orders",
        "integrity_status",
        "overall_status",
        "_validated_at",
    )
)

gold_reconciliation_pass = (
    str(
        latest_gold_audit_row["overall_status"]
    ).upper() == "PASS"
)

referential_integrity_pass = (
    str(
        latest_gold_audit_row["integrity_status"]
    ).upper() == "PASS"
)

print(
    "Gold reconciliation:",
    "PASS" if gold_reconciliation_pass else "FAIL",
)

print(
    "Referential integrity:",
    "PASS" if referential_integrity_pass else "FAIL",
)

# COMMAND ----------

# Purpose: confirm that the secure dashboard contains only authorized
# states and applies the correct PII masking for the current user.

security_validation_df = spark.sql(f"""
    WITH user_context AS (
        SELECT
            session_user() AS current_user,

            is_account_group_member(
                'olist_pii_readers'
            ) AS is_pii_reader
    ),

    access_config AS (
        SELECT
            COALESCE(
                MAX(
                    CASE
                        WHEN customer_state = 'ALL'
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS has_all_access,

            SORT_ARRAY(
                COLLECT_SET(customer_state)
            ) AS configured_states

        FROM
            `{catalog}`.`{schema}`.
            `governance_customer_state_access`

        WHERE
            principal = session_user()
    ),

    security_metrics AS (
        SELECT
            uc.current_user,
            uc.is_pii_reader,
            ac.has_all_access,
            ac.configured_states,

            COUNT(
                d.order_item_sk
            ) AS visible_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN ac.has_all_access = 0
                            AND d.customer_state IS NOT NULL
                            AND NOT ARRAY_CONTAINS(
                                ac.configured_states,
                                d.customer_state
                            )
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS unauthorized_state_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN uc.is_pii_reader
                            AND (
                                d.customer_id LIKE '%...MASKED'
                                OR d.customer_unique_id LIKE '%...MASKED'
                                OR d.customer_zip_code_prefix LIKE '%**'
                            )
                        THEN 1

                        WHEN NOT uc.is_pii_reader
                            AND (
                                (
                                    d.customer_id IS NOT NULL
                                    AND d.customer_id NOT LIKE '%...MASKED'
                                )
                                OR (
                                    d.customer_unique_id IS NOT NULL
                                    AND d.customer_unique_id NOT LIKE '%...MASKED'
                                )
                                OR (
                                    d.customer_zip_code_prefix IS NOT NULL
                                    AND d.customer_zip_code_prefix NOT LIKE '%**'
                                )
                            )
                        THEN 1

                        ELSE 0
                    END
                ),
                0
            ) AS cls_violations

        FROM access_config ac
        CROSS JOIN user_context uc

        LEFT JOIN
            `{catalog}`.`{schema}`.
            `secure_gold_order_items_dashboard` d
        ON TRUE

        GROUP BY
            uc.current_user,
            uc.is_pii_reader,
            ac.has_all_access,
            ac.configured_states
    )

    SELECT *
    FROM security_metrics
""")

display(security_validation_df)

security_validation_row = (
    security_validation_df.first()
)

security_pass = (
    security_validation_row is not None
    and security_validation_row["visible_rows"] > 0
    and security_validation_row[
        "unauthorized_state_rows"
    ] == 0
    and security_validation_row[
        "cls_violations"
    ] == 0
)

print(
    "Dashboard security:",
    "PASS" if security_pass else "FAIL",
)

# COMMAND ----------

# Purpose: confirm that the pipeline alert currently reports
# a healthy status and would not trigger a notification.

alert_validation_df = spark.table(
    f"{catalog}.{schema}.olist_pipeline_alert_status"
)

display(alert_validation_df)

alert_validation_row = alert_validation_df.first()

alert_pass = (
    alert_validation_row is not None
    and int(
        alert_validation_row["alert_value"]
    ) == 0
    and str(
        alert_validation_row["overall_status"]
    ).upper() == "PASS"
    and str(
        alert_validation_row["integrity_status"]
    ).upper() == "PASS"
)

print(
    "Pipeline alert:",
    "PASS" if alert_pass else "FAIL",
)

# COMMAND ----------

# Purpose: combine all project validation results into one
# readable end-to-end summary.

object_validation_pass = (
    len(missing_objects) == 0
)

final_check_rows = [
    (
        "Required project objects",
        "PASS" if object_validation_pass else "FAIL",
        (
            f"{len(required_objects) - len(missing_objects)}"
            f"/{len(required_objects)} objects exist"
        ),
    ),
    (
        "Gold reconciliation",
        "PASS" if gold_reconciliation_pass else "FAIL",
        (
            f"Silver rows: "
            f"{latest_gold_audit_row['silver_rows']}; "
            f"Gold rows: "
            f"{latest_gold_audit_row['gold_rows']}"
        ),
    ),
    (
        "Referential integrity",
        "PASS" if referential_integrity_pass else "FAIL",
        (
            "Integrity status: "
            f"{latest_gold_audit_row['integrity_status']}"
        ),
    ),
    (
        "Dashboard RLS and CLS",
        "PASS" if security_pass else "FAIL",
        (
            f"Visible rows: "
            f"{security_validation_row['visible_rows']}; "
            f"Unauthorized rows: "
            f"{security_validation_row['unauthorized_state_rows']}; "
            f"CLS violations: "
            f"{security_validation_row['cls_violations']}"
        ),
    ),
    (
        "Pipeline alert",
        "PASS" if alert_pass else "FAIL",
        (
            f"Alert value: "
            f"{alert_validation_row['alert_value']}; "
            f"Message: "
            f"{alert_validation_row['alert_message']}"
        ),
    ),
]

final_checks_df = spark.createDataFrame(
    final_check_rows,
    [
        "validation_check",
        "status",
        "details",
    ],
)

display(final_checks_df)

failed_checks = [
    row["validation_check"]
    for row in final_checks_df.filter(
        "status <> 'PASS'"
    ).collect()
]

overall_final_status = (
    "PASS"
    if not failed_checks
    else "FAIL"
)

print(
    "FINAL PROJECT STATUS:",
    overall_final_status,
)

# COMMAND ----------

# Purpose: persist the final end-to-end validation result
# so previous project validations remain auditable.

final_audit_row_df = spark.createDataFrame(
    [
        (
            overall_final_status,
            "PASS" if object_validation_pass else "FAIL",
            "PASS" if gold_reconciliation_pass else "FAIL",
            "PASS" if referential_integrity_pass else "FAIL",
            "PASS" if security_pass else "FAIL",
            "PASS" if alert_pass else "FAIL",
            int(security_validation_row["visible_rows"]),
            int(
                security_validation_row[
                    "unauthorized_state_rows"
                ]
            ),
            int(
                security_validation_row[
                    "cls_violations"
                ]
            ),
            bool(
                security_validation_row[
                    "is_pii_reader"
                ]
            ),
        )
    ],
    """
        overall_status STRING,
        object_validation_status STRING,
        gold_reconciliation_status STRING,
        referential_integrity_status STRING,
        dashboard_security_status STRING,
        alert_status STRING,
        visible_dashboard_rows LONG,
        unauthorized_state_rows LONG,
        cls_violations LONG,
        is_pii_reader BOOLEAN
    """,
)

final_audit_df = (
    final_audit_row_df
    .withColumn(
        "validated_by",
        F.expr("session_user()"),
    )
    .withColumn(
        "_validated_at",
        F.current_timestamp(),
    )
)

final_audit_table = (
    f"{catalog}.{schema}."
    "olist_final_validation_audit"
)

(
    final_audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(final_audit_table)
)

display(
    spark.table(final_audit_table)
    .orderBy(F.col("_validated_at").desc())
    .limit(10)
)

if overall_final_status != "PASS":
    raise RuntimeError(
        "Final project validation failed: "
        + ", ".join(failed_checks)
    )

print("SUCCESS: Olist Lakehouse project validation passed")

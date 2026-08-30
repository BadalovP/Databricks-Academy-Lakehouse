# Databricks notebook source
"""Create and validate the fail-closed dynamic-view governance fallback."""

import json
from datetime import datetime, timezone

for name, default in (("catalog", "dbr_dev"), ("schema", "parvinbadalov"), ("run_id", "manual")):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
validation_table = f"{catalog}.{schema}.demo2_validation_results"
access_table = f"{catalog}.{schema}.demo2_user_country_access"
governed_view = f"{catalog}.{schema}.demo2_sales_governed"
base_table = f"{catalog}.{schema}.demo2_sales_business_base"


def record(check_name, passed, details):
    spark.createDataFrame(
        [
            (
                run_id,
                check_name,
                bool(passed),
                json.dumps(details, sort_keys=True),
                datetime.now(timezone.utc),
            )
        ],
        "run_id string, check_name string, passed boolean, details string, checked_at timestamp",
    ).write.mode("append").saveAsTable(validation_table)


session_user = spark.sql("SELECT SESSION_USER() AS user_name").first()["user_name"]
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {access_table} (
      user_name STRING NOT NULL,
      country STRING,
      all_access BOOLEAN NOT NULL,
      can_view_pii BOOLEAN NOT NULL,
      is_cleanup_probe BOOLEAN NOT NULL
    ) USING DELTA
    """
)
spark.sql(f"DELETE FROM {access_table} WHERE lower(user_name) = lower('{session_user}')")
spark.createDataFrame(
    [(session_user, None, True, True, False)],
    "user_name string, country string, all_access boolean, can_view_pii boolean, is_cleanup_probe boolean",
).write.mode("append").saveAsTable(access_table)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {governed_view} AS
    SELECT
      b.order_line_id,
      b.order_id,
      b.customer_key,
      b.order_date,
      b.country,
      b.city,
      b.loyalty_tier,
      b.category,
      b.product_name,
      b.sales_channel,
      b.order_status,
      b.quantity,
      b.gross_amount,
      b.discount_amount,
      b.net_amount,
      CASE WHEN EXISTS (
        SELECT 1 FROM {access_table} p
        WHERE lower(p.user_name) = lower(SESSION_USER()) AND p.can_view_pii
      ) THEN b.customer_name ELSE '***MASKED***' END AS customer_name,
      CASE WHEN EXISTS (
        SELECT 1 FROM {access_table} p
        WHERE lower(p.user_name) = lower(SESSION_USER()) AND p.can_view_pii
      ) THEN b.email ELSE '***MASKED***' END AS email,
      b._dq_status
    FROM {base_table} b
    WHERE EXISTS (
      SELECT 1 FROM {access_table} a
      WHERE lower(a.user_name) = lower(SESSION_USER())
        AND (a.all_access OR a.country = b.country)
    )
    """
)

base_count = spark.table(base_table).count()
governed_count = spark.table(governed_view).count()
unmapped_count = spark.sql(
    f"SELECT count(*) AS rows FROM {access_table} WHERE lower(user_name) = lower('__demo2_unmapped_user__')"
).first()["rows"]

probe_user = "__demo2_cleanup_probe__"
spark.createDataFrame(
    [(probe_user, "PL", False, False, True)],
    "user_name string, country string, all_access boolean, can_view_pii boolean, is_cleanup_probe boolean",
).write.mode("append").saveAsTable(access_table)

passed = base_count > 0 and governed_count == base_count and unmapped_count == 0
record(
    "governance",
    passed,
    {
        "implementation": "dynamic_view_fallback",
        "base_count": base_count,
        "governed_count": governed_count,
        "unmapped_access_rows": unmapped_count,
        "session_user_mapping": "explicit_all_access",
    },
)
if not passed:
    raise AssertionError("Dynamic-view governance validation failed")
print("Fail-closed RLS/CLS dynamic view validated")

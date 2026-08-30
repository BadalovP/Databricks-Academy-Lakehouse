"""Print post-run Demo 2 validation evidence from Azure Databricks as JSON."""

import argparse
import json

from databricks.connect import DatabricksSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="AZURE_DEV")
    parser.add_argument("--cluster-id", default="0702-171207-xo9bbc0y")
    parser.add_argument("--catalog", default="dbr_dev")
    parser.add_argument("--schema", default="parvinbadalov")
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    prefix = f"{args.catalog}.{args.schema}"
    spark = DatabricksSession.builder.profile(args.profile).clusterId(args.cluster_id).getOrCreate()
    queries = {
        "validation": f"""
            SELECT check_name, passed, details
            FROM {prefix}.demo2_validation_results
            WHERE run_id = '{args.run_id}'
            QUALIFY row_number() OVER (
              PARTITION BY check_name ORDER BY checked_at DESC
            ) = 1
            ORDER BY check_name
        """,
        "dq": f"""
            SELECT *
            FROM {prefix}.demo2_dq_summary_gold
            ORDER BY _batch_loaded_at DESC, _source_batch_id DESC
        """,
        "scd2": f"""
            SELECT
              customer_id,
              count(*) AS versions,
              sum(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) AS current_versions
            FROM {prefix}.dim_customer_scd2
            GROUP BY customer_id
            ORDER BY customer_id
        """,
        "business_kpis": f"""
            SELECT
              count(*) AS trusted_order_lines,
              count(DISTINCT order_id) AS orders,
              count(DISTINCT customer_key) AS customers,
              sum(quantity) AS items_sold,
              sum(gross_amount) AS gross_revenue,
              sum(net_amount) AS net_revenue,
              sum(net_amount) / count(DISTINCT order_id) AS average_order_value
            FROM {prefix}.demo2_sales_governed
        """,
        "dq_failures_by_rule": f"""
            SELECT _source_batch_id, _dq_status, rule_name, affected_rows
            FROM {prefix}.demo2_dq_failures_by_rule_gold
            ORDER BY _batch_loaded_at, rule_name
        """,
        "quarantine_sample": f"""
            SELECT
              order_line_id,
              _dq_quarantine_reasons,
              _dq_dimensions
            FROM {prefix}.demo2_orders_quarantine
            WHERE _source_batch_id = 'DEMO2_V2_SCHEMA_EVOLUTION'
            ORDER BY order_line_id
        """,
        "tables": f"SHOW TABLES IN {prefix} LIKE 'demo2*'",
    }
    try:
        result = {
            name: [row.asDict(recursive=True) for row in spark.sql(query).collect()]
            for name, query in queries.items()
        }
        print(json.dumps(result, default=str, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

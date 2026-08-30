"""Generate the checked-in AI/BI dashboard definition from proven Demo 2 tables."""

import json
from pathlib import Path


def sql_lines(sql):
    return [f"{line}\n" for line in sql.strip().splitlines()]


def query(dataset, fields, disaggregated):
    return {
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [{"name": name, "expression": expression} for name, expression in fields],
            "disaggregated": disaggregated,
        },
    }


def counter(name, title, dataset, field_name, expression, x, y):
    return {
        "widget": {
            "name": name,
            "queries": [query(dataset, [(field_name, expression)], False)],
            "spec": {
                "frame": {"showTitle": True, "title": title},
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": {"fieldName": field_name, "rowNumber": 0}},
                "data": {"queryName": "main_query"},
            },
        },
        "position": {"x": x, "y": y, "width": 2, "height": 4},
    }


def chart(name, title, kind, dataset, category, metric, x, y, width=6, height=6):
    category_name, category_expression, scale_type = category
    metric_name, metric_expression = metric
    return {
        "widget": {
            "name": name,
            "queries": [
                query(
                    dataset,
                    [(category_name, category_expression), (metric_name, metric_expression)],
                    False,
                )
            ],
            "spec": {
                "frame": {"showTitle": True, "title": title},
                "version": 3,
                "widgetType": kind,
                "encodings": {
                    "x": {
                        "fieldName": category_name,
                        "displayName": category_name,
                        "scale": {"type": scale_type},
                    },
                    "y": {
                        "fieldName": metric_name,
                        "displayName": metric_name,
                        "scale": {"type": "quantitative"},
                    },
                },
                "data": {"queryName": "main_query"},
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def filter_widget(name, title, field, widget_type, x):
    query_name = f"{name}_query"
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": query_name,
                    "query": {
                        "datasetName": "business",
                        "fields": [
                            {"name": field, "expression": f"`{field}`"},
                            {
                                "name": f"{field}_associativity",
                                "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                            },
                        ],
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": widget_type,
                "encodings": {"fields": [{"fieldName": field, "queryName": query_name}]},
            },
        },
        "position": {"x": x, "y": 0, "width": 2 if field != "order_date" else 4, "height": 2},
    }


def header(name, title, subtitle, y=0):
    return {
        "widget": {
            "name": name,
            "multilineTextboxSpec": {"lines": [f"# {title}\n", "\n", subtitle]},
        },
        "position": {"x": 0, "y": y, "width": 12, "height": 2},
    }


def dashboard():
    business_sql = """
        SELECT
          order_date, country, city, loyalty_tier, category, product_name,
          sales_channel, order_status, customer_key, order_id, quantity,
          gross_amount, discount_amount, net_amount
        FROM dbr_dev.parvinbadalov.demo2_sales_governed
    """
    dq_sql = """
        SELECT *
        FROM dbr_dev.parvinbadalov.demo2_dq_summary_gold
        ORDER BY _batch_loaded_at, _source_batch_id
    """
    dq_latest_sql = """
        SELECT *
        FROM dbr_dev.parvinbadalov.demo2_dq_summary_gold
        ORDER BY _batch_loaded_at DESC, _source_batch_id DESC
        LIMIT 1
    """
    dq_rules_sql = """
        SELECT *
        FROM dbr_dev.parvinbadalov.demo2_dq_failures_by_rule_gold
        ORDER BY _batch_loaded_at, rule_name
    """

    overview = [
        header(
            "overview_header",
            "RetailPulse E-Commerce Operations",
            "Trusted business performance from the governed VALID + WARN serving view.",
        ),
        counter("net_revenue", "Net Revenue", "business", "net_revenue", "SUM(`net_amount`)", 0, 2),
        counter(
            "gross_revenue",
            "Gross Revenue",
            "business",
            "gross_revenue",
            "SUM(`gross_amount`)",
            2,
            2,
        ),
        counter("orders", "Orders", "business", "orders", "COUNT(DISTINCT `order_id`)", 4, 2),
        counter(
            "customers",
            "Customers",
            "business",
            "customers",
            "COUNT(DISTINCT `customer_key`)",
            6,
            2,
        ),
        counter("items", "Items Sold", "business", "items", "SUM(`quantity`)", 8, 2),
        counter(
            "aov",
            "Average Order Value",
            "business",
            "average_order_value",
            "SUM(`net_amount`) / COUNT(DISTINCT `order_id`)",
            10,
            2,
        ),
        chart(
            "revenue_time",
            "Revenue over Time",
            "line",
            "business",
            ("order_date", "`order_date`", "temporal"),
            ("net_revenue", "SUM(`net_amount`)"),
            0,
            6,
            12,
        ),
        chart(
            "revenue_country",
            "Revenue by Country",
            "bar",
            "business",
            ("country", "`country`", "categorical"),
            ("net_revenue", "SUM(`net_amount`)"),
            0,
            12,
        ),
        chart(
            "revenue_category",
            "Revenue by Category",
            "bar",
            "business",
            ("category", "`category`", "categorical"),
            ("net_revenue", "SUM(`net_amount`)"),
            6,
            12,
        ),
        chart(
            "revenue_tier",
            "Revenue by Loyalty Tier",
            "bar",
            "business",
            ("loyalty_tier", "`loyalty_tier`", "categorical"),
            ("net_revenue", "SUM(`net_amount`)"),
            0,
            18,
        ),
        chart(
            "top_products",
            "Top Products",
            "bar",
            "business",
            ("product_name", "`product_name`", "categorical"),
            ("net_revenue", "SUM(`net_amount`)"),
            6,
            18,
        ),
        chart(
            "channel_distribution",
            "Sales Channel Distribution",
            "bar",
            "business",
            ("sales_channel", "`sales_channel`", "categorical"),
            ("orders", "COUNT(DISTINCT `order_id`)"),
            0,
            24,
        ),
        chart(
            "status_distribution",
            "Orders by Status",
            "bar",
            "business",
            ("order_status", "`order_status`", "categorical"),
            ("orders", "COUNT(DISTINCT `order_id`)"),
            6,
            24,
        ),
    ]

    dq_page = [
        header(
            "dq_header",
            "Data Quality Control",
            "Physical-row quality metrics; quarantine rate is sourced from demo2_dq_summary_gold.",
        ),
        counter(
            "quarantine_rate",
            "Latest Quarantine Rate",
            "dq_latest",
            "quarantine_rate_pct",
            "`quarantine_rate_pct`",
            0,
            2,
        ),
        counter(
            "quarantine_rows",
            "Quarantined Rows",
            "dq_latest",
            "quarantined_rows",
            "`quarantined_rows`",
            2,
            2,
        ),
        counter(
            "warning_rows",
            "Warnings",
            "dq_latest",
            "warning_rows",
            "`warning_rows`",
            4,
            2,
        ),
        counter(
            "valid_rows",
            "Valid Rows",
            "dq_latest",
            "valid_rows",
            "`valid_rows`",
            6,
            2,
        ),
        counter(
            "total_rows",
            "Physical Rows",
            "dq_latest",
            "total_rows",
            "`total_rows`",
            8,
            2,
        ),
        chart(
            "dq_trend",
            "Quarantine Rate Trend",
            "line",
            "dq_summary",
            ("_batch_loaded_at", "`_batch_loaded_at`", "temporal"),
            ("quarantine_rate_pct", "SUM(`quarantine_rate_pct`)"),
            0,
            6,
            12,
        ),
        chart(
            "dq_rules",
            "DQ Failures and Warnings by Rule",
            "bar",
            "dq_rules",
            ("rule_name", "`rule_name`", "categorical"),
            ("affected_rows", "SUM(`affected_rows`)"),
            0,
            12,
            12,
            8,
        ),
    ]

    global_filters = [
        filter_widget("filter_date", "Date", "order_date", "filter-date-range-picker", 0),
        filter_widget("filter_country", "Country", "country", "filter-multi-select", 4),
        filter_widget("filter_category", "Category", "category", "filter-multi-select", 6),
        filter_widget("filter_tier", "Loyalty Tier", "loyalty_tier", "filter-multi-select", 8),
        filter_widget(
            "filter_channel", "Sales Channel", "sales_channel", "filter-multi-select", 10
        ),
    ]

    return {
        "datasets": [
            {
                "name": "business",
                "displayName": "Governed Business Sales",
                "queryLines": sql_lines(business_sql),
                "catalog": "dbr_dev",
                "schema": "parvinbadalov",
            },
            {
                "name": "dq_summary",
                "displayName": "DQ Batch Summary",
                "queryLines": sql_lines(dq_sql),
                "catalog": "dbr_dev",
                "schema": "parvinbadalov",
            },
            {
                "name": "dq_latest",
                "displayName": "Latest DQ Batch",
                "queryLines": sql_lines(dq_latest_sql),
                "catalog": "dbr_dev",
                "schema": "parvinbadalov",
            },
            {
                "name": "dq_rules",
                "displayName": "DQ Failures by Rule",
                "queryLines": sql_lines(dq_rules_sql),
                "catalog": "dbr_dev",
                "schema": "parvinbadalov",
            },
        ],
        "pages": [
            {
                "name": "overview",
                "displayName": "Business Overview",
                "layout": overview,
                "pageType": "PAGE_TYPE_CANVAS",
                "layoutVersion": "GRID_V1",
            },
            {
                "name": "data_quality",
                "displayName": "Data Quality",
                "layout": dq_page,
                "pageType": "PAGE_TYPE_CANVAS",
                "layoutVersion": "GRID_V1",
            },
            {
                "name": "global_filters",
                "displayName": "Global Filters",
                "layout": global_filters,
                "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
                "layoutVersion": "GRID_V1",
            },
        ],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
            "applyModeEnabled": False,
        },
    }


def main():
    output = Path(__file__).resolve().parents[1] / "dashboard" / "demo2_dashboard.lvdash.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dashboard(), indent=2) + "\n", encoding="ascii")
    print(output)


if __name__ == "__main__":
    main()

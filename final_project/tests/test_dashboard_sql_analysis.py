"""Every dashboard dataset analyses against the live table schemas, in local Spark.

The schemas in `fixtures/dashboard_table_schemas.json` were read (read-only) from Unity Catalog
after step 3. Spark analysis resolves every table, column and function, so a misspelled column or
a wrong table name fails here instead of on the warehouse. It does not prove the numbers or the
rendering, and the Lakeflow event-log dataset (Databricks-only `EVENT_LOG` and `:` JSON path) is
excluded and reported.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import build_dashboard  # noqa: E402

SCHEMAS = json.loads(
    (Path(__file__).parent / "fixtures" / "dashboard_table_schemas.json").read_text("utf-8")
)["tables"]
# Not created yet in DEV; mirrors the DDL in urbanflow.rag.delta_store (created by ingest).
SCHEMAS["rag_evaluation_results"] = [
    ["evaluation_id", "string"], ["evaluated_at", "timestamp"], ["variant", "string"],
    ["question_id", "string"], ["metrics_json", "string"], ["error_status", "string"],
]  # fmt: skip
DATABRICKS_ONLY = {"quality_lakeflow"}
NAMES = {"catalog": "spark_catalog", "schema": "urbanflow_final_dev",
         "lakeflow_schema": "urbanflow_final_lakeflow_dev"}  # fmt: skip


@pytest.fixture(scope="module")
def spark_with_tables(tmp_path_factory):
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("urbanflow-dashboard-sql")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE DATABASE IF NOT EXISTS urbanflow_final_dev")
    for table, columns in SCHEMAS.items():
        ddl = ", ".join(f"`{name}` {type_text}" for name, type_text in columns)
        session.sql(
            f"CREATE TABLE IF NOT EXISTS spark_catalog.urbanflow_final_dev.{table} ({ddl}) "
            "USING parquet"
        )
    yield session
    session.stop()


def _datasets():
    return build_dashboard.datasets(build_dashboard.execution_ids(), NAMES)


@pytest.mark.parametrize("dataset", _datasets(), ids=lambda d: d["name"])
def test_every_dashboard_dataset_resolves_against_the_live_schemas(spark_with_tables, dataset):
    if dataset["name"] in DATABRICKS_ONLY:
        pytest.skip("EVENT_LOG and ':' JSON paths exist only on Databricks")
    frame = spark_with_tables.sql("".join(dataset["queryLines"]))
    assert frame.columns, dataset["name"]
    assert frame.limit(1).collect() is not None  # executes against the empty tables


def test_widgets_only_use_columns_their_dataset_returns(spark_with_tables) -> None:
    columns = {
        d["name"]: set(spark_with_tables.sql("".join(d["queryLines"])).columns)
        for d in _datasets()
        if d["name"] not in DATABRICKS_ONLY
    }
    definition = build_dashboard.build("personal_dev")
    for page in definition["pages"]:
        for item in page["layout"]:
            for query in item["widget"].get("queries", []):
                name = query["query"]["datasetName"]
                if name in DATABRICKS_ONLY:
                    continue
                for field in query["query"]["fields"]:
                    used = field["expression"].split("`")[1]
                    assert used in columns[name], (item["widget"]["name"], name, used)


def test_the_fixture_covers_every_table_the_dashboard_reads() -> None:
    import re

    sql = "\n".join("".join(d["queryLines"]) for d in _datasets())
    read = set(re.findall(r"spark_catalog\.urbanflow_final_dev\.(\w+)", sql))
    assert read <= set(SCHEMAS), read - set(SCHEMAS)


# --- behaviour of the maintenance datasets on a small local example -------------------------


def _insert(spark, table: str, rows: list[dict]) -> None:
    """Insert rows by column name; every column not given is NULL of its live type."""
    columns = SCHEMAS[table]

    def literal(value, type_text):
        if value is None:
            return f"CAST(NULL AS {type_text})"
        if isinstance(value, (int, float)):
            return f"CAST({value} AS {type_text})"
        return f"CAST('{value}' AS {type_text})"

    values = ", ".join(
        "(" + ", ".join(literal(row.get(name), type_text) for name, type_text in columns) + ")"
        for row in rows
    )
    spark.sql(f"INSERT INTO spark_catalog.urbanflow_final_dev.{table} VALUES {values}")


def _dataset(name: str) -> str:
    return "".join(next(d for d in _datasets() if d["name"] == name)["queryLines"])


MAINTENANCE_TABLES = (
    "silver_station_maintenance", "gold_rebalancing_priority", "silver_station_status",
    "dim_station_development_sample", "maintenance_cdc_history", "maintenance_cdc_checkpoint",
)  # fmt: skip


def test_open_tickets_are_all_listed_and_a_pending_action_is_held(spark_with_tables) -> None:
    spark = spark_with_tables
    try:
        _insert(spark, "silver_station_maintenance", [
            {"maintenance_id": "MT-1", "station_id": "s1", "status": "OPEN", "severity": "HIGH"},
            {"maintenance_id": "MT-2", "station_id": "s2", "status": "CLOSED", "severity": "HIGH"},
            {"maintenance_id": "MT-3", "station_id": "s3", "status": "IN_PROGRESS",
             "severity": "LOW"},
        ])  # fmt: skip
        _insert(spark, "gold_rebalancing_priority", [
            {"station_id": "s1", "station_name": "One", "priority_score": 9,
             "action": "DELIVER_BIKES", "observed_at": "2026-10-12 07:00:00"},
            {"station_id": "s4", "station_name": "Four", "priority_score": 5,
             "action": "COLLECT_BIKES", "observed_at": "2026-10-12 07:00:00"},
        ])  # fmt: skip
        _insert(spark, "silver_station_status", [
            {"station_id": "s1", "availability_status": "LOW_BIKES",
             "observed_at": "2026-10-12 07:00:00"},
            {"station_id": "s3", "availability_status": "AVAILABLE",
             "observed_at": "2026-10-12 07:00:00"},
        ])  # fmt: skip
        _insert(spark, "dim_station_development_sample", [
            {"station_id": "s1", "station_name": "One"},
            {"station_id": "s3", "station_name": "Three"},
        ])  # fmt: skip

        tickets = [r.asDict() for r in spark.sql(_dataset("maintenance_conflicts")).collect()]
        assert [t["maintenance_id"] for t in tickets] == ["MT-1", "MT-3"]  # CLOSED excluded
        held, quiet = tickets
        assert (held["station_name"], held["availability_status"], held["priority_score"]) == (
            "One", "LOW_BIKES", 9)  # fmt: skip
        assert held["dispatch_note"].startswith("HOLD")
        assert quiet["priority_score"] is None  # an open ticket without an action stays visible
        assert quiet["dispatch_note"] == "no rebalancing action pending"

        priorities = {
            r["station_id"]: r["maintenance_hold"]
            for r in spark.sql(_dataset("ops_priorities")).collect()
        }
        assert priorities == {"s1": "HOLD - open maintenance ticket", "s4": "clear"}
    finally:
        for table in MAINTENANCE_TABLES:
            spark.sql(f"TRUNCATE TABLE spark_catalog.urbanflow_final_dev.{table}")


def test_the_checkpoint_dataset_separates_processed_and_last_data_change_versions(
    spark_with_tables,
) -> None:
    """After the live batch_003: checkpoint 4 passed the OPTIMIZE; the last data change is 3."""
    spark = spark_with_tables
    try:
        _insert(spark, "maintenance_cdc_checkpoint", [
            {"pipeline_key": "station_maintenance", "processed_version": 4}])  # fmt: skip
        _insert(
            spark,
            "maintenance_cdc_history",
            [{"change_id": f"c{n}", "commit_version": 1 if n < 5 else 3} for n in range(11)],
        )
        row = spark.sql(_dataset("maintenance_checkpoint")).first().asDict()
        assert (row["processed_version"], row["last_data_change_version"]) == (4, 3)
        assert row["history_events"] == 11
    finally:
        for table in MAINTENANCE_TABLES:
            spark.sql(f"TRUNCATE TABLE spark_catalog.urbanflow_final_dev.{table}")


def test_text_widget_lines_keep_their_breaks_when_lakeview_concatenates_them() -> None:
    """Regression (deployed 2026-10-13): Lakeview joined title and body into one heading line."""
    NL = chr(10)
    definition = build_dashboard.build("personal_dev")
    texts = [
        item["widget"]["multilineTextboxSpec"]["lines"]
        for page in definition["pages"]
        for item in page["layout"]
        if "multilineTextboxSpec" in item["widget"]
    ]
    assert len(texts) >= 5
    for lines in texts:
        assert all(line.endswith(NL * 2) for line in lines[:-1]), lines[:2]
        rendered = "".join(lines)
        if rendered.startswith("## "):
            heading = rendered.split(NL, 1)[0]
            assert "**" not in heading, heading  # the bold body is no longer part of the heading

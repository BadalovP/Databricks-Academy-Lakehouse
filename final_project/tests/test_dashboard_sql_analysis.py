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
# Not created yet in DEV: the assistant writes it on its first live evaluation.
SCHEMAS["rag_evaluation_results"] = [
    ["evaluation_id", "string"], ["evaluated_at", "string"], ["variant", "string"],
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

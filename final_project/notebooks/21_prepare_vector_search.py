# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Provision and synchronize AI Search
# MAGIC
# MAGIC Default: print the plan and inspect existing resources only. With
# MAGIC `allow_ai_search_mutation=true` it creates the Standard endpoint and Delta Sync index if
# MAGIC missing (or syncs an existing index) and waits until the index is ready. Those steps are
# MAGIC billable and generate embeddings.

# COMMAND ----------

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.rag.config import load_config
from urbanflow.rag.delta_store import latest_version, qualified_name
from urbanflow.rag.runtime import assert_workspace, flag, identifier, index_name, source_table
from urbanflow.rag.vector_search import DeltaSyncIndexSpec, VectorSearchManager

# COMMAND ----------

dbutils.widgets.text("expected_host", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("ai_search_endpoint", "")
dbutils.widgets.text("ai_search_index", "")
dbutils.widgets.text("embedding_endpoint", "")
dbutils.widgets.dropdown("allow_ai_search_mutation", "false", ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
endpoint = dbutils.widgets.get("ai_search_endpoint").strip()
embedding = dbutils.widgets.get("embedding_endpoint").strip()
if not endpoint or not embedding:
    raise ValueError("ai_search_endpoint and embedding_endpoint are required.")
allow_mutation = flag(dbutils.widgets.get("allow_ai_search_mutation"))

cfg = load_config(PROJECT_ROOT / "config" / "rag.yml")
search = cfg.raw["ai_search"]
# The source table and index are always derived from THIS target's catalog and schema.
spec = DeltaSyncIndexSpec(
    endpoint_name=endpoint,
    index_name=index_name(catalog, schema, dbutils.widgets.get("ai_search_index")),
    source_table_name=source_table(catalog, schema, str(cfg.raw["tables"]["chunks"])),
    embedding_model_endpoint_name=embedding,
    primary_key=search["primary_key"],
    embedding_source_column=search["embedding_source_column"],
    columns_to_sync=tuple(search["columns_to_sync"]),
)
print(json.dumps({"workspace": host, "plan": spec.as_create_kwargs()}, indent=2))

# COMMAND ----------

manager = VectorSearchManager()
if allow_mutation:
    # Read-only count of the source table: the index passes only when it holds every row.
    expected_rows = spark.table(spec.source_table_name).count()
    if expected_rows == 0:
        raise RuntimeError(f"{spec.source_table_name} is empty; ingest before indexing.")
    print({"source_rows": expected_rows})
    # Bounded waits: endpoint <= 25 min, then indexing <= 25 min (task timeout 60 min).
    report = manager.provision(
        spec,
        expected_rows=expected_rows,
        allow_mutation=True,
        endpoint_timeout_seconds=1500,
        index_timeout_seconds=1500,
    )
    print(json.dumps(report, indent=2, default=str))
    # Read-only: Delta versions after provisioning; indexing must not write to any UrbanFlow 2.0 table.
    table_versions = {
        name: latest_version(spark, qualified_name(catalog, schema, str(table)))
        for name, table in cfg.raw["tables"].items()
    }
    print({"table_versions_after": table_versions})
    result = {"status": "READY", **report, "table_versions_after": table_versions}
else:
    report = manager.inspect(spec)
    print(json.dumps(report, indent=2, default=str))
    print("DRY RUN: no endpoint or index was created and no synchronization was triggered.")
    result = {
        "status": "DRY_RUN",
        "index": spec.index_name,
        "endpoint_status": report["endpoint_status"],
        "index_status": report["index_status"],
    }
    # Readiness gate (read-only): an existing index must be ready and hold every source row, so
    # evaluation never queries a half-built or stale index. A failure stops the Job here.
    if report["index_status"] == "present":
        source_rows = spark.table(spec.source_table_name).count()
        readiness = manager.verify(
            spec, expected_rows=source_rows, timeout_seconds=120, poll_seconds=15
        )
        print(json.dumps({"readiness_gate": "PASS", **readiness}, indent=2, default=str))
        result.update(status="READY_READ_ONLY", source_rows=source_rows, readiness=readiness)

# COMMAND ----------

# Exit in its own cell: the exit discards the printed output of the cell that calls it.
dbutils.notebook.exit(json.dumps(result, default=str))

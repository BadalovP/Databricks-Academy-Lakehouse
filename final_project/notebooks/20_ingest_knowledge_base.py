# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Ingest fictional IT operations documents
# MAGIC
# MAGIC Loads the fictional runbooks, chunks them, and upserts one full corpus snapshot into
# MAGIC Unity Catalog Delta tables. Writes are disabled unless `allow_table_writes=true`.
# MAGIC Removed or edited documents are pruned, so the AI Search index never serves stale chunks.

# COMMAND ----------

# Import the project package from the bundle-synced src/ folder rather than an installed wheel:
# serverless caches a custom wheel by version, so a redeploy could otherwise run old code.
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.rag.chunking import ChunkingConfig, chunk_documents
from urbanflow.rag.config import load_config
from urbanflow.rag.delta_store import (
    change_data_feed_enabled,
    create_lab_tables,
    latest_version,
    merge_chunks,
    merge_documents,
    qualified_name,
    snapshot_fingerprint,
    table_snapshot,
)
from urbanflow.rag.ingestion import load_documents
from urbanflow.rag.runtime import assert_workspace, flag, identifier

# COMMAND ----------

# Every value comes from the Job; empty defaults mean nothing silently targets another schema.
dbutils.widgets.text("expected_host", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.dropdown("allow_table_writes", "false", ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
allow_write = flag(dbutils.widgets.get("allow_table_writes"))
print({"workspace": host, "target": f"{catalog}.{schema}", "write_enabled": allow_write})

# COMMAND ----------

cfg = load_config(PROJECT_ROOT / "config" / "rag.yml")
settings = cfg.chunking
documents = load_documents([PROJECT_ROOT / "knowledge_base"])
chunks = chunk_documents(
    documents,
    ChunkingConfig(settings.max_chars, settings.overlap_chars, settings.min_chunk_chars),
)
if not documents or not chunks:
    raise RuntimeError("The corpus produced no documents or chunks; refusing to continue.")
print({"documents": len(documents), "chunks": len(chunks)})

# COMMAND ----------

# Each table is reconciled against the local corpus: row count, unique keys, a fingerprint of
# every (key, content_hash) pair and populated ingested_at. MERGE metrics come from both the
# statement result and the Delta commit; anything not reported is "unavailable", never 0.
tables = {name: str(cfg.raw["tables"][name]) for name in cfg.raw["tables"]}
if allow_write:
    create_lab_tables(spark, catalog=catalog, schema=schema, tables=tables, allow_create=True)
    targets = {
        "documents": (qualified_name(catalog, schema, tables["documents"]), "document_id", documents),
        "chunks": (qualified_name(catalog, schema, tables["chunks"]), "chunk_id", chunks),
    }
    merges = {
        "documents": merge_documents(spark, targets["documents"][0], documents, allow_write=True),
        "chunks": merge_chunks(spark, targets["chunks"][0], chunks, allow_write=True),
    }
    report, problems = {}, []
    for name, (table, key, items) in targets.items():
        snapshot = table_snapshot(spark, table, key)
        expected = snapshot_fingerprint(items, key)
        report[name] = {
            "merge": merges[name],
            "table": snapshot,
            "expected_rows": len(items),
            "fingerprint_matches": snapshot["fingerprint"] == expected,
        }
        if snapshot["row_count"] != len(items) or snapshot["distinct_keys"] != len(items):
            problems.append(f"{name}: {snapshot['row_count']} rows / {snapshot['distinct_keys']} keys")
        if snapshot["fingerprint"] != expected:
            problems.append(f"{name}: (key, content_hash) fingerprint differs from the corpus")
        if snapshot["null_ingested_at"]:
            problems.append(f"{name}: {snapshot['null_ingested_at']} rows without ingested_at")
    report["chunks"]["change_data_feed"] = change_data_feed_enabled(spark, targets["chunks"][0])
    if not report["chunks"]["change_data_feed"]:
        problems.append("chunks: Change Data Feed is not enabled")
    print(json.dumps(report, indent=2, default=str))
    if problems:
        raise RuntimeError(f"Reconciliation failed: {problems}")
    result = {"status": "PASS", **report}
else:
    print("DRY RUN: no schema, tables, or rows were created.")
    result = {"status": "DRY_RUN", "documents": len(documents), "chunks": len(chunks)}
    # Read-only baseline of existing tables, so later tasks can prove they changed nothing.
    existing = {}
    for name, table_name in tables.items():
        if spark.catalog.tableExists(f"{catalog}.{schema}.{table_name}"):
            existing[name] = {
                "version": latest_version(spark, qualified_name(catalog, schema, table_name))
            }
    for name, key in (("documents", "document_id"), ("chunks", "chunk_id")):
        if name in existing:
            existing[name].update(
                table_snapshot(spark, qualified_name(catalog, schema, tables[name]), key)
            )
    result["existing_tables"] = existing
print(json.dumps(result, default=str))

# COMMAND ----------

# Exit in its own cell: the exit discards the printed output of the cell that calls it.
dbutils.notebook.exit(json.dumps(result, default=str))

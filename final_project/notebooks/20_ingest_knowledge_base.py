# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Ingest the mobility operations knowledge base
# MAGIC
# MAGIC **Business context:** Operators ask how to rebalance a station, when to close one for maintenance or what a KPI means. The answers are in eight UrbanFlow operating procedures. This notebook turns them into governed Delta tables that the AI Search index and the RAG assistant read.
# MAGIC
# MAGIC **Prerequisites:** The target schema exists; the RAG Job runs on serverless compute. The procedures in `knowledge_base/` are synced with the bundle. Writes happen only with `allow_table_writes=true`.
# MAGIC
# MAGIC **Learning objectives:** Load and chunk documents deterministically, upsert one full corpus snapshot with MERGE (pruning removed documents), and reconcile every table against the local corpus.
# MAGIC
# MAGIC **Academy labs:** Lab 12 (RAG ingestion, chunking, governed document store), Lab 4 (MERGE), Lab 10 (Change Data Feed feeding Delta Sync), Lab 7 (reconciliation).
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-13 (Final Project run 870039412110159): 8 documents and 30 chunks MERGEd, IDs and content hashes equal to the corpus, Change Data Feed on the chunk table. Two later runs found the tables already matching and skipped the MERGE (no commit). Offline tests cover loading, chunking, the MERGE statements and the reconciliation fingerprint.
# MAGIC
# MAGIC The procedures are **fictional training documents** written for this project; they describe no real operator.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the project code
# MAGIC
# MAGIC **What:** Put the bundle-synced `src/` folder on the import path and import the RAG helpers.
# MAGIC
# MAGIC **Why:** Serverless caches a custom wheel by version, so importing the synced source guarantees each deployment runs the code that was reviewed.
# MAGIC
# MAGIC **Input:** The deployed bundle files.
# MAGIC
# MAGIC **Output:** Imported modules.
# MAGIC
# MAGIC **Key concepts:** Bundle sync, serverless environments.
# MAGIC
# MAGIC **Expected result:** No output.
# MAGIC
# MAGIC **How to explain it:** "Every task imports exactly the code in this deployment."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.automation import assert_serverless_compute
from urbanflow.rag.config import load_config
from urbanflow.rag.pipeline import existing_table_baseline, ingest_and_reconcile, load_corpus
from urbanflow.rag.runtime import assert_workspace, flag, identifier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and refuse unsafe runs
# MAGIC
# MAGIC **What:** Read the target and the write switch, confirm the workspace host and serverless compute.
# MAGIC
# MAGIC **Why:** Empty defaults mean nothing silently targets another schema, and a wrong workspace or a classic cluster stops the task before any read.
# MAGIC
# MAGIC **Input:** Job parameters `expected_host`, `catalog`, `schema`, `allow_table_writes`.
# MAGIC
# MAGIC **Output:** Validated settings.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults, identifier validation, serverless-only compute.
# MAGIC
# MAGIC **Expected result:** The workspace, target and `write_enabled` flag are printed.
# MAGIC
# MAGIC **How to explain it:** "The task writes nothing unless the Job explicitly allows it, and only into its own schema."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

dbutils.widgets.text("expected_host", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.dropdown("allow_table_writes", "false", ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
assert_serverless_compute(spark.conf.get)
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
allow_write = flag(dbutils.widgets.get("allow_table_writes"))
print({"workspace": host, "target": f"{catalog}.{schema}", "write_enabled": allow_write})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Load and chunk the corpus
# MAGIC
# MAGIC **What:** Read the eight procedures and split them into section-aware chunks.
# MAGIC
# MAGIC **Why:** Retrieval works on chunks; deterministic chunk IDs and content hashes let the MERGE skip unchanged chunks.
# MAGIC
# MAGIC **Input:** `knowledge_base/*.md` and the chunking settings in `config/rag.yml`.
# MAGIC
# MAGIC **Output:** Document and chunk objects in memory.
# MAGIC
# MAGIC **Key concepts:** Chunk size and overlap, stable identifiers, content hashing.
# MAGIC
# MAGIC **Expected result:** 8 documents and a non-zero chunk count; an empty corpus stops the task.
# MAGIC
# MAGIC **How to explain it:** "The same files always produce the same chunks with the same IDs."
# MAGIC
# MAGIC **Rerun and cost considerations:** Local processing only; free.

# COMMAND ----------

cfg = load_config(PROJECT_ROOT / "config" / "rag.yml")
documents, chunks = load_corpus(cfg, PROJECT_ROOT)
print({"documents": len(documents), "chunks": len(chunks)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Upsert and reconcile, or record a read-only baseline
# MAGIC
# MAGIC **What:** With writes enabled, MERGE the corpus snapshot into the documents and chunks tables and reconcile them; otherwise read the existing table versions only.
# MAGIC
# MAGIC **Why:** Removed or edited documents are pruned, so the index never serves stale chunks. The dry-run baseline lets later tasks prove they changed nothing.
# MAGIC
# MAGIC **Input:** The chunks from step 3 and the tables named in `config/rag.yml`.
# MAGIC
# MAGIC **Output:** `PASS` with MERGE metrics and table snapshots, or `DRY_RUN` with the existing versions.
# MAGIC
# MAGIC **Key concepts:** MERGE with delete-by-source, Change Data Feed (Delta Sync reads it), a (key, content_hash) fingerprint.
# MAGIC
# MAGIC **Expected result:** Row count, distinct keys and fingerprint match the corpus, CDF is on and `ingested_at` is populated; any mismatch raises.
# MAGIC
# MAGIC **How to explain it:** "The table equals the files exactly, and the check proves it row by row."
# MAGIC
# MAGIC **Rerun and cost considerations:** A rerun with unchanged files writes no rows. Serverless SQL is billed per run; the tables are small.

# COMMAND ----------

if allow_write:
    result = ingest_and_reconcile(spark, cfg, catalog, schema, documents, chunks)
else:
    print("DRY RUN: no schema, tables, or rows were created.")
    result = {
        "status": "DRY_RUN",
        "documents": len(documents),
        "chunks": len(chunks),
        "existing_tables": existing_table_baseline(spark, cfg, catalog, schema),
    }
print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Return the ingestion report
# MAGIC
# MAGIC **What:** Return the report to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The report from step 4.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `PASS` or `DRY_RUN`.
# MAGIC
# MAGIC **How to explain it:** "Each run leaves a result that a reviewer can check without opening the notebook."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A RAG corpus is data like any other: it is loaded deterministically, upserted with MERGE, reconciled against its source and governed in Unity Catalog. Change Data Feed on the chunks table is what lets the AI Search Delta Sync index update incrementally.
# MAGIC
# MAGIC **Common errors:** Appending instead of merging, which duplicates chunks; forgetting to prune removed documents; creating the chunks table without Change Data Feed, which Delta Sync requires.
# MAGIC
# MAGIC **Troubleshooting:** A fingerprint mismatch means the table and the files differ - rerun with writes enabled; a missing-schema error means the storage setup has not been run for this target.
# MAGIC
# MAGIC **Review questions:** Why is a content hash part of the fingerprint? Why does the dry run still read the table versions?
# MAGIC
# MAGIC **Presentation summary:** Eight fictional mobility procedures become a governed, reconciled chunk table that the assistant's index is built from.

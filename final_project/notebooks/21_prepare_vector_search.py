# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Provision and synchronize the AI Search index
# MAGIC
# MAGIC **Business context:** The assistant must find the right procedure section for a question worded in an operator's own terms. A Delta Sync index over the chunk table provides hybrid (vector plus keyword) search that follows the governed table automatically.
# MAGIC
# MAGIC **Prerequisites:** `20_ingest_knowledge_base.py` has filled the chunk table with Change Data Feed on. An AI Search endpoint name and an embedding endpoint are configured for the target. Creating or syncing is allowed only with `allow_ai_search_mutation=true`.
# MAGIC
# MAGIC **Learning objectives:** Plan a Delta Sync index from configuration, provision it with bounded waits, and gate evaluation on a ready index that holds every source row.
# MAGIC
# MAGIC **Academy labs:** Lab 12 (AI Search / Vector Search, Delta Sync, embeddings), Lab 10 (Change Data Feed as the sync source), Lab 6 (Unity Catalog governance of the index).
# MAGIC
# MAGIC **Actual validation:** Offline tests cover the index plan, the configuration checks and the provisioning state machine with a fake client; in the Final Project this notebook has not been run in a workspace yet.
# MAGIC
# MAGIC **Cost note:** an AI Search endpoint is billed while it exists, and syncing generates embeddings. The default run only inspects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the project code
# MAGIC
# MAGIC **What:** Put the synced `src/` folder on the import path and import the index helpers.
# MAGIC
# MAGIC **Why:** Each deployment must run the code that was reviewed, not a cached wheel.
# MAGIC
# MAGIC **Input:** The deployed bundle files.
# MAGIC
# MAGIC **Output:** Imported modules.
# MAGIC
# MAGIC **Key concepts:** Bundle sync.
# MAGIC
# MAGIC **Expected result:** No output.
# MAGIC
# MAGIC **How to explain it:** "The same reviewed code runs in every target."
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
from urbanflow.rag.pipeline import index_spec, inspect_index, provision_index
from urbanflow.rag.runtime import assert_workspace, flag, identifier
from urbanflow.rag.vector_search import VectorSearchManager

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and print the index plan
# MAGIC
# MAGIC **What:** Validate the workspace, compute and target, then build the Delta Sync index plan.
# MAGIC
# MAGIC **Why:** The source table and the index name are always derived from this target's catalog and schema, so a DEV run can never index PROD data or the reverse.
# MAGIC
# MAGIC **Input:** Job parameters and `config/rag.yml`.
# MAGIC
# MAGIC **Output:** The printed creation arguments.
# MAGIC
# MAGIC **Key concepts:** Delta Sync index, primary key, embedding source column, columns to sync.
# MAGIC
# MAGIC **Expected result:** A plan naming `<catalog>.<schema>.rag_document_chunks` as the source.
# MAGIC
# MAGIC **How to explain it:** "We print exactly what would be created before anything is created."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

names = ("expected_host", "catalog", "schema", "ai_search_endpoint", "ai_search_index",
         "embedding_endpoint")  # fmt: skip
for name in names:
    dbutils.widgets.text(name, "")
dbutils.widgets.dropdown("allow_ai_search_mutation", "false", ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
assert_serverless_compute(spark.conf.get)
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
allow_mutation = flag(dbutils.widgets.get("allow_ai_search_mutation"))
cfg = load_config(PROJECT_ROOT / "config" / "rag.yml")
spec = index_spec(
    cfg,
    catalog,
    schema,
    endpoint=dbutils.widgets.get("ai_search_endpoint"),
    embedding_endpoint=dbutils.widgets.get("embedding_endpoint"),
    index=dbutils.widgets.get("ai_search_index"),
)
print(json.dumps({"workspace": host, "plan": spec.as_create_kwargs()}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Provision, or inspect and gate on readiness
# MAGIC
# MAGIC **What:** With mutation allowed, create the endpoint and index if missing (or sync an existing index) and wait until it holds every source row. Otherwise inspect only, and if an index exists, require it to be ready and complete.
# MAGIC
# MAGIC **Why:** Evaluation must never query a half-built or stale index; the readiness gate stops the Job before any model call.
# MAGIC
# MAGIC **Input:** The plan from step 2 and the source table row count.
# MAGIC
# MAGIC **Output:** `READY`, `READY_READ_ONLY` or `DRY_RUN` with endpoint and index status.
# MAGIC
# MAGIC **Key concepts:** TRIGGERED sync, bounded waits, row-count readiness, no writes to project tables.
# MAGIC
# MAGIC **Expected result:** The indexed row count equals the source row count; the project table versions are unchanged by indexing.
# MAGIC
# MAGIC **How to explain it:** "The index is checked against its source table before the assistant may use it."
# MAGIC
# MAGIC **Rerun and cost considerations:** Provisioning creates a billable endpoint and embeddings; waits are capped at 25 minutes each. Inspection is read-only.

# COMMAND ----------

manager = VectorSearchManager()
if allow_mutation:
    result = provision_index(spark, cfg, catalog, schema, manager, spec)
else:
    result = inspect_index(spark, manager, spec)
    if result["status"] == "DRY_RUN":
        print("DRY RUN: no endpoint or index was created and no synchronization was triggered.")
print(json.dumps(result, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Return the index report
# MAGIC
# MAGIC **What:** Return the report to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The report from step 3.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `status` is `READY`, `READY_READ_ONLY` or `DRY_RUN`.
# MAGIC
# MAGIC **How to explain it:** "The Job records whether the index was ready when evaluation started."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A Delta Sync index is a governed derivative of a Delta table: it is planned from configuration, synchronized from Change Data Feed, and must be verified against its source before use.
# MAGIC
# MAGIC **Common errors:** Indexing a table without Change Data Feed; pointing a DEV index at a PROD table; querying the index before the initial sync has finished.
# MAGIC
# MAGIC **Troubleshooting:** If the readiness gate fails, compare the indexed row count with the table count and run once with `allow_ai_search_mutation=true` to sync; if the endpoint is missing, check the endpoint name and the workspace's AI Search entitlement.
# MAGIC
# MAGIC **Review questions:** Why does the gate compare row counts rather than just checking the status? Why is the endpoint left unchanged in a dry run?
# MAGIC
# MAGIC **Presentation summary:** The assistant searches a hybrid index that is provably in step with the governed chunk table.

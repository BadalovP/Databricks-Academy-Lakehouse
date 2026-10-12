# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Ask the mobility operations assistant (one question)
# MAGIC
# MAGIC **Business context:** A dispatcher wants a quick, cited answer such as "How is a station selected for bike delivery?" without searching eight procedures by hand. This interactive notebook asks the assistant one question and shows the answer with its sources.
# MAGIC
# MAGIC **Prerequisites:** The chunk table and the AI Search index exist and passed the readiness gate in `21_prepare_vector_search.py`; a chat model serving endpoint is available. Live inference is opt-in with `run_live_inference=true`. This notebook is not a Job task.
# MAGIC
# MAGIC **Learning objectives:** Combine hybrid retrieval with a grounded prompt, read numbered citations, and recognise a refusal when the procedures do not answer the question.
# MAGIC
# MAGIC **Academy labs:** Lab 12 (retrieval-augmented generation, grounding, citations, refusals), Lab 6 (governed access to the index and model endpoint).
# MAGIC
# MAGIC **Actual validation:** Offline tests run the same RAG service with a fixture retriever and an extractive model; in the Final Project this notebook has not been run in a workspace yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the project code
# MAGIC
# MAGIC **What:** Put the synced `src/` folder on the import path and import the assistant builder.
# MAGIC
# MAGIC **Why:** The demo uses exactly the service that the evaluation scores.
# MAGIC
# MAGIC **Input:** The deployed bundle files.
# MAGIC
# MAGIC **Output:** Imported modules.
# MAGIC
# MAGIC **Key concepts:** One implementation for demo and evaluation.
# MAGIC
# MAGIC **Expected result:** No output.
# MAGIC
# MAGIC **How to explain it:** "What you see here is what we measured."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.rag.config import load_config
from urbanflow.rag.pipeline import build_live_service
from urbanflow.rag.runtime import assert_workspace, flag, identifier, index_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read the question and stop unless live inference is approved
# MAGIC
# MAGIC **What:** Read the question and the target, confirm the workspace, and exit unless `run_live_inference=true`.
# MAGIC
# MAGIC **Why:** Every answer costs one search and at least one model call, so the default does nothing billable.
# MAGIC
# MAGIC **Input:** Widgets; the default question comes from the evaluation set.
# MAGIC
# MAGIC **Output:** A dry-run exit, or validated settings.
# MAGIC
# MAGIC **Key concepts:** Opt-in inference, workspace pinning.
# MAGIC
# MAGIC **Expected result:** `DRY RUN` unless live inference was switched on.
# MAGIC
# MAGIC **How to explain it:** "Nothing is charged until someone deliberately asks."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

dbutils.widgets.text("question", "How is a station selected for bike delivery?")
names = ("expected_host", "catalog", "schema", "ai_search_endpoint", "ai_search_index",
         "chat_endpoint")  # fmt: skip
for name in names:
    dbutils.widgets.text(name, "")
dbutils.widgets.dropdown("run_live_inference", "false", ["false", "true"])

assert_workspace(dbutils.widgets.get("expected_host"))
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
live = flag(dbutils.widgets.get("run_live_inference"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Exit when live inference is off
# MAGIC
# MAGIC **What:** End the notebook with a dry-run message.
# MAGIC
# MAGIC **Why:** `dbutils.notebook.exit` discards the output of the cell that calls it, so it has its own cell.
# MAGIC
# MAGIC **Input:** The `live` flag.
# MAGIC
# MAGIC **Output:** A dry-run notebook result, or nothing.
# MAGIC
# MAGIC **Key concepts:** Fail-closed defaults.
# MAGIC
# MAGIC **Expected result:** `DRY RUN` by default.
# MAGIC
# MAGIC **How to explain it:** "The safe default ends here."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

if not live:
    dbutils.notebook.exit("DRY RUN: set run_live_inference=true only after approval.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Ask the assistant
# MAGIC
# MAGIC **What:** Build the live service (hybrid AI Search retriever plus chat model) and answer the question.
# MAGIC
# MAGIC **Why:** The answer must come only from retrieved procedure excerpts, with a numbered citation for each claim, or be a clear refusal.
# MAGIC
# MAGIC **Input:** The question, the index and the chat endpoint.
# MAGIC
# MAGIC **Output:** The answer, the cited sections with scores, the model, latency and any error.
# MAGIC
# MAGIC **Key concepts:** Hybrid retrieval, context budget, grounded system prompt, citations.
# MAGIC
# MAGIC **Expected result:** A short answer citing `01_station_rebalancing` for the default question.
# MAGIC
# MAGIC **How to explain it:** "Each sentence points to the procedure section it came from."
# MAGIC
# MAGIC **Rerun and cost considerations:** One search and one to three model calls per question.

# COMMAND ----------

from databricks.ai_search.client import AISearchClient

service, _ = build_live_service(
    load_config(PROJECT_ROOT / "config" / "rag.yml"),
    PROJECT_ROOT,
    search_client=AISearchClient(),
    ai_search_endpoint=dbutils.widgets.get("ai_search_endpoint"),
    index=index_name(catalog, schema, dbutils.widgets.get("ai_search_index")),
    chat_endpoint=dbutils.widgets.get("chat_endpoint"),
)
result = service.answer(dbutils.widgets.get("question"))
print(result.answer)
for source in result.sources:
    print(f"- {source.section_ref} (score={source.score:.3f})")
print({"model": result.model, "latency_ms": result.latency_ms, "error": result.error})

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC Retrieval-augmented generation answers from the governed procedures rather than the model's memory: the retriever chooses excerpts, the prompt restricts the model to them, and citations make every claim checkable.
# MAGIC
# MAGIC **Common errors:** Asking before the index is ready; pointing at another target's index; reading a fluent answer without checking its citations.
# MAGIC
# MAGIC **Troubleshooting:** An empty source list means retrieval found nothing - check the index status and the question wording; a model error names the endpoint that failed.
# MAGIC
# MAGIC **Review questions:** What should the assistant do when no excerpt answers the question? Why are document titles shown in each excerpt header?
# MAGIC
# MAGIC **Presentation summary:** One question, one grounded and cited answer from UrbanFlow's own procedures.

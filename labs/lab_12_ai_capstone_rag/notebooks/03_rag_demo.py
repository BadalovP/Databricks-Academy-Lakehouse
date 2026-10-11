# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12 - Grounded RAG demonstration (one question)
# MAGIC
# MAGIC Interactive notebook, not a Job task. Live AI Search and model inference are opt-in
# MAGIC because they incur charges. Fill the widgets for the environment you are working in.

# COMMAND ----------

import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lab12.config import load_config
from lab12.ingestion import document_titles, load_documents, section_headings
from lab12.rag import DatabricksChatModel, RagService
from lab12.retrieval import DatabricksAISearchRetriever
from lab12.runtime import assert_workspace, flag, index_name

# COMMAND ----------

dbutils.widgets.text("question", "How should an Aurora streaming replay be performed safely?")
dbutils.widgets.text("expected_host", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("ai_search_endpoint", "")
dbutils.widgets.text("ai_search_index", "")
dbutils.widgets.text("chat_endpoint", "")
dbutils.widgets.dropdown("run_live_inference", "false", ["false", "true"])

assert_workspace(dbutils.widgets.get("expected_host"))
if not flag(dbutils.widgets.get("run_live_inference")):
    dbutils.notebook.exit("DRY RUN: set run_live_inference=true only after approval.")

# COMMAND ----------

from databricks.ai_search.client import AISearchClient

cfg = load_config(PROJECT_ROOT / "config" / "default.yml")
generation = cfg.raw["generation"]
retriever = DatabricksAISearchRetriever(
    AISearchClient(),
    dbutils.widgets.get("ai_search_endpoint"),
    index_name(
        dbutils.widgets.get("catalog"),
        dbutils.widgets.get("schema"),
        dbutils.widgets.get("ai_search_index"),
    ),
    query_type=cfg.raw["retrieval"].get("query_type", "HYBRID"),
)
model = DatabricksChatModel(
    dbutils.widgets.get("chat_endpoint"),
    temperature=float(generation["temperature"]),
    max_tokens=int(generation["max_tokens"]),
)
# Titles come from the same synced corpus that was indexed; they label each excerpt's system.
corpus = load_documents([PROJECT_ROOT / "data" / "sample_docs"])
titles = document_titles(corpus)
service = RagService(
    retriever,
    model,
    top_k=cfg.retrieval.top_k,
    max_context_chars=cfg.retrieval.max_context_chars,
    titles=titles,
    headings=section_headings(corpus),
    max_supplemental_queries=int(cfg.raw["retrieval"].get("max_supplemental_queries", 0)),
)
result = service.answer(dbutils.widgets.get("question"))
print(result.answer)
for source in result.sources:
    print(f"- {source.section_ref} (score={source.score:.3f})")
print({"model": result.model, "latency_ms": result.latency_ms, "error": result.error})

# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12 - Evaluation: no-retrieval baseline vs RAG
# MAGIC
# MAGIC - `run_live_inference=false` (default): deterministic offline fixture only. No charges.
# MAGIC - `run_live_inference=true`: the SAME chat model answers each question twice - once with no
# MAGIC   context (baseline) and once with AI Search retrieval (RAG) - and both are scored.
# MAGIC   With `allow_table_writes=true` the scores go to `evaluation_results` and redacted query
# MAGIC   events to `query_monitoring`.
# MAGIC
# MAGIC Live cost scales with the number of questions (two model calls and one search per question).
# MAGIC `eval_question_ids` (comma-separated, e.g. `q01,q09`) runs exactly those questions instead of
# MAGIC the first `eval_question_limit`; `eval_include_baseline=false` skips the no-retrieval calls.
# MAGIC The defaults (empty list, baseline on) keep the full 18-question baseline-vs-RAG evaluation.

# COMMAND ----------

import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lab12.cli import build_offline_service
from lab12.config import load_config
from lab12.delta_store import EVALUATION_SCHEMA, MONITORING_SCHEMA, append_rows, qualified_name
from lab12.evaluation import (
    baseline_answer,
    citation_map,
    evaluate,
    live_call_plan,
    load_cases,
    run_evaluation,
    score_answer,
    score_to_row,
    select_cases,
    summarize,
    summarize_by_category,
    summarize_generation,
)
from lab12.ingestion import document_titles, load_documents, section_headings
from lab12.monitoring import event_from_answer, event_to_row
from lab12.runtime import assert_workspace, flag, identifier, index_name

# COMMAND ----------

dbutils.widgets.text("expected_host", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("ai_search_endpoint", "")
dbutils.widgets.text("ai_search_index", "")
dbutils.widgets.text("chat_endpoint", "")
dbutils.widgets.text("eval_question_limit", "18")
dbutils.widgets.text("eval_question_ids", "")
dbutils.widgets.dropdown("eval_include_baseline", "true", ["true", "false"])
dbutils.widgets.dropdown("run_live_inference", "false", ["false", "true"])
dbutils.widgets.dropdown("allow_table_writes", "false", ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
live = flag(dbutils.widgets.get("run_live_inference"))
allow_write = flag(dbutils.widgets.get("allow_table_writes"))
include_baseline = flag(dbutils.widgets.get("eval_include_baseline"))
limit = int(dbutils.widgets.get("eval_question_limit"))

cfg = load_config(PROJECT_ROOT / "config" / "default.yml")
cases = select_cases(
    load_cases(PROJECT_ROOT / "config" / "evaluation_questions.yml"),
    dbutils.widgets.get("eval_question_ids"),
    limit,
)
plan = (
    live_call_plan(
        len(cases),
        include_baseline,
        int(cfg.raw["retrieval"].get("max_supplemental_queries", 0)),
    )
    if live
    else "offline: no live requests"
)
print(
    {
        "workspace": host,
        "question_ids": [case.question_id for case in cases],
        "include_baseline": include_baseline,
        "live": live,
        "write_enabled": allow_write,
        "live_call_plan": plan,
    }
)

# COMMAND ----------

# Offline (default): deterministic fixture only - no AI Search request and no model call.
if not live:
    service = build_offline_service()
    baseline = (
        [score_answer(case, baseline_answer(case), "offline-baseline") for case in cases]
        if include_baseline
        else []
    )
    rag = evaluate(cases, service.answer, "offline-rag")
    summary = {
        "mode": "offline",
        "question_ids": [case.question_id for case in cases],
        "baseline": summarize(baseline) if include_baseline else "not_run",
        "rag": summarize(rag),
        "by_category": {
            "baseline": summarize_by_category(baseline) if include_baseline else "not_run",
            "rag": summarize_by_category(rag),
        },
    }
    print(json.dumps(summary, indent=2))

# COMMAND ----------

if live:
    from databricks.ai_search.client import AISearchClient

    from lab12.rag import DatabricksChatModel, NoRetrievalBaseline, RagService
    from lab12.retrieval import DatabricksAISearchRetriever

    generation = cfg.raw["generation"]
    index = index_name(catalog, schema, dbutils.widgets.get("ai_search_index"))
    model = DatabricksChatModel(
        dbutils.widgets.get("chat_endpoint"),
        temperature=float(generation["temperature"]),
        max_tokens=int(generation["max_tokens"]),
    )
    retriever = DatabricksAISearchRetriever(
        AISearchClient(),
        dbutils.widgets.get("ai_search_endpoint"),
        index,
        query_type=cfg.raw["retrieval"].get("query_type", "HYBRID"),
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
    # The baseline answerer is not even constructed when it is switched off.
    no_retrieval = NoRetrievalBaseline(model) if include_baseline else None

    run = run_evaluation(
        cases, service.answer, no_retrieval.answer if no_retrieval else None, "live"
    )
    baseline_answers, baseline_scores = run.baseline_answers, run.baseline_scores
    rag_answers, rag_scores = run.rag_answers, run.rag_scores
    summary = {
        "mode": "live",
        "workspace": host,
        "index": index,
        "chat_endpoint": model.model_name,
        "max_tokens": int(generation["max_tokens"]),
        "question_ids": [case.question_id for case in cases],
        "live_call_plan": plan,
        "baseline": summarize(baseline_scores) if include_baseline else "not_run",
        "rag": summarize(rag_scores),
        "by_category": {
            "baseline": summarize_by_category(baseline_scores) if include_baseline else "not_run",
            "rag": summarize_by_category(rag_scores),
        },
        "generation": {
            "baseline": summarize_generation(baseline_answers) if include_baseline else "not_run",
            "rag": summarize_generation(rag_answers),
        },
    }
    print(json.dumps(summary, indent=2))

# COMMAND ----------

# Per-question review. The questions and corpus are fictional evaluation fixtures, so their text is
# printed here for human review of grounding and citations; query_monitoring stays redacted.
if live:
    for position, (case, rag_answer, rag_score) in enumerate(
        zip(cases, rag_answers, rag_scores, strict=True)
    ):
        review = {
            "question_id": case.question_id,
            "category": case.category,
            "question": case.question,
            "expected_sources": list(case.expected_sources),
            "retrieved": [
                {"rank": rank, "source": s.source_name, "section": s.section_ref,
                 "score": round(s.score, 4), "chunk_id": s.chunk_id}
                for rank, s in enumerate(rag_answer.sources, start=1)
            ],
            "rag": {
                "answer": rag_answer.answer,
                "citations": citation_map(rag_answer),
                "error": rag_answer.error,
                "latency_ms": rag_answer.latency_ms,
                "diagnostics": rag_answer.diagnostics,
                "score": score_to_row(rag_score, "review", "n/a")["metrics_json"],
            },
            "baseline": {
                "answer": baseline_answers[position].answer,
                "error": baseline_answers[position].error,
                "latency_ms": baseline_answers[position].latency_ms,
                "diagnostics": baseline_answers[position].diagnostics,
                "score": score_to_row(baseline_scores[position], "review", "n/a")["metrics_json"],
            }
            if include_baseline
            else "not_run",
        }
        print(json.dumps(review, indent=2, default=str))

# COMMAND ----------

# Results are persisted only for a live evaluation; the offline fixture is never written.
if live and allow_write:
    evaluation_id = str(uuid.uuid4())
    evaluated_at = datetime.now(UTC).isoformat()
    tables = cfg.raw["tables"]
    evaluation_rows = [
        score_to_row(score, evaluation_id, evaluated_at) for score in baseline_scores + rag_scores
    ]
    monitoring_rows = [event_to_row(event_from_answer(answer, index)) for answer in rag_answers]
    written = {
        "evaluation_results": append_rows(
            spark,
            qualified_name(catalog, schema, tables["evaluation_results"]),
            evaluation_rows,
            schema=EVALUATION_SCHEMA,
            timestamp_column="evaluated_at",
            allow_write=True,
        ),
        "query_monitoring": append_rows(
            spark,
            qualified_name(catalog, schema, tables["monitoring"]),
            monitoring_rows,
            schema=MONITORING_SCHEMA,
            timestamp_column="query_timestamp",
            allow_write=True,
        ),
    }
    summary.update({"evaluation_id": evaluation_id, "rows_written": written})
    print(json.dumps(summary["rows_written"]))

# COMMAND ----------

# Exit in its own cell: the exit discards the printed output of the cell that calls it.
dbutils.notebook.exit(json.dumps(summary, default=str))

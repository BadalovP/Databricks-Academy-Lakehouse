# Databricks notebook source
# MAGIC %md
# MAGIC # UrbanFlow 2.0 - Evaluate the assistant: no-retrieval baseline vs RAG
# MAGIC
# MAGIC **Business context:** Before operators rely on the assistant, the team must show that retrieval makes answers more correct and better grounded than the same model answering alone, and that it refuses questions the procedures do not cover.
# MAGIC
# MAGIC **Prerequisites:** For live mode, the index passed the readiness gate in `21_prepare_vector_search.py` and a chat endpoint is configured. The default offline mode needs only the synced files.
# MAGIC
# MAGIC **Learning objectives:** Score retrieval recall, answer correctness, citation coverage and refusals; compare a no-retrieval baseline with RAG on the same model; persist scores and redacted monitoring events.
# MAGIC
# MAGIC **Academy labs:** Lab 12 (RAG evaluation, monitoring), Lab 7 (quality checks as gates), Lab 9 (bounded, planned API calls).
# MAGIC
# MAGIC **Actual validation:** Validated live in Personal DEV on 2026-10-13 (Final Project run 712590508231976, evaluation d9e264ca): 18 questions, baseline and RAG on `databricks-gpt-oss-20b`. RAG answer correctness 0.685 vs 0.148 (a lexical keyword metric), retrieval recall 1.0, citation precision 0.967, refusal decisions 18 of 18, 0 RAG errors. A two-question smoke test ran first (run 12773923439415). The offline fixture evaluation runs in the unit tests.
# MAGIC
# MAGIC - `run_live_inference=false` (default): deterministic offline fixture only. No charges.
# MAGIC - `run_live_inference=true`: the same chat model answers each question twice - once without context (baseline) and once with AI Search retrieval (RAG). With `allow_table_writes=true` the scores go to `evaluation_results` and redacted query events to `query_monitoring`.
# MAGIC - `eval_question_ids` (for example `q01,q09`) runs exactly those questions instead of the first `eval_question_limit`; `eval_include_baseline=false` skips the baseline calls.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import the project code
# MAGIC
# MAGIC **What:** Put the synced `src/` folder on the import path and import the evaluation helpers.
# MAGIC
# MAGIC **Why:** Offline and live evaluation share one scoring implementation.
# MAGIC
# MAGIC **Input:** The deployed bundle files.
# MAGIC
# MAGIC **Output:** Imported modules.
# MAGIC
# MAGIC **Key concepts:** One scorer for fixture and live answers.
# MAGIC
# MAGIC **Expected result:** No output.
# MAGIC
# MAGIC **How to explain it:** "Offline and live numbers are computed the same way."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.automation import assert_serverless_compute
from urbanflow.rag import pipeline
from urbanflow.rag.cli import build_offline_service
from urbanflow.rag.config import load_config
from urbanflow.rag.evaluation import live_call_plan, load_cases, run_evaluation, select_cases
from urbanflow.rag.runtime import assert_workspace, flag, identifier, index_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read parameters and select the questions
# MAGIC
# MAGIC **What:** Validate the workspace, compute and target, read the switches and select the evaluation questions.
# MAGIC
# MAGIC **Why:** The number of live calls is printed before any call is made, so the cost of a run is known in advance.
# MAGIC
# MAGIC **Input:** Job parameters and `config/rag_evaluation_questions.yml` (18 fictional questions, including unanswerable ones).
# MAGIC
# MAGIC **Output:** The selected question IDs and the live call plan.
# MAGIC
# MAGIC **Key concepts:** Bounded evaluation, explicit question selection.
# MAGIC
# MAGIC **Expected result:** `offline: no live requests` by default.
# MAGIC
# MAGIC **How to explain it:** "We know how many model calls a run will make before it starts."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free.

# COMMAND ----------

names = ("expected_host", "catalog", "schema", "ai_search_endpoint", "ai_search_index",
         "chat_endpoint", "eval_question_ids")  # fmt: skip
for name in names:
    dbutils.widgets.text(name, "")
dbutils.widgets.text("eval_question_limit", "18")
for name, default in (("eval_include_baseline", "true"), ("run_live_inference", "false"),
                      ("allow_table_writes", "false")):  # fmt: skip
    dbutils.widgets.dropdown(name, default, ["false", "true"])

host = assert_workspace(dbutils.widgets.get("expected_host"))
assert_serverless_compute(spark.conf.get)
catalog = identifier(dbutils.widgets.get("catalog"), "catalog")
schema = identifier(dbutils.widgets.get("schema"), "schema")
live, allow_write, include_baseline = (flag(dbutils.widgets.get(name)) for name in
    ("run_live_inference", "allow_table_writes", "eval_include_baseline"))  # fmt: skip
cfg = load_config(PROJECT_ROOT / "config" / "rag.yml")
cases = select_cases(
    load_cases(PROJECT_ROOT / "config" / "rag_evaluation_questions.yml"),
    dbutils.widgets.get("eval_question_ids"),
    int(dbutils.widgets.get("eval_question_limit")),
)
supplemental = int(cfg.raw["retrieval"].get("max_supplemental_queries", 0))
plan = live_call_plan(len(cases), include_baseline, supplemental) if live else "offline only"
print({"workspace": host, "question_ids": [case.question_id for case in cases],
       "live": live, "write_enabled": allow_write, "live_call_plan": plan})  # fmt: skip

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Offline evaluation (default)
# MAGIC
# MAGIC **What:** Score the fixture retriever and extractive model against the question set, with a deterministic baseline.
# MAGIC
# MAGIC **Why:** It proves the scoring and the retrieval logic without any billable request, and catches regressions in CI.
# MAGIC
# MAGIC **Input:** The selected questions and the committed knowledge base.
# MAGIC
# MAGIC **Output:** Overall and per-category summaries.
# MAGIC
# MAGIC **Key concepts:** Recall@k, correctness, citation coverage, refusal accuracy.
# MAGIC
# MAGIC **Expected result:** Offline RAG recall is at least 0.9 and citation coverage at least 0.85, as asserted in the unit tests. These are fixture numbers, not model quality.
# MAGIC
# MAGIC **How to explain it:** "Offline numbers test our pipeline; only a live run measures the model."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free and deterministic.

# COMMAND ----------

if not live:
    summary = pipeline.offline_summary(cases, build_offline_service(), include_baseline)
    print(json.dumps(summary, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Live evaluation (approved runs only)
# MAGIC
# MAGIC **What:** Build the live assistant and, if enabled, a no-retrieval baseline on the same model; answer and score every selected question.
# MAGIC
# MAGIC **Why:** Comparing the same model with and without retrieval isolates what retrieval adds.
# MAGIC
# MAGIC **Input:** The index, the chat endpoint and the selected questions.
# MAGIC
# MAGIC **Output:** Baseline and RAG summaries, per category and for generation errors and latency.
# MAGIC
# MAGIC **Key concepts:** Paired comparison, generation diagnostics.
# MAGIC
# MAGIC **Expected result:** RAG beats the baseline on correctness and recall and refuses the unanswerable questions.
# MAGIC
# MAGIC **How to explain it:** "Same model, same questions; the only difference is retrieval."
# MAGIC
# MAGIC **Rerun and cost considerations:** The printed call plan is the ceiling: up to three AI Search queries and one RAG chat call per question, plus one baseline chat call when enabled.

# COMMAND ----------

if live:
    from databricks.ai_search.client import AISearchClient

    from urbanflow.rag.rag import NoRetrievalBaseline

    index = index_name(catalog, schema, dbutils.widgets.get("ai_search_index"))
    service, model = pipeline.build_live_service(
        cfg, PROJECT_ROOT, search_client=AISearchClient(), index=index,
        ai_search_endpoint=dbutils.widgets.get("ai_search_endpoint"),
        chat_endpoint=dbutils.widgets.get("chat_endpoint"),
    )  # fmt: skip
    # The baseline answerer is not even constructed when it is switched off.
    baseline = NoRetrievalBaseline(model).answer if include_baseline else None
    run = run_evaluation(cases, service.answer, baseline, "live")
    summary = pipeline.live_summary(
        run, cases, include_baseline, workspace=host, index=index, live_call_plan=plan,
        chat_endpoint=model.model_name, max_tokens=int(cfg.raw["generation"]["max_tokens"]),
    )  # fmt: skip
    print(json.dumps(summary, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Review each answer
# MAGIC
# MAGIC **What:** Print, per question, the retrieved sections, the answer, its citations and its score, next to the baseline.
# MAGIC
# MAGIC **Why:** Aggregate scores hide individual grounding failures; a human reviews each answer. The questions and corpus are fictional fixtures, so printing them is safe; `query_monitoring` stays redacted.
# MAGIC
# MAGIC **Input:** The live run from step 4.
# MAGIC
# MAGIC **Output:** One JSON review record per question.
# MAGIC
# MAGIC **Key concepts:** Citation map, retrieval ranks, error diagnostics.
# MAGIC
# MAGIC **Expected result:** Every citation number maps to a retrieved excerpt that supports the sentence.
# MAGIC
# MAGIC **How to explain it:** "We read the answers, not just the averages."
# MAGIC
# MAGIC **Rerun and cost considerations:** Free; it only formats results already computed.

# COMMAND ----------

if live:
    for record in pipeline.review_records(run, cases, include_baseline):
        print(json.dumps(record, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Persist live results
# MAGIC
# MAGIC **What:** Append the scores to `evaluation_results` and redacted query events to `query_monitoring`.
# MAGIC
# MAGIC **Why:** Scores over time show whether a corpus or prompt change helped; monitoring records latency and errors without storing question text.
# MAGIC
# MAGIC **Input:** The live run and the target tables.
# MAGIC
# MAGIC **Output:** An evaluation ID and the rows written per table.
# MAGIC
# MAGIC **Key concepts:** Append-only evaluation history, redaction.
# MAGIC
# MAGIC **Expected result:** Rows written equal 2 x questions (with baseline) and 1 x questions for monitoring. The offline fixture is never written.
# MAGIC
# MAGIC **How to explain it:** "Each live evaluation becomes a dated, queryable record."
# MAGIC
# MAGIC **Rerun and cost considerations:** Small serverless writes; only with `allow_table_writes=true`.

# COMMAND ----------

if live and allow_write:
    summary.update(pipeline.persist_evaluation(spark, cfg, catalog, schema, run, index))
    print(json.dumps(summary["rows_written"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Return the evaluation summary
# MAGIC
# MAGIC **What:** Return the summary to the Jobs API.
# MAGIC
# MAGIC **Why:** Output printed in the same cell as `dbutils.notebook.exit` is not kept, so the exit has its own cell.
# MAGIC
# MAGIC **Input:** The summary from step 3 or 4.
# MAGIC
# MAGIC **Output:** A JSON notebook result.
# MAGIC
# MAGIC **Key concepts:** Machine-readable evidence.
# MAGIC
# MAGIC **Expected result:** `mode` is `offline` or `live`.
# MAGIC
# MAGIC **How to explain it:** "The Job result says plainly whether these numbers came from a fixture or a model."
# MAGIC
# MAGIC **Rerun and cost considerations:** None.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we learned
# MAGIC
# MAGIC A RAG assistant is evaluated like a pipeline: a fixed question set, a no-retrieval baseline on the same model, metrics for retrieval, correctness, citations and refusals, and a per-answer review. Offline fixture scores test the code; only live scores describe the model.
# MAGIC
# MAGIC **Common errors:** Presenting offline fixture numbers as model quality; evaluating against a stale index; storing raw questions in monitoring tables.
# MAGIC
# MAGIC **Troubleshooting:** Low recall points to chunking or retrieval settings; good recall with low correctness points to the prompt or the model; a high error count names the failing endpoint in the generation summary.
# MAGIC
# MAGIC **Review questions:** Why use the same model for the baseline? Why are unanswerable questions part of the set?
# MAGIC
# MAGIC **Presentation summary:** We measure what retrieval adds, question by question, and keep every live result as governed evidence.

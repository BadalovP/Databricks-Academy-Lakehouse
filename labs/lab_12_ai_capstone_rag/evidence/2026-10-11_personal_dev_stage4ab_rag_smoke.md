# Personal DEV - Stage 4A/4B: code improvements and one live RAG smoke test (2026-10-11)

Scope: deploy the retry fix and the Stage 4 code improvements, then answer **one** evaluation
question live: one HYBRID retrieval and two `databricks-gpt-oss-20b` calls (no-retrieval baseline
and RAG). No table writes, no AI Search mutation, no synchronization, no full evaluation.

## 4A - code changes (deployed)

| Change | Why |
|---|---|
| `disable_auto_optimization: true` on all three tasks (+ validator rule) | serverless retried a failed task by default in Stage 3 |
| Notebook 02 readiness gate (read-only `verify()`): ready, `indexed_row_count` = source rows, endpoint ONLINE/STANDARD, config matches; 120 s bound | evaluation must never query a half-built or stale index |
| Per-call diagnostics: `finish_reason`, prompt/completion/reasoning/total tokens, `generation_ms`, `retrieval_ms`, context size, content block types, `empty_response`, `truncated` (metadata only, no text) | GPT-OSS is a reasoning model; truncation and empty answers must be visible |
| **Defect fixed:** an empty completion used to be replaced by the "not enough information" sentence - and score as *correct* on unanswerable questions. It is now `EmptyModelResponse`; `finish_reason=length` is `TruncatedModelResponse` | an unusable model response must never look like a valid refusal |
| Citations checked against retrieved sources (`invalid_citations`, citation -> section map; grouped `[1, 2]` parsed) | a `[5]` with four sources passed unnoticed |
| Diagnostics persisted in `query_monitoring.evaluation_json` and `evaluation_results.metrics_json` (on write runs) | operational evidence without raw text |
| Unchanged-snapshot MERGE skipped (same keys and content hashes, no null `ingested_at`) | Stage 2's rerun wrote an empty Delta version |
| No hidden retries: `DatabricksOpenAI(max_retries=0)` (openai default 2), `similarity_search(total_retries=0)` (SDK default 3) | a failed request is reported once, never silently repeated |

Local checks: Ruff pass; pytest **110 passed** (15 new); static validator pass; `bundle validate
--strict` OK. Plan before deploy: exactly three task-setting updates
(`disable_auto_optimization: true`), 0 add / 0 delete; the sync uploaded 15 files, deleted none.
After deploy: `0 to change`; the Job shows `disable_auto_optimization: true` on every task.

## 4B - smoke run `486145196071375` - SUCCESS (17:44:43 - 17:46:55 UTC, 132.6 s)

Overridden: `allow_table_writes=false`, `allow_ai_search_mutation=false`,
`run_live_inference=true`, `eval_question_limit=1`.

| Task | Task run | Result | Output |
|---|---|---|---|
| `ingest_documents` | `580424511477683` | SUCCESS, 3 s / 73 s | dry run; `documents` v2 and `document_chunks` v2, fingerprints as Stage 2 |
| `provision_ai_search` | `650446098115250` | SUCCESS, 2 s / 24 s | readiness gate **PASS**: ready, `ONLINE_NO_PENDING_UPDATE`, 30/30 rows, endpoint ONLINE STANDARD, config matches; 1 attempt |
| `evaluate_rag` | `54661915959108` | SUCCESS, 2 s / 26 s | 1 HYBRID query, 2 chat calls |

### Question q01

> What evidence must be captured before recovering the Orion nightly batch?

Source text (`01_batch_job_recovery.md`, section *Scope*): "The on-call engineer must first record
the failed run ID, failure timestamp, and last successful task in the incident ticket."

**Retrieval (HYBRID, top 4):**

| Rank | Source | Section | Score |
|---|---|---|---|
| 1 | `01_batch_job_recovery.md` | `#scope` | 1.0000 |
| 2 | `04_pipeline_failure.md` | `#failure-classification` | 0.9541 |
| 3 | `05_monitoring_alerting.md` | `#data-protection` | 0.9262 |
| 4 | `02_incident_escalation.md` | `#required-update-format` | 0.9173 |

The expected document and the exact section holding the answer ranked first. Ranks 2-4 are
related operational sections from other runbooks that do not answer the question.

**RAG answer (verbatim):**

> Before attempting to recover the Orion nightly billing batch, the on-call engineer must capture
> the following evidence in the incident ticket:
>
> - The **failed run ID**
> - The **failure timestamp**
> - The **last successful task** that completed before the failure
>
> These items are required to be recorded first, as specified in the runbook for the Orion nightly
> batch. [1]

Citations: `[1]` -> `01_batch_job_recovery.md#scope` (the rank-1 chunk); no invalid citations.
Every fact is in the cited section; nothing from ranks 2-4 was used; nothing was invented.
("that completed before the failure" is a paraphrase of "last successful task".)

**No-retrieval baseline (verbatim):** "I'm sorry, but the available documents do not contain
enough information to answer that." - the expected behavior without context.

| | RAG | Baseline |
|---|---|---|
| finish_reason | `stop` | `stop` |
| prompt / completion / total tokens | 445 / 386 / 831 | 153 / **663** / 816 |
| reasoning tokens | unavailable (endpoint returns no `completion_tokens_details`) | unavailable |
| content blocks | `reasoning`, `text` | `reasoning`, `text` |
| answer chars | 375 | 88 |
| truncated / empty | no / no | no / no |
| latency | 3755 ms (retrieval 1802, generation 1953) | 2896 ms |
| error | none | none |
| retrieval relevance / correctness | 1.0 / 1.0 | 0.0 / 0.0 |
| citation coverage / uncited sentences | 0.25 / 1 | 1.0 / 0 |

## Findings to address before the full evaluation

1. **Token headroom.** The baseline used 663 of `max_tokens: 700` completion tokens for an 88-character
   answer - most of the budget is reasoning (inferred: reasoning tokens are not reported). The full
   run would likely truncate some calls.
2. **Refusal detection.** The baseline paraphrased the refusal ("I'm sorry, but the available
   documents do not contain enough information to answer that."). Scoring looks for the exact
   sentence, so a correct refusal on an unanswerable question would be scored wrong.
3. **Metric limits seen here.** `citation_coverage` 0.25 penalizes not citing the three irrelevant
   distractors although the one citation is exact; "unsupported claims" counts sentences without an
   inline `[n]` (the intro sentence), not ungrounded facts. Neither indicates a grounding problem.

## After the run (read-only, 17:47 UTC)

| Check | Result |
|---|---|
| Index | `ONLINE_NO_PENDING_UPDATE`, ready, **30 rows**, last processed commit still v2 |
| Pipeline `19ac364a...` | still **one** update (`5507d63a`, COMPLETED) - no synchronization |
| Endpoints | `lab12-rag-dev` only, ONLINE, 1 index |
| Tables (SQL) | 6 / 30 / 0 / 0 rows; fingerprints and latest versions (v2, v2, v0, v0) identical to Stage 2 |
| Monitoring persistence | none (writes were disabled); diagnostics are in the run log |
| `system.serving.endpoint_usage` | smoke calls not yet visible at 17:47 (latest row 16:44:34) - system-table ingestion lag |

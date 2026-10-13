# Phase E5: live baseline-versus-RAG evaluation, 18 questions (Personal DEV, 2026-10-13): PASS

**Outcome:** the same chat model (`databricks-gpt-oss-20b`, temperature 0, max 1,800 tokens) answered the 18 evaluation questions twice:
- without retrieval (baseline);
- with hybrid retrieval over the 8 fictional procedures (RAG).

The run made exactly the planned 36 chat calls, with 0 RAG errors. It stored 36 score rows and 18 redacted monitoring rows. RAG answered all 16 answerable questions with citations and refused both unanswerable ones. A manual review found no unsupported claim in the RAG answers.

These are **live model results on a small, fictional corpus**. They show grounding behaviour on 18 questions, not general model quality.

## Run

| Item | Value |
|---|---|
| Run | `712590508231976`, token `urbanflow-final-e5-full-eval-20261013-001` |
| Window | 2026-10-13 00:31:12 → 00:34:12 UTC (180 s), `SUCCESS`, no retry, run once |
| Parameters | `allow_table_writes=true`, `allow_ai_search_mutation=false` (no re-sync), `run_live_inference=true`, all 18 questions, baseline on |
| `ingest_knowledge_base` (45 s) | `PASS`, tables already matched the corpus (no commit) |
| `prepare_vector_search` (25 s) | `READY_READ_ONLY`: the existing index was ready with 30/30 rows; nothing created or synced |
| `evaluate_rag` (108 s) | Call plan 18 questions, 54 AI Search queries, 36 chat calls. Evaluation `d9e264ca-8091-4882-8ab4-e4ccd41cf763`: 36 result rows, 18 monitoring rows |

## Results (scored in the run, metric unchanged since before the run)

| Metric | Baseline (no retrieval) | RAG |
|---|---|---|
| Answer correctness (lexical keyword match, 18 questions) | 0.148 | **0.685** |
| Retrieval recall (16 answerable) | n/a | **1.000** |
| Citation precision (15 answers with parsed citations) | n/a | **0.967** |
| Source coverage (16 answerable) | n/a | **0.938** |
| Invalid citations | 0 | 0 |
| Clean refusals | 7 (2 unanswerable + 5 answerable) | 2 (exactly the 2 unanswerable) |
| Refusal decisions correct (refuse iff unanswerable) | 13 / 18 | **18 / 18** |
| Errors | 1 (q11 hit `max_tokens`, `TruncatedModelResponse`) | **0** |
| Review flags | 6 | 0 |
| Latency, median / max | 2,562 / 7,655 ms | **1,513 / 3,337 ms** |
| Tokens (prompt + completion) | 2,722 + 9,241 = 11,963 | 11,955 + 3,152 = 15,107 |

RAG prompts are four times longer (they carry the excerpts), but the answers are a third as long and more than 30% faster.

## Per question

| Id | Category | RAG correctness | Baseline correctness | Review |
|---|---|---|---|---|
| q01 | rebalancing | 0.667 | 0.0 | Supported, cited 01 (two sections). Baseline: generic delivery factors |
| q02 | rebalancing | 0.667 | 0.0 | "Inspect the station" — the procedure's exact action. Baseline: generic emergency steps |
| q03 | maintenance | 1.0 | 0.0 (refused) | Functional check, completion photo, UTC timestamp |
| q04 | maintenance | 0.333 | 0.333 | Safety hazard, or renting/returning disabled: correct; metric strict |
| q05 | incident | 0.333 | 0.0 (refused) | "Verify the source feed and ingestion status": the correct first step |
| q06 | incident | 1.0 | 0.0 (refused) | 5-minute acknowledge, incident commander, 15-minute notify |
| q07 | quality | 0.667 | 0.0 | Quarantined with the failed rule, never dropped |
| q08 | quality | 0.667 | 0.0 (refused) | First arrival to Silver, later deliveries to the duplicate audit set |
| q09 | recovery | 1.0 | 0.0 (refused) | Bounded replay steps, new execution ID, isolated checkpoint |
| q10 | recovery | 0.333 | 0.0 | "No; needs approval and a rollback; prefer a selective refresh". Baseline also said no, without the reason |
| q11 | definitions | 0.667 | 0.0 (truncated) | UUID vs short_name; join historical IDs via short_name |
| q12 | definitions | 0.333 | 0.0 | Correct (one NYC coordinate). Cited as `【1】`, which the parser does not read, so citation coverage is 0 |
| q13 | kpi | 1.0 | 0.0 | Correct. Cited the KPI page and the rebalancing procedure; only the first is expected, so precision 0.5 |
| q14 | kpi | 0.667 | 0.0 | Minutes between now and the latest source event |
| q15 | security | 0.667 | 0.333 | Azure Key Vault via an approved secret scope or SDK identity |
| q16 | security | 0.333 | 0.0 | Mutates governed resources, incurs cost, explicit approvals |
| q17 | unanswerable | 1.0 (refused) | 1.0 (refused) | Both refused |
| q18 | unanswerable | 1.0 (refused) | 1.0 (refused) | Both refused |

**Manual review (all 36 answers):**
- Every RAG claim traces to a sentence in the cited procedure: **0 unsupported claims**.
- The 11 answered baseline responses are generic industry text, not UrbanFlow procedure. Two (q10, q12) reach the right yes/no without the procedure's reason.
- The baseline refused 5 answerable questions. Without the procedures that is honest, but it does not help a dispatcher.

**Metric limitations, reported rather than tuned:**
- Correctness is a literal keyword match: paraphrases ("low‑bike threshold", "prioritize") and Unicode hyphens score 0.
- The citation parser reads `[n]` but not the `【n】` style the model used once (q12).
- Citation precision counts a second correct source as imprecise (q13).

These scores were computed in the run and are recorded unchanged. Any later scorer change will be reported as a separate, post-hoc analysis.

Cost: the endpoint existed for 25.7 minutes (≈ 1.4 DBU at the measured rate, ≈ $0.10 list). Billed usage was not in `system.billing.usage` yet when this was written; see `COST_AND_PERFORMANCE.md`.

Machine-readable record: [`2026-10-13_personal_dev_e5_rag_evaluation.json`](2026-10-13_personal_dev_e5_rag_evaluation.json), including all 18 review records.

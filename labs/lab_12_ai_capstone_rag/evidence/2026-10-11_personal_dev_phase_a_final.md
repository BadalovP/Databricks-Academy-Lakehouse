# Personal DEV - Phase A: monitoring privacy, q13 retrieval, final acceptance (2026-10-11)

Scope: fix the Stage 5C monitoring finding, improve retrieval for q13, deploy to Personal DEV and
verify with at most two GPT-OSS calls. No synchronization, no AI Search resource change, no
baseline, no full evaluation. All documents and questions are fictional training material.

**Outcome: PASS.** Persisted rows now hold only allowlisted diagnostics; q13 retrieved the
Rollback section and answered completely with correct citations, using 1 GPT-OSS call.

## A1 - Persisted-diagnostics privacy

Stage 5C persisted `diagnostics.scope.systems` (the fictional system a question names) in
`query_monitoring.evaluation_json`. A check of both tables found it in **3 monitoring rows and 3
evaluation rows**, all from evaluation `1f86e65e-...` (19:59:32 UTC): q01 `Orion`, q09 `Aurora`,
q12 `Meridian`. Nothing else in those rows comes from the question or answer.

Fix: `monitoring.safe_diagnostics` is an **allowlist** applied to `query_monitoring` and to the
diagnostics inside `evaluation_results.metrics_json`: listed numeric counts and timings, the two
booleans, `finish_reason` and content-block types from fixed sets (otherwise `other`), an
exception class name (otherwise `Error`), and `scope` reduced to `{applied, dropped}`. Any other
key - such as `scope.systems` or the new `supplemental_headings` - is never persisted. The full
diagnostics are still printed in the Job's per-question review output.

Existing rows: left unchanged, as instructed. **Recommendation: no remediation.** The values are
fictional system names that are also document titles (as sensitive as the retrieved chunk IDs
already stored), not user text. An `UPDATE` would also not remove them: earlier Delta versions keep
them until a `VACUUM` after the retention period. If remediation is ever wanted, it is one approved
`UPDATE` of those six rows followed by `VACUUM` after retention.

## A2 - q13 retrieval

Cause (Stage 5C): q13 ("resize compute or roll back?") retrieved `06#deployment-controls`,
`03#evidence-first`, `04#checkpoints-and-replay`, `03#safe-actions` - not `06#rollback`, which holds
the documented rollback criteria.

Two general changes, neither specific to q13:

1. **Section-aware supplemental retrieval** (`SectionTopics`). When a question explicitly names a
   section topic of the corpus - every word of a heading appears in the question, or the heading's
   letters appear contiguously ("roll back" -> "Rollback") - one extra search filtered to that
   section adds its best excerpt if missing. At most `retrieval.max_supplemental_queries: 2` extra
   searches per question; a failed extra search is counted and skipped. In the 18-question suite it
   triggers only for q03 (Stop conditions), q12 (Before deployment) and q13 (Rollback), never for an
   unanswerable question.
2. **Title-only chunks dropped** (`drop_preamble`). A chunk whose heading is its document's title and
   whose body is at most 200 characters (each runbook's title plus "fictional training document"
   note) is removed. These were retrieved 9 times in Stage 4D without answer content. Side effect:
   the offline fixture no longer "answers" q17 from the Atlas title chunk.

System scoping, the grounding prompt, the six documents, the chunking and the index are unchanged.
For filtered searches the offline lexical fixture now returns matches below its score threshold,
as AI Search does (the live retriever sets no threshold); unfiltered fixture behavior is unchanged.

Offline fixture (not model quality): RAG correctness 0.667 -> 0.722; unanswerable correctness
0.667 -> **1.0** (3 clean refusals); citation precision 0.879 unchanged; q13's offline context now
includes `06#rollback`.

## A3 - Validation and deployment

| Check | Result |
|---|---|
| Ruff | pass |
| pytest | **195 passed** (170 + 25 in `tests/test_phase_a_privacy_and_retrieval.py`) |
| Notebook syntax (4) / static validator / `bundle validate --strict -t personal_dev` | pass / pass / OK |
| Plan before deploy | `0 to add, 0 to change, 0 to delete, 1 unchanged` - no Job change |
| Sync | 15 files uploaded, none deleted |
| Plan after deploy | unchanged (`1 unchanged`) |

The new privacy tests serialize the persisted rows (`event_to_row`, `score_to_row`) and assert no
system name or question/answer text appears, using the real Stage 5C q01 diagnostics.

## Live q13 run `990551743620271` - SUCCESS (20:21:05 - 20:23:31 UTC, 145.5 s)

Parameters: `eval_question_ids=q13`, `eval_include_baseline=false`, `run_live_inference=true`,
`allow_table_writes=true`, `allow_ai_search_mutation=false`. Printed plan before any request:
`{"questions": 1, "ai_search_queries": 3, "rag_chat_calls": 1, "baseline_chat_calls": 0,
"chat_calls": 1}` (3 = 1 main + up to 2 section-filtered searches).

| Task | Task run | Result |
|---|---|---|
| `ingest_documents` | `343249740107763` | SUCCESS; both MERGEs skipped (tables match the corpus) |
| `provision_ai_search` | `520409010193237` | SUCCESS; read-only readiness gate PASS (30/30) |
| `evaluate_rag` | `253331563368771` | SUCCESS; 1 answer; rows written 1 + 1 |

Actual usage: **1 GPT-OSS call** (716 prompt + 684 completion = 1,400 tokens, `stop`, not
truncated), **2 AI Search queries** (main + one filtered to `Rollback`, which added 1 excerpt),
latency 5.7 s (retrieval 2.8 s, generation 2.9 s). The second permitted GPT-OSS call was not used.

Retrieved (rank: section, score): 1 `06#deployment-controls` 1.0; 2 `03#evidence-first` 0.953;
3 `04#checkpoints-and-replay` 0.946; 4 `03#safe-actions` 0.931; 5 `06#rollback` 1.0 (supplemental).

**Answer (verbatim):**

> A slow deployment is not, by itself, a trigger for rollback.
> First capture the query profile and other performance metrics ([2]) and compare the slow run to a
> known healthy run.
> Only if the query profile shows a compute bottleneck should you consider resizing the warehouse,
> and that resize must be approved by the cost owner ([4]).
> Rollback is reserved for situations where a critical validation fails, the error rate breaches
> the release threshold, or output reconciliation differs ([5]).

Manual review: **correct and complete.** [2] matches `03#evidence-first` (capture the query
profile, compare with a healthy execution); [4] matches `03#safe-actions` (no resize until the
profile shows a compute bottleneck and the cost owner approves); [5] matches `06#rollback` word for
word. The opening sentence is an uncited inference from [5] (slowness is not among the rollback
criteria); it is consistent with the source but not stated in it. No invented rule, no
cross-system attribution. Automated scores: citation precision 1.0, source coverage 1.0, weak
citations 0, invalid citations 0; correctness 0.667 is a lexical miss ("approved" vs "approval").

Combined with Stage 5C, the four Stage 4D imperfect answers (q01, q09, q12, q13) are now all
correct in their latest live run (one run each; the model is not deterministic).

## A4 - Final Personal DEV acceptance (20:24 UTC, read-only)

| Check | Result |
|---|---|
| Job run | SUCCESS |
| Index | ready, `ONLINE_NO_PENDING_UPDATE`, **30 rows**, processed source v2 |
| Synchronization | pipeline still **2** updates - none triggered |
| Endpoint | `lab12-rag-dev` ONLINE, the only endpoint, 1 index |
| `documents` / `document_chunks` | 6 / 30 rows, **v2**, Stage 2 fingerprints |
| `evaluation_results` | **41** rows: `a845c8df` (18 baseline + 18 RAG), `1f86e65e` (4), `65a81c54-6e37-4620-8a82-f0f4359bac97` (1, new) |
| `query_monitoring` | **23** rows (22 preserved + 1) |
| Privacy | new evaluation and monitoring rows: no system names, no "rollback"/question words; monitoring diagnostics contain only allowlisted keys |
| Call limits | 1 GPT-OSS call (limit 2), 2 AI Search queries (bound 3), 0 baseline calls, no retries |
| Changes outside Lab 12 | none (original worktree status unchanged) |

## Usage across Personal DEV live stages (from response usage fields)

| Stage | GPT-OSS calls | Tokens |
|---|---|---|
| 4B smoke | 2 | 1,647 |
| 4D full evaluation | 36 requested (35 answered, 1 rate-limited) | 26,861 |
| 5C targeted | 4 | 3,276 |
| Phase A q13 | 1 | 1,400 |

`system.serving.endpoint_usage` and `system.billing.usage` lag; the only billed pipeline row seen
so far is the Stage 3 initial sync (0.5676 DBU, see Stage 5B evidence). The STANDARD endpoint
`lab12-rag-dev` keeps running until cleanup is approved.

## Remaining limitations

- Live answers are single runs of a non-deterministic model.
- Automated correctness is lexical; manual review is the reference.
- System scoping assumes one system per runbook; section topics assume the corpus's headings.
- The 6 rows from Stage 5C containing fictional system names remain (no remediation recommended).

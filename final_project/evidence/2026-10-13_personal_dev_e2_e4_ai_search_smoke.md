# Phases E2–E4: AI Search endpoint, Delta Sync index and smoke test (Personal DEV, 2026-10-13): PASS

**Outcome:** one STANDARD endpoint and one TRIGGERED Delta Sync index were created for the Final Project only.
- The index held all 30 source chunks, and its configuration matched the specification.
- The two-question smoke test produced a grounded, correctly cited answer and a clean refusal, with no errors.
- The endpoint was deleted 25.7 minutes after creation ([E6](2026-10-13_personal_dev_e6_ai_search_cleanup.md)).

## Safety net before creation

AI Search bills for every hour an endpoint exists. Deletion was required within 3 hours, even if the operator's session ended. So before anything was created, a **temporary scheduled guard Job** was set up in the same workspace (script: `scripts/ai_search_cleanup_guard.py`):
- It ran every 30 minutes.
- It could delete only `urbanflow-final-dev` and its index, once the endpoint was 150 minutes old. That guarantees deletion within 3 hours.
- It could pause itself only after its own expiry, so it could not disarm before the endpoint existed.

| Guard step | Result |
|---|---|
| Created | Job `331156530436117`, expiry 1791864235987 ms (≈ 4 h) |
| Test run 1 (`599932542123256`) | Logic correct (decision `none`), but marked FAILED: serverless Python tasks run under IPython, which reports `SystemExit(0)` as a failure. Entry point fixed and re-uploaded. |
| Test run 2 (`247193340227492`) | SUCCESS: reached the AI Search API, endpoint absent, decision `none` |
| Scheduled run (`402209363218717`, ≈ 00:30 UTC) | SUCCESS: armed; the endpoint was 21 minutes old |
| Removed | After the verified cleanup, the Job and its script folder were deleted (both `ResourceDoesNotExist`) |

## Preconditions

| Check | Result |
|---|---|
| E1 | PASS and independently verified |
| Models | `databricks-gte-large-en` and `databricks-gpt-oss-20b`: `READY` |
| Collisions | No AI Search endpoint in the workspace; index `workspace.urbanflow_final_dev.rag_document_chunks_index` absent |
| STANDARD support and deletion rights | STANDARD endpoints were created and deleted in this workspace before (Lab 12). The operator is a workspace admin and the creator of the endpoint. |
| Price | `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE` $0.07/DBU list. Lab 12's endpoint measured ≈ 3.3 DBU per hour (≈ $0.23/h). An estimate, not an invoice. |

## Run `12773923439415` (create, sync, smoke test)

| Item | Value |
|---|---|
| Token | `urbanflow-final-e2-e4-smoke-20261013-001` |
| Parameters | `allow_table_writes=true`, `allow_ai_search_mutation=true`, `run_live_inference=true`, `eval_question_ids=q01,q17`, `eval_include_baseline=false` |
| Window | 2026-10-13 00:08:14 → 00:28:11 UTC, `SUCCESS`, no retry |
| `ingest_knowledge_base` (43 s) | `PASS`: both tables already matched the corpus. MERGE skipped, no commit, versions stay 1 |
| `prepare_vector_search` (1,120 s) | `READY`: endpoint created, index created; first sync started automatically |
| `evaluate_rag` (33 s) | Smoke test, below |

**Endpoint and index:**

| Item | Value |
|---|---|
| Endpoint created | **2026-10-13 00:09:15.720 UTC** (`creation_timestamp` 1791850155720), STANDARD, ONLINE |
| Index states | `pending endpoint provisioning` (to 00:22) → `pending setup of pipeline resources` → `syncing initial data` (00:25) → **ready, 30 rows** (00:27:37) |
| Verification | `ready: true`, `indexed_row_count` 30 = source 30. Endpoint ONLINE / STANDARD / 1 index. Configuration matches: `DELTA_SYNC`, primary key `chunk_id`, source `workspace.urbanflow_final_dev.rag_document_chunks`, `TRIGGERED`, embedding column `content`, model `databricks-gte-large-en` |
| Project tables | Unchanged by indexing (documents v1, chunks v1, evaluation v0, monitoring v0) |

## Smoke test (live model, `databricks-gpt-oss-20b`, temperature 0)

| | q01 "How is a station selected for bike delivery?" | q17 "What is the mayor's private mobile phone number?" (unanswerable) |
|---|---|---|
| Retrieved (hybrid, top 4) | 02 `intake-and-safety`, 07 `availability`, **01 `priority-and-dispatch`, 01 `trigger-conditions`** | 02, 01, 04 (not used) |
| Answer | Selected when available bikes are at or below the low-bike threshold, which places it in the rebalancing queue; dispatchers work by the sum of severity, deficit and size points, checking nearby stations and telemetry; both critically low → INSPECT STATION; out-of-service stations excluded. Cited `[4][3]`. | "I'm sorry, but I can't help with that." |
| Manual check | Every claim appears in `01_station_rebalancing.md`, and both citations point to the right sections | Clean refusal, no invented number |
| Scores | Recall 1.0, citation precision 1.0, coverage 1.0, invalid citations 0. **Keyword correctness 0.0** (see note) | Correctness 1.0 (clean refusal) |
| Tokens / latency | 715 prompt + 464 completion; 3.6 s | 568 + 102; 1.3 s |

**Note on the 0.0:** the correctness metric is lexical. It requires the phrases `low bikes`, `deliver bikes` and `priority`, and the model wrote "low‑bike threshold" (with a Unicode non-breaking hyphen), "bike delivery" and "prioritize". The answer is right; the metric is strict. It was left unchanged for the full evaluation, so the recorded scores were not tuned after seeing results.

**Other smoke criteria:** no fabricated citation (0 invalid); no system-prompt text in either answer; no model or API error. The monitoring rows store only a SHA-256 question hash, character counts, chunk IDs and diagnostics, with no raw question or answer. Persisted: evaluation `582a50be-ea28-452f-9b1d-be3f90564b7e`, 2 result rows, 2 monitoring rows.

Machine-readable record: [`2026-10-13_personal_dev_e2_e4_ai_search_smoke.json`](2026-10-13_personal_dev_e2_e4_ai_search_smoke.json).

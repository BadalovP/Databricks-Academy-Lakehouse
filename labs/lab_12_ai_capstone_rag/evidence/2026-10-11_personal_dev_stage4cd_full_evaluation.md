# Personal DEV - Stage 4C/4D: evaluation improvements and full live evaluation (2026-10-11)

Scope: improve scoring, then run the 18-question evaluation once - each question answered by the
same `databricks-gpt-oss-20b` model without retrieval (baseline) and with HYBRID AI Search
retrieval (RAG). Results persisted to `evaluation_results` and redacted `query_monitoring`.
No retries, no AI Search mutation, no synchronization.

## 4C - changes (deployed; Job definition unchanged)

- `generation.max_tokens` 700 -> 2000 (Stage 4B baseline used 663 of 700).
- Refusals recognised by meaning, with a `refusal` status per answer: `none`, `clean`,
  `with_extra_content` (correct for unanswerable but flagged for review), `with_specific_claims`
  (numbers, codes, hostnames, addresses alongside a refusal - scored 0 as possible fabrication).
  Errors are never refusals. Refusing an answerable question scores 0 and is flagged.
- New metrics, each `null` / `not_applicable` where it does not apply, with `<metric>_n` counts:
  retrieval recall, **citation precision** (valid citations pointing at an expected source),
  **source coverage** (expected sources actually cited), invalid citations, and the
  **uncited-sentences heuristic** (explicitly not a hallucination measure). Per-category summaries.
- Diagnostics (tokens, latency, finish reason, truncation, empty responses, errors) unchanged.
- Local: Ruff pass, pytest **133 passed**, static validator pass, `bundle validate --strict` OK,
  plan `0 to add, 0 to change, 0 to delete` (files only: 11 uploaded, none deleted).

## Preflight (17:57 UTC, read-only) - passed

Index `ONLINE_NO_PENDING_UPDATE`, ready, 30 rows, last processed commit v2; endpoint
`lab12-rag-dev` ONLINE (only endpoint); pipeline has one update; tables 6 / 30 / 0 / 0 with
Stage 2 fingerprints. The `document_chunks_index` entry in the schema listing was checked: it is
the Stage 3 index (`FOREIGN`, `VECTOR_INDEX_FORMAT`, created 16:14:52), not a new table.

## Run `410537433766515` - SUCCESS (17:57:56 - 18:01:40 UTC, 223.5 s)

Overridden: `allow_table_writes=true`, `allow_ai_search_mutation=false`,
`run_live_inference=true`, `eval_question_limit=18`.

| Task | Task run | Result | Output |
|---|---|---|---|
| `ingest_documents` | `784573595234201` | SUCCESS, 3 s / 76 s | both MERGEs **skipped** ("table already matches the snapshot"); reconciliation PASS; no new Delta version |
| `provision_ai_search` | `738242899330091` | SUCCESS, 2 s / 23 s | readiness gate PASS (ready, 30/30, endpoint ONLINE, config matches) |
| `evaluate_rag` | `1031431366356386` | SUCCESS, 2 s / 114 s | 18 HYBRID searches, 36 chat calls; rows written: `evaluation_results` 36, `query_monitoring` 18 |

## Overall results

| Metric (automated, lexical) | Baseline (no retrieval) | RAG |
|---|---|---|
| Answer correctness | 0.181 (n=18) | **0.843** (n=18) |
| Answer completeness | 0.181 (n=18) | 0.843 (n=18) |
| Retrieval recall | not applicable | **1.000** (n=15) |
| Citation precision | not applicable | **0.929** (n=14) |
| Source coverage | not applicable | 0.933 (n=15) |
| Invalid citations | 0 | 0 |
| Uncited sentences (heuristic) | not applicable | 62 |
| Refusals | clean 9, none 9 | clean 4, none 14 |
| Errors | **3** (RateLimitError, EmptyModelResponse, TruncatedModelResponse) | **0** |

| Manual review (this document) | Baseline | RAG |
|---|---|---|
| Correct | 3 (the three unanswerable refusals) | **14** |
| Partially correct | 0 | 3 (q01, q12, q13) |
| Incorrect / refused an answerable question | 12 | 1 (q09, false refusal) |
| Error, no answer | 3 | 0 |

By category (automated correctness; manual RAG verdict):

| Category | n | Baseline | RAG | RAG manual | RAG citation precision | RAG source coverage |
|---|---|---|---|---|---|---|
| answerable | 13 | 0.019 | 0.833 | 10 correct, 2 partial, 1 incorrect | 0.944 (n=12) | 0.923 |
| ambiguous | 2 | 0.000 | 0.666 | 1 correct, 1 partial | 0.834 (n=2) | 1.000 |
| unanswerable | 3 | 1.000 | 1.000 | 3 correct | n/a | n/a |

Automated correctness under-counts four correct RAG answers because it matches keywords
literally: q01 ("timestamp of the failure" vs "failure timestamp"), q04 ("incident channel" is
not part of the question), q07 and q13 ("approved" vs "approves"/"approval").

**Baseline better than RAG: on no question**, automated or manual. Ties: q09 (both refused) and
q14, q15, q17 (both refused correctly).

## Per-question comparison

Retrieved = rank:document#section (documents 01-06), score in brackets. Tokens = prompt /
completion. All RAG finish reasons were `stop`.

| Q | Category | Expected | Retrieved (rank 1-4) | RAG citations -> source | Auto RAG / Base | Manual RAG | Manual baseline |
|---|---|---|---|---|---|---|---|
| q01 | answerable | 01 | 01#scope (1.00), 04#failure-classification, 05#data-protection, 02#required-update-format | [1] 01#scope, [2] 04#failure-classification, [3] 05#data-protection | 0.667 / 0 | partial: 2 claims from other systems | refused |
| q02 | answerable | 01 | 01#scope, 01#safe-recovery, 01#stop-conditions, 06#rollback | [1] 01#scope, [2] 01#safe-recovery | 1.0 / 0 | correct | refused |
| q03 | answerable | 01 | 01#stop-conditions, 01#scope, 01#initial-triage, 01#safe-recovery | [1] 01#stop-conditions | 1.0 / 0 | correct | error: truncated |
| q04 | answerable | 02 | 02#escalation-timing, 02#required-update-format, 05#review, 02#closure | [1] 02#escalation-timing | 0.5 / 0 | correct | incorrect ("5 minutes or less") |
| q05 | answerable | 02 | 02#required-update-format, 01#scope, 02#escalation-timing, 05#review | [1] 02#required-update-format | 1.0 / 0 | correct (minor gloss) | refused |
| q06 | answerable | 03 | 03#common-causes, 03#evidence-first, 03#validation, 06#before-deployment | [2] 03#evidence-first | 1.0 / 0 | correct | generic, ungrounded |
| q07 | answerable | 03 | 03#(title), 04#schema-drift, 03#safe-actions, 05#alert-design | [3] 03#safe-actions | 0.667 / 0 | correct | incorrect ("any time") |
| q08 | answerable | 04 | 04#checkpoints-and-replay, 01#scope, 04#(title), 01#stop-conditions | [1] 04#checkpoints-and-replay | 1.0 / 0 | correct | generic, ungrounded |
| q09 | answerable | 04 | 04#verification (1.00), 04#(title), 04#checkpoints-and-replay, 02#required-update-format | none | 0 / 0 | **incorrect: false refusal** | refused |
| q10 | answerable | 05 | 05#alert-design, 05#(title), 05#ownership-and-deduplication, 05#review | [1] 05#alert-design | 1.0 / 0 | correct | error: empty |
| q11 | answerable | 05 | 05#data-protection, 05#review, 05#(title), 01#scope | [1] 05#data-protection | 1.0 / 0.25 | correct | generic, partly overlapping |
| q12 | answerable | 06 | 06#before-deployment, 06#validation, 02#closure, 06#(title) | [1] 06#before-deployment, [2] 06#validation | 1.0 / 0 | partial: one conflated item | refused |
| q13 | ambiguous | 03, 06 | 06#deployment-controls, 03#evidence-first, 04#checkpoints-and-replay, 03#safe-actions | [1] 06#deployment-controls, [2] 03#evidence-first, [4] 03#safe-actions | 0.333 / 0 | partial: rollback rule mis-cited | generic, ungrounded |
| q14 | unanswerable | - | 06#deployment-controls, 02#(title), 06#before-deployment, 02#closure | none | 1.0 / 1.0 | correct refusal | correct refusal |
| q15 | unanswerable | - | 03#(title), 01#scope, 02#closure, 01#safe-recovery | none | 1.0 / 1.0 | correct refusal | correct (safety refusal) |
| q16 | ambiguous | 04, 06 | 01#safe-recovery, 04#verification, 06#rollback, 04#schema-drift | [1] 01#safe-recovery, [2] 04#verification, [3] 06#rollback | 1.0 / 0 | correct | error: rate limited |
| q17 | unanswerable | - | 03#(title), 01#scope, 01#initial-triage, 06#validation | none | 1.0 / 1.0 | correct refusal | correct refusal |
| q18 | answerable | 05 | 01#(title), 05#data-protection, 05#review, 06#validation | [2] 05#data-protection | 1.0 / 0 | correct | refused |

Diagnostics per question:

| Q | RAG latency (retrieval + generation) | RAG tokens | Baseline latency | Baseline tokens | Baseline finish / error |
|---|---|---|---|---|---|
| q01 | 3943 ms (1231 + 2712) | 445 / 482 | 2632 ms | 153 / 663 | stop |
| q02 | 2941 ms (639 + 2302) | 444 / 701 | 1487 ms | 153 / 249 | stop |
| q03 | 1720 ms (486 + 1234) | 452 / 198 | 6636 ms | 149 / 2000 | **length - TruncatedModelResponse** (no text) |
| q04 | 1507 ms (591 + 916) | 435 / 95 | 1444 ms | 155 / 237 | stop |
| q05 | 2910 ms (1404 + 1507) | 436 / 269 | 3180 ms | 151 / 829 | stop |
| q06 | 1552 ms (308 + 1244) | 427 / 389 | 4438 ms | 150 / 1239 | stop |
| q07 | 4092 ms (2639 + 1453) | 407 / 205 | 2212 ms | 149 / 500 | stop |
| q08 | 2147 ms (539 + 1608) | 390 / 336 | 5489 ms | 149 / 1823 | stop |
| q09 | 790 ms (397 + 393) | 394 / 97 | 1225 ms | 153 / 147 | stop |
| q10 | 3619 ms (1882 + 1737) | 380 / 380 | 5002 ms | 149 / 1472 | stop - **EmptyModelResponse** (reasoning only) |
| q11 | 2114 ms (929 + 1185) | 399 / 146 | 2733 ms | 152 / 886 | stop |
| q12 | 1654 ms (266 + 1388) | 401 / 433 | 1245 ms | 151 / 204 | stop |
| q13 | 2975 ms (288 + 2687) | 431 / 631 | 4403 ms | 154 / 903 | stop |
| q14 | 1017 ms (493 + 523) | 404 / 75 | 1019 ms | 151 / 118 | stop |
| q15 | 2438 ms (1591 + 846) | 418 / 74 | 271 ms | 151 / 58 | stop |
| q16 | 1749 ms (338 + 1411) | 447 / 441 | 30 ms | unavailable | **RateLimitError** (HTTP 429, not retried) |
| q17 | 1357 ms (260 + 1096) | 411 / 167 | 447 ms | 149 / 118 | stop |
| q18 | 823 ms (427 + 396) | 406 / 95 | 1366 ms | 151 / 104 | stop |

Totals: RAG 7,527 prompt + 5,214 completion = 12,741 tokens, median 2.1 s, max 4.1 s, no errors.
Baseline 2,570 + 11,550 = 14,120 tokens over the 17 calls that reported usage; median 2.2 s,
max 6.6 s. Reasoning tokens are not reported by the endpoint (unavailable on all 36 calls).

## Manual review notes

Each RAG answer was compared with the cited section and the full corpus.

- **q01 - partially correct.** The first item (run ID, failure timestamp, last successful task
  [1]) is exact. Items 2 and 3 are faithful to their excerpts but come from the Aurora pipeline
  playbook [2] and the Beacon logging policy [3] and are presented as Orion recovery evidence -
  an attribution error, caught by citation precision 0.333. (The Stage 4B smoke answer to the
  same question, at `max_tokens: 700`, contained only the correct item; outputs are not
  deterministic.)
- **q02 - correct.** Transient infrastructure failure, retry only the failed task once. The answer
  ends with duplicated GPT-OSS citation markers (`[2]【2】, [1]【1】`), a formatting artifact.
- **q05 - correct, minor gloss.** All six fields are right; "Unknowns" is added as a seventh field
  (the source says "state unknowns explicitly"), and the one-line field descriptions are the
  model's own wording.
- **q09 - incorrect (false refusal).** The rank-1 chunk, `04#verification`, contains the answer
  (source-to-Bronze counts, Bronze-to-Silver accepted plus quarantined counts, latest event
  timestamp), yet the model answered that the documents do not contain enough information.
  Retrieval was right; generation was over-conservative. A likely cause - a hypothesis, not
  verified - is the wording gap between the question ("reconciliations") and the section
  ("Validate ... counts") combined with the strict instruction to refuse when excerpts do not
  support the answer.
- **q12 - partially correct.** Six of seven items match `06#before-deployment`. Item 3 presents
  the post-deployment validation checklist (`06#validation`) as pre-deployment "test evidence".
- **q13 - partially correct.** Evidence capture [2] and the resize condition [4] are grounded. The
  final sentence ("if the evidence does not point to a compute issue, roll back") is not stated
  in the cited `06#deployment-controls`; the rollback criteria section was not retrieved. This is
  the one unsupported claim with a misleading citation in the run.
- **q16 - correct** for an ambiguous question: reconcile counts [1] (Orion batch procedure applied
  generally), validate the three counts before resuming [2], roll back if reconciliation differs [3].
- **q14, q15, q17 - correct refusals**; no invented phone number, password or namespace.
- **No RAG answer invented a number, contact, credential, command or system.** The unsupported
  content found is cross-document attribution (q01, q12) and one inferred rule (q13).
- **Baseline:** six answerable questions refused; q04 and q07 contain invented specifics
  ("commonly 5 minutes or less"; "resize at any time ... maintenance window"); q06, q08, q11 and
  q13 are generic industry guidance unrelated to the fictional runbooks.

## Retrieval observations

- The expected document was retrieved for all 15 answerable/ambiguous questions (recall 1.0) and
  ranked first for 13. The answer-bearing section ranked first for 11; q06, q07 and q18 needed
  rank 2-3, which the model used correctly.
- HYBRID scores are compressed (0.91-1.00), so `score_threshold: 0.05` never filters; ranks 2-4
  usually include related sections from other runbooks.
- The H1 title chunks of each runbook (heading plus the "fictional training document" note) were
  retrieved 9 times and carry no answer content.

## After the run (read-only, 18:03 UTC)

| Check | Result |
|---|---|
| Index | `ONLINE_NO_PENDING_UPDATE`, ready, **30 rows**, last processed commit v2 |
| Pipeline | still **one** update (`5507d63a`, COMPLETED) - no synchronization |
| Endpoints | `lab12-rag-dev` only, ONLINE |
| `documents` / `document_chunks` | 6 / 30 rows, Stage 2 fingerprints, still **v2** (MERGE skipped) |
| `evaluation_results` | **36 rows** (18 `live-baseline` with 3 errors, 18 `live-rag` with 0), one evaluation ID, v1 `WRITE` |
| `query_monitoring` | **18 rows**, 18 distinct question hashes, diagnostics on all 18, 0 errors, latency 790-4092 ms, v1 `WRITE` |
| Redaction | 0 monitoring rows contain question or answer words; columns hold hashes, ids, counts and diagnostics only |
| `system.serving.endpoint_usage` | no rows after 16:44:34 UTC yet at 18:05 (ingestion lag); Stage 4B/4D calls not yet visible there |

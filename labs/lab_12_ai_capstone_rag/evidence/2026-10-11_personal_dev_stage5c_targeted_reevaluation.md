# Personal DEV - Stage 5C: deploy Stage 5A fixes and re-evaluate q01, q09, q12, q13 (2026-10-11)

Scope: deploy the Stage 5A grounding fixes and the targeted-run parameters to Personal DEV, then
answer exactly four questions with RAG only - at most four `databricks-gpt-oss-20b` calls, no
baseline, no retries - and append four evaluation and four monitoring rows. No synchronization,
no index or endpoint change, no Azure action, no commit. All questions and documents are
fictional training material.

**Outcome: all four answers are grounded and correctly cited. q01, q09 and q12 are now correct;
q13 no longer makes the unsupported rollback claim but is incomplete because the rollback section
was not retrieved. Exactly 4 chat calls; index and source tables unchanged.**

## Local verification and deployment

| Check | Result |
|---|---|
| Ruff / pytest | pass / **170 passed** |
| Notebook syntax (4) / static bundle validator / `bundle validate --strict -t personal_dev` | pass / pass / OK |
| Plan before deploy | `0 to add, 1 to change, 0 to delete`: only the two new Job parameters `eval_question_ids` (default `""`) and `eval_include_baseline` (default `"true"`) |
| Sync | 19 files uploaded, none deleted |
| Plan after deploy | `0 to add, 0 to change, 0 to delete, 1 unchanged` |
| Deployed Job defaults | `eval_question_ids=""`, `eval_include_baseline=true`, `allow_table_writes`/`allow_ai_search_mutation`/`run_live_inference=false`; `disable_auto_optimization: true` on all three tasks |

## Four-call limit (verified before the run)

- `select_cases` returns exactly the listed IDs; an unknown, repeated or empty ID or a limit below
  the list is an error.
- With `eval_include_baseline=false` the no-retrieval answerer is not constructed.
- `RagService.answer` makes at most one `complete()` call per question.
- No hidden retries: `DatabricksOpenAI(max_retries=0)`, `similarity_search(total_retries=0)`,
  `disable_auto_optimization: true` on every task.
- Printed by the notebook before any request: `{"questions": 4, "ai_search_queries": 4,
  "rag_chat_calls": 4, "baseline_chat_calls": 0, "chat_calls": 4}`.

## Preflight (19:55 UTC, read-only) - matched the Stage 5B end state

Endpoint `lab12-rag-dev` ONLINE (only endpoint, 1 index); index ready, `ONLINE_NO_PENDING_UPDATE`,
30 rows, processed v2; pipeline IDLE with 2 completed updates; 0 active Job runs; `documents` 6 /
`document_chunks` 30 at v2 with the Stage 2 fingerprints; `evaluation_results` 36 and
`query_monitoring` 18 at v1.

## Run `152927833894746` - SUCCESS (19:57:03 - 19:59:46 UTC, 162.8 s)

Started once with `jobs run-now` and JSON parameters, because `bundle run --params` splits values
on commas. (A first `run-now` attempt was rejected by the CLI before sending - job ID given twice -
and `list-runs` confirmed no run was created.)

Parameters: `eval_question_ids=q01,q09,q12,q13`, `eval_include_baseline=false`,
`run_live_inference=true`, `allow_table_writes=true`, `allow_ai_search_mutation=false`.

| Task | Task run | Result |
|---|---|---|
| `ingest_documents` | `525436096196074` | SUCCESS; both MERGEs **skipped** (tables match the corpus); no new version |
| `provision_ai_search` | `939573799125115` | SUCCESS; read-only readiness gate PASS (30/30); no sync |
| `evaluate_rag` | `262991950562426` | SUCCESS; 4 RAG answers; rows written `evaluation_results` 4, `query_monitoring` 4 |

## Results

| Q | Stage 4D (manual) | Stage 5C (manual) | Scope | Citations -> section | Auto correctness | Tokens (in/out) | Latency |
|---|---|---|---|---|---|---|---|
| q01 | partial: Aurora/Beacon steps attributed to Orion | **correct** | Orion; 3 other-system excerpts dropped | [1] `01#scope` | 1.0 | 409 / 136 | 2.9 s |
| q09 | **incorrect**: false refusal | **correct** | Aurora; 1 dropped | [1] `04#verification` | 0.667 * | 526 / 358 | 7.0 s |
| q12 | partial: validation checklist as pre-deployment evidence | **correct** (minor gloss) | Meridian; 1 dropped | [1] `06#before-deployment` | 1.0 | 538 / 219 | 2.8 s |
| q13 | partial: unsupported rollback rule, misleading citation | **partial**: grounded but incomplete | none named | [4] `03#safe-actions` | 0.333 | 638 / 452 | 4.0 s |

\* q09's answer contains every expected fact; the lexical score misses "source-to-Bronze" because
the model wrote it with a non-breaking hyphen.

RAG summary (4 questions): retrieval recall 1.0, **citation precision 1.0**, source coverage
0.875, invalid citations 0, **weak citations 0**, refusals none, errors 0. Generation: 4 calls,
4 x `stop`, no truncation, no empty response, 2,111 prompt + 1,165 completion = 3,276 tokens,
median latency 4.0 s.

### Answers (verbatim) and review

**q01** - "What evidence must be captured before recovering the Orion nightly batch?"

> The engineer must record the following evidence in the incident ticket before initiating
> recovery: the failed run ID, the failure timestamp, and the last successful task. [1]

Exactly the Scope section; the three excerpts from other systems were dropped before the prompt.

**q09** - "Which reconciliations are required before Aurora resumes downstream processing?"

> The Aurora pipeline must pass all three verification checks before downstream processing can
> resume:
> 1. **Source-to-Bronze counts** - confirm that the number of records ingested into Bronze
>    matches the source.
> 2. **Bronze-to-Silver accepted plus quarantined counts** - verify that the counts of records
>    moved to Silver (both accepted and quarantined) are correct.
> 3. **Latest event timestamp** - ensure the most recent event timestamp is as expected.
>
> Only after all three of these reconciliations succeed does the pipeline resume downstream
> processing. [1]

The false refusal is gone: it answered from `04#verification` despite the wording gap. The
one-line explanations are the model's paraphrase of the three checks, not new facts.

**q12** - "What evidence is required before a Meridian production deployment?"

> Before a Meridian production deployment, the following evidence must be in place:
> - A linked change record
> - Peer review of the change
> - Test evidence demonstrating the change was exercised in a test environment
> - A rollback plan
> - A named validation owner who will record acceptance
>
> Additionally, the deployer must verify the target workspace and environment immediately before
> execution, and credentials must be obtained from the approved identity provider, not from
> repository files. [1]

Every item is in `06#before-deployment`; the post-deployment validation checklist (also in
context, from `06#validation`) was **not** mixed in. Minor gloss: "exercised in a test
environment" is not stated (the source says only "test evidence").

**q13** - "A deployment is slow; should we resize compute or roll back?"

> According to the Atlas SQL Performance Troubleshooting Guide, you should not increase warehouse
> size until the query profile shows a compute bottleneck and the cost owner approves the change.
> The guide does not provide a specific recommendation for rolling back a deployment in this
> scenario. [4]

The unsupported rule is gone, and the answer states the gap instead of inventing a rule. It is
still incomplete: the corpus has rollback criteria in `06#rollback` (critical validation fails,
error-rate breach, reconciliation differs), but that section was not among the four retrieved
excerpts (`06#deployment-controls` was), and the answer did not use `03#evidence-first`
(capture the query profile first). Remaining limitation: retrieval, not grounding.

One run per question; the model is not deterministic, so this shows the fixes working, not a
guarantee for every run.

## After the run (read-only, 20:00 UTC)

| Check | Result |
|---|---|
| Endpoint | `lab12-rag-dev` ONLINE, 1 index, only endpoint |
| Index | ready, `ONLINE_NO_PENDING_UPDATE`, **30 rows**, processed **v2** |
| Pipeline | still **2** updates - no synchronization |
| `documents` / `document_chunks` | 6 / 30 rows, still **v2**, fingerprints unchanged |
| `evaluation_results` | **40** rows (36 + 4), v2 `WRITE`; new evaluation `1f86e65e-...`: 4 `live-rag` rows (q01, q09, q12, q13), 0 errors; the Stage 4D evaluation `a845c8df-...` (18 baseline + 18 RAG) preserved |
| `query_monitoring` | **22** rows (18 + 4), v2 `WRITE`; the 4 new rows: 0 errors, 4 x `stop`, latency 2,830-6,974 ms, 11 retrieved chunk IDs in total |

## Finding: system name in monitoring diagnostics

The redaction probe found question-derived words in 2 of the new monitoring rows. Checked field
by field: the only source is the new `diagnostics.scope.systems` value (`Orion`, `Aurora`;
`Meridian` in a third row) - the fictional system name a question mentions, which is one of the six
document titles. No other question or answer text is present. It is metadata comparable to the
retrieved chunk IDs, but the monitoring policy allows only hashes, IDs, counts and diagnostics.
**Proposed fix (not applied):** persist only `scope.applied` and `scope.dropped` in monitoring,
keep the system names in the run log. The four rows already written are left unchanged.

## Usage and cost

3,276 GPT-OSS tokens over 4 calls (from the response usage fields), plus 4 HYBRID query
embeddings. `system.serving.endpoint_usage` lags (its latest row was 18:01 UTC at the Stage 5B
check), so this run is not yet visible there.

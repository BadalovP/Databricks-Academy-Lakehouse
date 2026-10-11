# Personal DEV - Stage 5A: AI Dev Kit, answer-quality fixes, read-only sync readiness (2026-10-11)

Scope: install Databricks Agent Skills for Claude Code (project scope), fix the four imperfect
live answers offline, add regression tests, and verify read-only that the index can take one
manual repeat synchronization. No inference, synchronization, deployment, commit or Azure action.
All documents, questions and answers referenced here are fictional training material.

## AI Dev Kit

See [AI_DEV_KIT.md](../docs/AI_DEV_KIT.md). Summary:

- Installed: `databricks aitools install --agents claude-code --scope project --skills-only`
  (skills v0.2.10, 29 skills) in the Lab 12 worktree; 290 files under `.databricks/aitools/skills/`
  plus 29 symlinks in `.claude/skills/`; git-ignored; a before/after snapshot showed no change
  outside the worktree.
- Found: an older global Codex installation of the same skills (last updated 2026-08-30).
- **Not achieved in this session:** native skill invocation. The session had started in the
  original repository, so `Skill(databricks-core)` returned `Unknown skill` (twice). The installed
  `databricks-core` and `databricks-vector-search` skill files were read directly and followed
  for the read-only checks below. A new session in the worktree is still needed to invoke them.

## The four imperfect answers: causes and fixes

| Q | Live failure | Cause (evidence) | Fix (offline) |
|---|---|---|---|
| q09 | False refusal although rank 1 (`04#verification`) held the answer | Generation, not retrieval: the question says "reconciliations", the section says "Validate ... counts", and the prompt told the model to refuse when excerpts "do not support the answer" | Prompt: "The question may use different words than the excerpts ... answer from it even when the wording differs." System scoping also removes the unrelated rank-4 excerpt |
| q01 | Aurora and Beacon steps presented as Orion recovery evidence | Excerpt headers showed only a file anchor, not which system a runbook covers; ranks 2-3 came from other systems | Each header now reads `SOURCE: <anchor> \| DOCUMENT: <title> \| SECTION: <heading>`; prompt rule against moving one system's procedure to another; **system scoping**: when a question names Orion/Northstar/Atlas/Aurora/Beacon/Meridian, excerpts from other systems' runbooks are dropped (never all of them) |
| q12 | The post-deployment validation checklist presented as pre-deployment test evidence | Both sections of the same runbook were in context with no section label in the header | Header shows `SECTION: Before deployment` / `SECTION: Validation`; prompt rule against moving a step from one phase to another |
| q13 | "Roll back if the evidence does not point to compute" cited to `06#deployment-controls`, which does not say it | The model added its own inference and attached a citation | Prompt: cite only an excerpt that explicitly states the claim; do not add recommendations, conditions or rules no excerpt states. Evaluation: new **weak-citation heuristic** flags cited sentences that share under 25 % of their content words with every excerpt they cite |

Unchanged on purpose: the six documents, the chunking, the index, the exact refusal sentence, the
no-invention rule, refusal scoring, and the "untrusted document text" rule.

System scoping is metadata-based: the system name is the first word of each document's title,
taken at runtime from the same synced corpus that was indexed. It applies only when the question
names a system, and it falls back to all excerpts if none would remain, so it cannot create a
refusal by itself. It suits this corpus (one system per runbook); a corpus with shared runbooks
would need a different rule.

## Offline verification

| Check | Result |
|---|---|
| Ruff | pass |
| pytest | **155 passed** (133 + 22 new in `tests/test_grounding_regressions.py`) |
| Notebook syntax (4 notebooks) | pass |
| Static bundle validator | pass |
| `bundle validate --strict -t personal_dev` | OK |
| `bundle plan -t personal_dev` | `0 to add, 0 to change, 0 to delete, 1 unchanged` (nothing deployed) |

The regression tests use the live run's real rankings and answer sentences. They pin the
mechanisms: each prompt rule; headers with document and section; q01 keeps only the Orion excerpt
(3 dropped); q09 keeps `04#verification` first and drops the Northstar excerpt; questions naming
no system (q13) are untouched; scoping never empties the context; q17 (unanswerable, names Atlas)
still refuses; the weak-citation heuristic flags q13's rollback sentence and none of the correct
q01/q04/q18 citations.

Offline fixture comparison (token-overlap retriever + extractive answerer - **not** model quality):

| Offline fixture metric | Before | After |
|---|---|---|
| RAG answer correctness | 0.611 | 0.667 |
| RAG citation precision | 0.606 | 0.879 |
| RAG source coverage | 0.700 | 0.700 |
| Clean refusals | 6 | 6 |
| Unanswerable correctness | 0.667 | 0.667 (q17 fixture limitation, below) |

The offline extractive fixture already "answered" q17 by quoting the Atlas title chunk before
this change, with or without scoping; the live model refused q17 correctly. A test asserts that
scoping never reduces offline refusals.

**Not verified:** that the live model now answers q01, q09, q12 and q13 correctly. That needs an
approved live re-run of the evaluation after deployment.

## Read-only Personal DEV checks (18:50 UTC), following the installed skill files

| Check (command per `databricks-vector-search`) | Result |
|---|---|
| `vector-search-endpoints get-endpoint lab12-rag-dev` | `STANDARD`, `ONLINE`, `num_indexes: 1` |
| `vector-search-endpoints list-endpoints` | only `lab12-rag-dev` |
| `vector-search-indexes list-indexes lab12-rag-dev` | only `dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index` |
| index status (REST `GET /api/2.0/vector-search/indexes/...`) | `ready: true`, `ONLINE_NO_PENDING_UPDATE`, `indexed_row_count: 30`, last processed commit **v2** (2026-10-11T15:56:05Z), `TRIGGERED`, subtype `HYBRID` |
| `pipelines get 19ac364a-...` / `list-updates` | **IDLE**; **1** update (`5507d63a`, COMPLETED) |
| Tables (read-only SQL) | `documents` 6 rows v2, `document_chunks` 30 rows **v2** (Stage 2 fingerprint), `evaluation_results` 36 v1, `query_monitoring` 18 v1; CDF on |

The source has not changed since the version the index last processed (v2), so a repeat sync
should find no changes to embed.

## Proposed Stage 5B (not executed)

See the operator procedure in the Stage 5A report; summary: one
`databricks vector-search-indexes sync-index dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index --profile personal_lab10`
after the same read-only pre-checks, then read-only monitoring until the second pipeline update
completes, and read-only verification (2 updates, 30 rows, last processed v2, tables unchanged,
no chat-model usage).

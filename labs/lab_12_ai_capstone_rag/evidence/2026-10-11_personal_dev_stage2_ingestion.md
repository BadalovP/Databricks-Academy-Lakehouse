# Personal DEV - Stage 2: document ingestion (2026-10-11)

Scope: create the Lab 12 schema and its four tables, ingest the fictional corpus, and prove a
rerun changes nothing. AI Search mutation and live inference stayed disabled.

## Code changes deployed before the runs

- Every Job notebook calls `dbutils.notebook.exit` in its own cell, so the printed output of the
  preceding cells is kept in the run logs (Stage 1 lost the AI Search inspect report).
- Notebook 01 reports, per table: the row counts returned by the `MERGE` statement, the operation
  metrics of the Delta commit it produced, row count, key uniqueness, an order-independent SHA-256
  fingerprint of every `(key, content_hash)` pair compared with the local corpus, `ingested_at`
  coverage, and Change Data Feed on the chunk table. The task fails if any check fails. A metric
  that is not reported is recorded as `unavailable`, never as 0.
- Notebook 02's dry run reports `endpoint_status` / `index_status` (`present`, `missing`, or the
  error type, so a permission problem is not mistaken for "missing").

Local checks: Ruff pass; pytest **87 passed**; static bundle validator pass;
`bundle validate --strict -t personal_dev` OK; plan `0 to add, 0 to change, 0 to delete,
1 unchanged` before and after the redeploy; the sync uploaded 10 changed files and deleted none.

## Runs

Job `463111948320208`. Both runs overrode exactly `allow_table_writes=true`,
`allow_ai_search_mutation=false`, `run_live_inference=false`.

| | Run 1 - first load | Run 2 - idempotency |
|---|---|---|
| Run | `364638550422539` SUCCESS | `680000904141062` SUCCESS |
| Window (UTC) | 15:52:12 - 15:54:45 (153.4 s) | 15:55:20 - 15:56:51 (91.4 s) |
| `ingest_documents` | `174828697433699` SUCCESS, 3 s / 111 s | `2538852011119` SUCCESS, 2 s / 50 s |
| `provision_ai_search` | `1093961467845237` SUCCESS, 2 s / 17 s | SUCCESS (inspect only) |
| `evaluate_rag` | `398911273031411` SUCCESS, 2 s / 15 s (offline) | SUCCESS (offline) |

## Ingestion metrics

| Table | Source | Run 1 inserted / updated / deleted | Run 2 inserted / updated / deleted |
|---|---|---|---|
| `documents` | MERGE statement | 6 / 0 / 0 | 0 / 0 / 0 |
| `documents` | Delta commit | 6 / 0 / 0 (version 0 -> 1) | 0 / 0 / 0 (version 1 -> 2) |
| `document_chunks` | MERGE statement | 30 / 0 / 0 | 0 / 0 / 0 |
| `document_chunks` | Delta commit | 30 / 0 / 0 (version 0 -> 1) | 0 / 0 / 0 (version 1 -> 2) |

Run 2 wrote an empty `MERGE` commit (a new version with zero row metrics); no row was rewritten.

| Table | Rows / unique keys | Fingerprint (both runs) | Matches corpus | `max(ingested_at)` (both runs) |
|---|---|---|---|---|
| `documents` | 6 / 6 | `853e9dbb...3a7ea82e` | yes | 2026-10-11 15:53:32.13188 |
| `document_chunks` | 30 / 30 | `906f28b1...6fd0667` | yes | 2026-10-11 15:53:48.016598 |

`null_ingested_at` was 0 for both tables in both runs. `delta.enableChangeDataFeed=true` on
`document_chunks` (reported by the notebook and by the Unity Catalog tables API).

AI Search inspect (both runs): `endpoint_status: missing` (`AI Search endpoint lab12-rag-dev not
found`), `index_status: missing`.

## Workspace state after Stage 2

| Object | State |
|---|---|
| Schema `dbr_dev.parvinbadalov_lab12_rag_dev` | owner `parvinbadalov@yahoo.com` |
| `documents`, `document_chunks` | managed Delta, 6 and 30 rows |
| `evaluation_results`, `query_monitoring` | managed Delta, created empty (writes require live inference) |
| AI Search endpoints / index | none |
| Job runs | 3, all SUCCESS (Stage 1 dry run + these two) |

No model was invoked: `evaluate_rag` ran its offline branch, and the live cell (which constructs
the chat and search clients) is guarded by `run_live_inference`. The two empty tables were not
queried directly (that needs a SQL warehouse); they are empty because no code path writes to them
without live inference.

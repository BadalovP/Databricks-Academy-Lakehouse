# Phase E1: assistant knowledge ingestion (Personal DEV, 2026-10-13): PASS

**Outcome:** one bounded run of the Mobility RAG Job ingested the 8 committed procedures and their 30 chunks into Final Project-owned Delta tables.
- Every document and chunk ID, content hash and section reference equals the committed corpus.
- There are no duplicates or orphan chunks.
- Change Data Feed is on for the chunk table.
- The evaluation and monitoring tables exist and are empty.
- No AI Search resource was created or synced, and no model was called.

The procedures are **fictional, original operating documents** written for the project.

## Preconditions (read-only)

| Check | Result |
|---|---|
| Workspace / identity | `https://dbc-1750318a-76a9.cloud.databricks.com`, `parvinbadalov@yahoo.com` (workspace admin) |
| Job `887464717518046` | Deployed at `8220aaa` (RAG code unchanged since). No schedule or trigger. Task timeouts 20/60/30 min, Job 90 min. Libraries pinned (`databricks-ai-search==0.78`, `databricks-openai==0.17.1`) |
| Active runs | None on the RAG or platform Job; the RAG Job had never run |
| AI Search | No endpoint in the workspace; no index |
| RAG tables | None existed in `workspace.urbanflow_final_dev` |
| Corpus (local) | 8 documents, 30 chunks, 30 unique chunk IDs |
| Lab 12 | No Lab 12 name or object referenced by the RAG code, config or Job |

## Run

| Item | Value |
|---|---|
| Run | `870039412110159`, token `urbanflow-final-e1-rag-ingest-20261013-001` |
| Window | 2026-10-12 23:59:20 → 2026-10-13 00:01:57 UTC (157 s), `SUCCESS`, no retry |
| Parameters | `allow_table_writes=true`, `allow_ai_search_mutation=false`, `run_live_inference=false`; all others at their defaults |

| Task | Seconds | Result |
|---|---|---|
| `ingest_knowledge_base` | 115 | `PASS`. Documents: 8 inserted, 0 updated, 0 deleted, v0 → v1. Chunks: 30 inserted, v0 → v1. `(key, content_hash)` fingerprints match the corpus. No null `ingested_at`. CDF on |
| `prepare_vector_search` | 21 | `DRY_RUN`: endpoint `missing`, index `missing`. Nothing was created and no sync was triggered |
| `evaluate_rag` | 19 | Offline fixture only (`mode: offline`). Nothing was persisted. Offline fixture figures, not model quality: RAG correctness 0.648, recall 0.938; baseline correctness 0.111 |

## Independent verification (read-only SQL after the run)

| Check | Result |
|---|---|
| `rag_documents` | 8 rows; IDs and content hashes equal the committed corpus |
| `rag_document_chunks` | 30 rows; chunk IDs, document IDs, content hashes and section references (`<file>#<section>`, used for citations) equal the corpus |
| Duplicate keys / orphan chunks | 0 / 0 / 0 |
| Chunks per document | 4, 3, 3, 4, 4, 4, 4, 4 (files `01`–`08`) |
| `rag_evaluation_results` / `rag_query_monitoring` | Created, 0 rows (no fabricated results) |
| Change Data Feed | `delta.enableChangeDataFeed = true` on `rag_document_chunks` (the index source); not needed on the other three |
| Delta history | Documents and chunks: v0 `CREATE TABLE`, v1 `MERGE`. Evaluation and monitoring: v0 `CREATE TABLE` |
| Other tables | The 20 existing main-schema tables: no new commit. Lakeflow Bronze still at its step 5 commit (2026-10-12 22:55:46 UTC). Nothing outside `workspace.urbanflow_final_dev` changed |

Machine-readable record: [`2026-10-13_personal_dev_e1_rag_ingestion.json`](2026-10-13_personal_dev_e1_rag_ingestion.json).

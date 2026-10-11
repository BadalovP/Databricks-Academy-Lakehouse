# Personal DEV - Stage 1: deploy and dry run (2026-10-11)

Scope: deploy the bundle and run the Job once with every safety flag `false`. No tables, AI
Search endpoint or index, embeddings or model calls were allowed or produced.

## Deployment

| Item | Value |
|---|---|
| Workspace | `https://dbc-1750318a-76a9.cloud.databricks.com` |
| Identity | `parvinbadalov@yahoo.com` (CLI profile `personal_lab10`) |
| Bundle / target | `lab12_ai_capstone_rag` / `personal_dev` (mode development) |
| CLI | Databricks CLI v1.12.1 |
| Pre-deploy plan | `create jobs.lab12_rag_workflow` - `1 to add, 0 to change, 0 to delete` |
| Job | `463111948320208` - `[dev parvinbadalov] [personal_dev] Lab 12 - IT Operations RAG` |
| Bundle root | `/Workspace/Users/parvinbadalov@yahoo.com/.bundle/lab12_ai_capstone_rag/personal_dev` |
| Post-deploy plan | `0 to add, 0 to change, 0 to delete, 1 unchanged` |

The first deploy uploaded 48 unintended files (28 from the local `.venv`, 14 `__pycache__`, 6
`egg-info`) because the unanchored sync include `config/**` matched `.venv/.../_pytest/config/`.
The sync block was narrowed to the corpus plus explicit excludes, a static check was added, and a
redeploy removed all 48 remote files before the run. The Job definition did not change.

## Run

| Item | Value |
|---|---|
| Run | `339422254987549` - `TERMINATED` / `SUCCESS` |
| Window (UTC) | 15:41:32 - 15:43:19 (106.6 s, serverless) |
| Parameters overridden | `allow_table_writes=false`, `allow_ai_search_mutation=false`, `run_live_inference=false`; all others from the bundle |

| Task | Task run | Result | Setup / execution |
|---|---|---|---|
| `ingest_documents` | `421418019749381` | SUCCESS | 3 s / 56 s |
| `provision_ai_search` | `445953055118268` | SUCCESS | 2 s / 21 s |
| `evaluate_rag` | `677433270651616` | SUCCESS | 2 s / 19 s |

Task output (verbatim):

```text
ingest_documents     {'workspace': 'https://dbc-1750318a-76a9.cloud.databricks.com', 'target': 'dbr_dev.parvinbadalov_lab12_rag_dev', 'write_enabled': False}
                     {'status': 'DRY_RUN', 'documents': 6, 'chunks': 30}
provision_ai_search  plan: endpoint lab12-rag-dev, source dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks,
                     index dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index, TRIGGERED,
                     databricks-gte-large-en, primary key chunk_id, embedding column content
                     {"status": "DRY_RUN", "index": "dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index"}
evaluate_rag         {'workspace': 'https://dbc-1750318a-76a9.cloud.databricks.com', 'questions': 18, 'live': False, 'write_enabled': False}
                     offline baseline: relevance 0.167, correctness 0.167
                     offline rag:      relevance 0.944, correctness 0.611, citation coverage 0.454, errors 0
```

The evaluation figures are the **offline fixture** (token-overlap retriever, extractive answerer).
They say nothing about live semantic retrieval or LLM quality.

## Proven / not proven

Proven in the real workspace: the bundle deploys; serverless installs the pinned environment
(`databricks-ai-search==0.78`, `databricks-openai==0.17.1`); notebooks import the synced `src/`;
the workspace guard accepts the Personal host; the corpus is present (6 documents, 30 chunks); the
index plan derives every name from the `_dev` schema; all gates hold.

Not proven: Delta writes, AI Search creation/sync, embeddings, retrieval, generation, persistence.

## Post-run verification

- `dbr_dev.parvinbadalov_lab12_rag_dev`: does not exist (no schema, no tables).
- AI Search endpoints: none. Index `...document_chunks_index`: does not exist.
- Only Lab 12 object: Job `463111948320208`, one run.
- No model invocation: `evaluate_rag` exited in its offline branch before the live imports.

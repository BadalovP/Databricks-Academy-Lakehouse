# Phase E6: mandatory AI Search cleanup (Personal DEV, 2026-10-13): PASS

**Outcome:** the Final Project's only AI Search endpoint and its index were deleted 10 seconds after the evaluation finished, before any analysis. They existed for **25.7 minutes**, well inside the 3-hour limit. Independent checks confirm that:
- no endpoint or index remains;
- no run is active;
- the knowledge and evaluation tables are intact.

The temporary guard Job was then removed.

| Item | Value |
|---|---|
| Endpoint `urbanflow-final-dev` created | 2026-10-13 00:09:15.720 UTC |
| Evaluation finished | 00:34:26 UTC |
| Cleanup started | 00:34:40 UTC (operator script deleting only the named index and endpoint) |
| Index `workspace.urbanflow_final_dev.rag_document_chunks_index` | Delete requested; gone after one 15 s poll |
| Endpoint delete requested / gone | 00:34:58.9 / 00:34:59.1 UTC |
| Endpoint lifetime | **25 min 43 s** |

## Independent verification (fresh client, after deletion)

| Check | Result |
|---|---|
| `list_endpoints()` | `[]`: no AI Search endpoint in the workspace |
| `get_endpoint("urbanflow-final-dev")` | `NotFound` |
| `get_index(...)` | `ResourceDoesNotExist` |
| Active Job runs (any Job) | None; no index sync running |
| `rag_documents` / `rag_document_chunks` | 8 / 30 rows, intact |
| `rag_evaluation_results` | 38 rows in 2 evaluations: smoke `582a50be…` (2) and full `d9e264ca…` (18 baseline + 18 RAG) |
| `rag_query_monitoring` | 20 redacted rows |
| Lab 12 resources | None existed in this workspace at the start (`list_endpoints()` was empty); nothing of Lab 12 was touched |
| Guard Job `331156530436117` and its script folder | Deleted after verification; both `ResourceDoesNotExist` |

## Cost

| Item | Value |
|---|---|
| Endpoint-hours | 0.43 |
| Estimate at the measured rate | ≈ 3.3 DBU/h × 0.43 h ≈ 1.4 DBU × $0.07 ≈ **$0.10** (list price; an estimate) |
| Index sync, embeddings, chat tokens, Job compute | Separate SKUs; see `COST_AND_PERFORMANCE.md` |
| Measured | `system.billing.usage` had not yet received these hours when this was written; recorded later |

Machine-readable record: [`2026-10-13_personal_dev_e6_ai_search_cleanup.json`](2026-10-13_personal_dev_e6_ai_search_cleanup.json).

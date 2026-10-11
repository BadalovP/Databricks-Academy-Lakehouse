# Personal DEV - Stage 3: AI Search endpoint and initial indexing (2026-10-11)

Scope: create the Standard endpoint and the TRIGGERED Delta Sync index on the Stage 2 chunk table,
then verify with read-only checks. No chat model, no table writes, no second synchronization.

**Outcome: the endpoint is ONLINE and the index is ready with all 30 chunks.** The Job run itself
failed - its bounded wait expired before the service started the initial sync - and a platform
retry attempted a sync that the service rejected. Details below.

## Code changes deployed before the run

- `provision()` passes the SDK waits a `datetime.timedelta`. The previous code passed an int to
  `wait_until_ready`, which in `databricks-ai-search` 0.78 raises `TypeError` right after the index
  is created; the old test fake accepted an int, which hid it.
- New read-only `verify()`: polls `describe()` until `status.ready` is true **and**
  `status.indexed_row_count` equals the source row count (bounded, 30 s interval), then checks the
  endpoint (ONLINE, STANDARD) and the index configuration. Values the API omits are reported as
  `unavailable` and can never pass. On any failure the error lists what was already created;
  nothing is deleted or recreated.
- Notebook 01's dry run records each table's Delta version and fingerprint (read-only); notebook
  02 records the versions after provisioning.
- Job change: `provision_ai_search.timeout_seconds` 2400 -> 3600 (plan showed only this field).

## Job run `1073900954011225` - FAILED (16:13:06 - 16:40:46 UTC, 1660 s)

Parameters overridden: `allow_table_writes=false`, `allow_ai_search_mutation=true`,
`run_live_inference=false`.

| Task (attempt) | Task run | Result | Detail |
|---|---|---|---|
| `ingest_documents` | `938335694173246` | SUCCESS (79 s) | dry run; baseline: `documents` v2, `document_chunks` v2, both fingerprints unchanged; `evaluation_results` v0, `query_monitoring` v0 |
| `provision_ai_search` (0) | `547378965822595` | FAILED (1547 s) | created endpoint, created index, then `TimeoutError` after 1523 s: `detailed_state: PROVISIONING_ENDPOINT`, "Delta sync index creation is pending endpoint provisioning.", `indexed_row_count: unavailable` |
| `provision_ai_search` (1) | `892194807233450` | FAILED (21 s) | automatic serverless retry; took the existing-index path and called `sync()` -> `BadRequest: Vector index ... is not ready.` The sync was rejected; nothing ran |
| `evaluate_rag` | `577284161848800` | SKIPPED | upstream failed |

Why a retry happened: the tasks had no retry policy, but serverless jobs auto-optimize retries by
default. Fix prepared locally (not deployed): `disable_auto_optimization: true` on every task, plus a
validator rule; the plan shows only those three task-setting updates.

## What the service did (managed pipeline `19ac364a-ed10-477b-8e85-11bbd8990c8b`)

| UTC | Event |
|---|---|
| 16:14:50 | endpoint `lab12-rag-dev` created (ONLINE within about 2 minutes per the API) |
| 16:14:51 | index and its pipeline created by `parvinbadalov@yahoo.com` |
| 16:14:51 - 16:41:20 | no pipeline activity; index status "pending endpoint provisioning" |
| 16:41:20 | the AI Search service principal edits the pipeline and starts update `5507d63a-a9d7-46c7-8915-a87ff2c51ebc` (`API_CALL`) |
| 16:41:21 / 16:41:55 / 16:44:38 | `WAITING_FOR_RESOURCES` / `INITIALIZING` / `SETTING_UP_TABLES` |
| 16:44:50 | flow `__online_index_view` completed; update `COMPLETED` |

The pipeline has exactly one update, with no warning or error events. The delay was the service
finishing endpoint capacity after reporting ONLINE; there is no evidence of a quota, permission or
model-access problem.

## Read-only verification (17:09 UTC)

| Check | Result |
|---|---|
| Endpoints in workspace | 1: `lab12-rag-dev`, STANDARD, `ONLINE`, `num_indexes: 1` |
| Index | `dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index`, `DELTA_SYNC`, subtype `HYBRID` |
| Index status | `ready: true`, `detailed_state: ONLINE_NO_PENDING_UPDATE`, `indexed_row_count: 30`, "Index creation succeeded" |
| Last processed source commit | version 2 (2026-10-11T15:56:05Z) = current `document_chunks` version |
| Configuration | endpoint `lab12-rag-dev`; primary key `chunk_id`; source `dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks`; `TRIGGERED`; embedding column `content` with `databricks-gte-large-en` - all as specified |

Tables (SQL warehouse, read-only `SELECT` / `DESCRIBE HISTORY`):

| Table | Rows | Fingerprint | Latest commit |
|---|---|---|---|
| `documents` | 6 (6 keys) | `853e9dbb...3a7ea82e` (as Stage 2) | v2 MERGE 15:55:56 (Stage 2) |
| `document_chunks` | 30 (30 keys) | `906f28b1...6fd0667` (as Stage 2) | v2 MERGE 15:56:05 (Stage 2) |
| `evaluation_results` | 0 | - | v0 CREATE TABLE |
| `query_monitoring` | 0 | - | v0 CREATE TABLE |

No table has a commit after Stage 2; CDF is still `true` on `document_chunks`.

Model usage (`system.serving.endpoint_usage`, data present through 16:44:34 UTC, since 15:00 UTC):
`databricks-gte-large-en` 3 requests (16:14:50 - 16:44:34, index creation and initial embedding);
`databricks-gpt-oss-20b` **0 requests**.

## Account tier

Not confirmed. Read-only probes show a serverless-only workspace (classic node-types API returns
Not Found; no clusters or pools; one 2X-Small "Serverless Starter Warehouse") whose pricing tier
does not offer IP access lists. That is consistent with Free Edition and with a paid serverless
workspace below Enterprise. No accessible API reports the edition or remaining Free Edition quota.

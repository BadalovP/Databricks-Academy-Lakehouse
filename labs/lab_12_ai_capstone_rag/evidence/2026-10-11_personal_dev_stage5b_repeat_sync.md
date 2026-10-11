# Personal DEV - Stage 5B: one repeat AI Search synchronization (2026-10-11)

Scope: trigger exactly **one** manual synchronization of the existing TRIGGERED Delta Sync index,
with no source change, and verify that it completes and changes nothing. No Job run, no table
write, no index or endpoint creation, no retry, no cancellation, no chat-model call, no deployment.
The source corpus is fictional training material.

**Outcome: the request was accepted, created one pipeline update, and that update COMPLETED in
57 s. The index stayed ready with 30 rows throughout, and it still reports source commit v2 as
processed. Nothing else changed.**

## Databricks AI Dev Kit skills (native)

The session ran in the Lab 12 worktree, and `Skill(databricks-core)` and
`Skill(databricks-vector-search)` loaded natively from `.claude/skills/`. This closes the Stage 5A
gap, where both returned `Unknown skill`. They were used for:

- `databricks-core`: CLI version (v1.12.1), profile selection (`personal_lab10`, named by the
  operator), `current-user me` for identity, and `experimental aitools tools query` for read-only SQL.
- `databricks-vector-search`: `list-endpoints`, `get-endpoint`, `list-indexes`, `get-index`, and
  `sync-index` (signature confirmed with `--help` during the preflight, executed once here).

The detailed index status (`detailed_state`, `triggered_update_status`) came from
`databricks api get /api/2.0/vector-search/indexes/<name>`. In Git Bash this needs
`MSYS_NO_PATHCONV=1`, because otherwise the shell rewrites `/api/...` into a Windows path and the call
returns `Not Found`. No credential, token or `auth env` / `auth token` command was used or shown.

## Preflight (19:32:37 UTC, read-only) - passed

| Check | Result |
|---|---|
| Profile / workspace | `personal_lab10` -> `https://dbc-1750318a-76a9.cloud.databricks.com`, valid |
| Identity | `parvinbadalov@yahoo.com` (id `75952854126293`, active) |
| Endpoints | 1: `lab12-rag-dev`, STANDARD, **ONLINE**, `num_indexes: 1` |
| Index | `dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index`, the only index on the endpoint; `ready: true`, `ONLINE_NO_PENDING_UPDATE`, **30** rows |
| Index configuration | `DELTA_SYNC`, subtype `HYBRID`, **`TRIGGERED`**, embedding column `content` with `databricks-gte-large-en`, source `dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks`, pipeline `19ac364a-ed10-477b-8e85-11bbd8990c8b` |
| Last processed source commit | **v2** (2026-10-11T15:56:05Z) = current `document_chunks` version |
| Pipeline | **IDLE**; **1** update: `5507d63a-a9d7-46c7-8915-a87ff2c51ebc`, COMPLETED, `API_CALL`, not a full refresh |
| Job `463111948320208` | 0 active runs; latest run `410537433766515` (Stage 4D, SUCCESS) |
| `documents` | 6 rows / 6 keys, **v2** MERGE 15:55:56, fingerprint `853e9dbbe07008b25b5dc1dd27f676013dcfe1b08cc1b9f2fc2631353a7ea82e` (Stage 2) |
| `document_chunks` | 30 rows / 30 keys, **v2** MERGE 15:56:05, fingerprint `906f28b1a74830bd587cdf0ffd25c99e9cb36966e14857176b340b0bb6fd0667` (Stage 2) |
| `evaluation_results` / `query_monitoring` | 36 rows v1 WRITE 18:01:33 / 18 rows v1 WRITE 18:01:36 (Stage 4D) |

The fingerprint is the notebook's order-independent SHA-256 over every `(key, content_hash)` pair.
During the 5B planning preflight, the local corpus was recomputed offline with the lab code. It
produced the same two fingerprints, so the live tables still match the corpus in this worktree.

## Execution

```
databricks vector-search-indexes sync-index dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks_index --profile personal_lab10
```

Run once, at 19:33:20.639 UTC. It returned at 19:33:24.704 with exit code 0 and an empty response
body. The call only starts a sync, so it does not show that the sync completed. Completion was
established separately, as recorded below.

## Pipeline update `5d25757a-65f3-446b-aca2-5bec2408c353`

`cause: API_CALL`, `full_refresh: false`, `validate_only: false`, `mode: DEFAULT`. The pipeline
event log for this update (all events `INFO`, no `WARN` or `ERROR` in the pipeline's 34 events):

| UTC | Event |
|---|---|
| 19:33:23.743 | principal `ee7f378f-00aa-4de8-89e5-2ca63b4e55d5` edits pipeline settings |
| 19:33:24.422 | the same principal starts an update |
| 19:33:24.435 | update created (`API_CALL`) |
| 19:33:25.762 | `WAITING_FOR_RESOURCES` |
| 19:34:04.818 | `INITIALIZING` |
| 19:34:12.165 | `SETTING_UP_TABLES`; flow `__online_index_view` defined, graph loaded |
| 19:34:12.626 | `RUNNING` |
| 19:34:13.063 - 19:34:21.196 | flow `__online_index_view` STARTING -> RUNNING -> **COMPLETED** |
| 19:34:21.284 | update **COMPLETED** |

The same principal edited the settings and started the first update in Stage 3 (16:41:20). It is
the AI Search service identity that runs index pipelines, not the operator.

**Duration:** 56.8 s from update creation to COMPLETED, 60.6 s from the command to COMPLETED. The
first update (Stage 3) took about 3.5 min.

The flow-progress events report **no row metrics** for this update, and none for the first one
either. So the number of rows this update processed or embedded is **not reported**. It is not
recorded as 0.

## Status checks during the sync (read-only, 30 s interval, 20 min limit)

| UTC | Index | Pipeline | New update |
|---|---|---|---|
| 19:33:29 | ready, `ONLINE_UPDATING_PIPELINE_RESOURCES`, 30 rows, processed v2 | RUNNING | `WAITING_FOR_RESOURCES` |
| 19:34:02 | ready, `ONLINE_UPDATING_PIPELINE_RESOURCES`, 30 rows, processed v2 | RUNNING | `WAITING_FOR_RESOURCES` |
| 19:34:35 | ready, **`ONLINE_NO_PENDING_UPDATE`**, 30 rows, processed v2 | **IDLE** | **COMPLETED** |

Monitoring stopped at 19:34:35, the first check that found the update in a terminal state and the
index idle. The 20-minute limit was not reached. The index remained `ready: true` with 30 rows at
every check, so queries would not have seen a gap.

## Post-sync verification (19:35:08 UTC, read-only)

The same check script ran before and after. The **only** difference is the second pipeline update.

| Check | Result |
|---|---|
| Endpoint | `lab12-rag-dev` STANDARD **ONLINE**, 1 index; still the only endpoint |
| Index | `ready: true`, **`ONLINE_NO_PENDING_UPDATE`**, **30 / 30** rows, configuration unchanged |
| Last processed source commit | still **v2** (2026-10-11T15:56:05Z) - correct, as there is no newer commit |
| Pipeline | **IDLE**; **2** updates: `5507d63a` COMPLETED (Stage 3) and `5d25757a` COMPLETED (this sync) |
| `documents` | 6 / 6, **v2**, fingerprint unchanged |
| `document_chunks` | 30 / 30, **v2**, fingerprint unchanged |
| `evaluation_results` / `query_monitoring` | 36 rows v1 / 18 rows v1 - unchanged |
| Lab 12 Job | **no new runs** (latest still `410537433766515`), 0 active |

## Usage and cost

**Confirmed:**

- Nothing about this sync's usage can be confirmed yet. At 19:35 UTC, `system.serving.endpoint_usage`
  had rows only up to **18:01:26 UTC**, and `system.billing.usage` only up to **17:00 UTC**. Both are
  before the 19:33 sync.
- The **first** pipeline update (Stage 3) is visible in billing: **0.5676 DBU**,
  `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO`, 16:40-16:50 UTC. That is the only pipeline usage
  row so far.

**Expected, not confirmed:**

| Item | Expectation | Basis |
|---|---|---|
| Pipeline compute | a serverless charge for one update, probably below 0.5676 DBU | a real update ran on a cluster (`1011-193325-21menzn9-v2n`) for 57 s, versus about 3.5 min for the first |
| Embedding (`databricks-gte-large-en`) | 0 requests | no source commit after v2, and the index still reports v2 as processed; row counts were not reported, so this is unverified |
| Chat model (`databricks-gpt-oss-20b`) | 0 requests | nothing in this stage calls it: no Job run, no notebook, no query |
| Endpoint | the STANDARD endpoint's hourly charge, the same as if no sync had run | the endpoint runs whether or not a sync happens |

To confirm later, query `system.serving.endpoint_usage` (joined to `system.serving.served_entities`)
and `system.billing.usage` (`usage_metadata.dlt_pipeline_id = '19ac364a-...'`) from 19:33 UTC
onwards, once their latest rows pass 19:35 UTC.

## Restrictions observed

Personal DEV only. One sync command, with no retry, cancellation, recreation, Job run, deployment,
table write, source-code change, build, commit or Azure action. The only file written is this
evidence record.

## Open items

- Confirm the expected usage above once the system tables catch up.
- Stage 5C (not started): redeploy the Stage 5A code. The Job runs notebooks from the synced `src/`,
  not a wheel, so the stale `build/` and `dist/` folders do not affect the deployed Job.

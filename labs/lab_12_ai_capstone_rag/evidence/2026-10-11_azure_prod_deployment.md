# Azure PROD - deployment, provisioning and smoke test (2026-10-11)

Scope: deploy the merged Lab 12 source to the `azure_prod` target in `dbr_dev_trial_2` and run the
same staged sequence proven in Personal DEV: dry run, ingestion, AI Search provisioning, one
bounded RAG smoke test. No full evaluation, no baseline, no retries, nothing deleted. The corpus is
fictional training material.

**Outcome: PASS.** Job deployed, 6 documents / 30 chunks ingested, Standard endpoint and TRIGGERED
index created with 30/30 rows, and one grounded, correctly cited answer (1 GPT-OSS call).

## Source

- PR [#70](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/70), CI passed, squash-merged
  as `92fc13051aec7f55f24d3f0501550c72af155670` on `main` (post-merge CI passed).
- Its repository tree (`ea620c0...`) is identical to the reviewed commit `1d37603`; the deployment
  was made from a clean working tree with no diff against `92fc130`.

## Preflight (read-only)

| Check | Result |
|---|---|
| Workspace | `https://adb-7405618361895520.0.azuredatabricks.net` = `dbr_dev_trial_2`, Azure SKU `trial`, created 2026-10-06 (trial ends about 2026-10-20), region `eastus`, account pricing tier PREMIUM |
| Identity | `parvinbadalov@softserve.academy`, member of `admins`; workspace admins unrestricted (`ALLOW_ALL`); **not** an account admin |
| Unity Catalog | `dbr_dev` (managed catalog): `ALL_PRIVILEGES`, `MANAGE`; target schema `parvinbadalov_lab12_rag_prod` did not exist; no other Lab 12 schema |
| Models | `databricks-gte-large-en`, `databricks-gpt-oss-20b`: READY, my permission level `CAN_MANAGE` |
| AI Search | 0 endpoints; name `parvinbadalov-lab12-rag-prod` free; **creation permission not verifiable read-only** (confirmed by step 4) |
| Serverless Jobs | all 11 existing jobs serverless, with recent successful runs |
| Collisions | no Lab 12 job; bundle folder held only empty `azure_prod/files` and `dev/files` (from earlier read-only validate/plan) |
| Bundle | `validate --strict -t azure_prod` OK; plan `1 to add` (the Job only); parameters resolve to the PROD host, schema and endpoint, all live flags `false`, `disable_auto_optimization: true` on every task |

## Deployment

`databricks bundle deploy -t azure_prod` (Azure CLI auth) -> Job **`477888651684662`**
`[azure_prod] Lab 12 - IT Operations RAG`, files under
`/Workspace/Users/parvinbadalov@softserve.academy/.bundle/lab12_ai_capstone_rag/azure_prod`.
Post-deploy plan: `0 to add, 0 to change, 0 to delete, 1 unchanged`.

## Runs (all task attempts 0 - no retries)

| Step | Run | Parameters | Result |
|---|---|---|---|
| 2 Dry run | `36252759945549` (188 s) | all flags `false` | workspace guard PASS; 6 documents / 30 chunks loaded; no tables; endpoint and index `missing`; offline evaluation only |
| 3 Ingest | `197648627442841` (165 s) | `allow_table_writes=true` | MERGE inserted 6 / 30 (statement and Delta history agree); fingerprints `853e9dbb...` / `906f28b1...` **identical to Personal DEV**; no null `ingested_at`; CDF on |
| 4 Provision | `934213412377051` (1,637 s) | `allow_ai_search_mutation=true` | created endpoint + index; verified ready, **30** rows, endpoint ONLINE STANDARD, configuration as specified; table versions unchanged |
| 5 Smoke | `598699323723407` (151 s) | `run_live_inference=true`, `eval_question_ids=q01`, `eval_include_baseline=false`, `allow_table_writes=true` | readiness gate PASS; MERGEs skipped; 1 answer; rows written 1 + 1 |

Step 4 timeline: endpoint ONLINE about 2.5 min after the run started; index `PROVISIONING_ENDPOINT`
(capacity wait) until about 21:15, then `PROVISIONING_PIPELINE_RESOURCES`,
`PROVISIONING_INITIAL_SNAPSHOT`, ready with 30 rows at 21:18. The verification waited **1,485 s of
its 1,500 s limit** (48 checks): it passed, but with little margin - the same endpoint-capacity delay
as in Personal DEV (26 min there).

Operator note: the local helper used `jobs run-now` without `--no-wait` for step 4, so the CLI's own
20-minute wait expired while the run continued. The run was found with `list-runs` and attached to;
no second run was started, nothing was cancelled or retried. Step 5 used `--no-wait`.

## AI Search state (read-only, after step 4)

| Item | Value |
|---|---|
| Endpoint | `parvinbadalov-lab12-rag-prod`, STANDARD, ONLINE, 1 index (the only endpoint in the workspace) |
| Index | `dbr_dev.parvinbadalov_lab12_rag_prod.document_chunks_index`, `DELTA_SYNC`, `HYBRID`, `TRIGGERED`, `content` embedded with `databricks-gte-large-en`, primary key `chunk_id` |
| Status | ready, `ONLINE_NO_PENDING_UPDATE`, **30 rows**, processed source commit v1 (= current `document_chunks` version) |
| Sync pipeline | `cdc3e564-0ac6-4a66-98ee-5c6e2a26f9e9`: 1 update (`1a93c540`), COMPLETED; reported 30 synced rows, about 4.0 s embedding time per row |

## Smoke test (q01)

> The engineer must record the following evidence in the incident ticket before initiating
> recovery: the failed run ID, the failure timestamp, and the last successful task. [1]

Correct and complete; [1] = `01_batch_job_recovery.md#scope`, the only excerpt in context after system
scoping dropped 3 other-system excerpts. Printed plan before the request: 1 question, up to 3 searches,
1 RAG chat call, 0 baseline calls. Actual: **1 GPT-OSS call** (409 prompt + 136 completion = 545
tokens, `stop`), 1 AI Search query, latency 3.1 s. Citation precision 1.0, no invalid or weak
citations, no errors.

## Tables (read-only SQL, after step 5)

| Table | Rows | Latest version |
|---|---|---|
| `documents` | 6 (6 keys, no null `ingested_at`) | v1 MERGE |
| `document_chunks` | 30 (30 keys), CDF `true` | v1 MERGE |
| `evaluation_results` | 1 (`q01`, `live-rag`, no error) | v1 WRITE |
| `query_monitoring` | 1 (73 question chars, 1 retrieved chunk, 3,129 ms, no error) | v1 WRITE |

**Privacy, live:** the monitoring row's diagnostics contain only allowlisted keys, and its scope is
`{"applied": true, "dropped": 3}` - no system name, although the question names Orion. No
question-derived word appears in either table. This confirms the Phase A fix in production.

## Cost

Incurred (Azure trial workspace, Academy subscription): five short serverless Job runs (about 2-3
min each, plus about 25 min for step 4, mostly waiting); one initial index sync (30 rows); 1 GPT-OSS
call (545 tokens); 1 query embedding. Ongoing: the STANDARD endpoint, about 4 DBU per hour while it
exists. How DBUs for serverless and AI Search are charged under the `trial` SKU was not verified;
system billing tables lag. **The endpoint should be deleted after handover or before the trial ends
(about 2026-10-20) - only with explicit approval.**

## Not done (by design)

No 18-question evaluation and no repeat sync in Azure PROD (not approved); no resource deleted.

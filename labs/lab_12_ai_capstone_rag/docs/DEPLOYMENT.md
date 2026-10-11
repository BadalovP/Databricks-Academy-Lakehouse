# Deployment and approval runbook

Order is fixed: **Personal DEV → GitHub → Azure PROD.** Azure PROD deploys only the Git commit
that was executed and validated in Personal DEV. Personal DEV steps 1-4 and a one-question live smoke test were
executed on 2026-10-11 ([stage 1](../evidence/2026-10-11_personal_dev_stage1_dry_run.md),
[stage 2](../evidence/2026-10-11_personal_dev_stage2_ingestion.md),
[stage 3](../evidence/2026-10-11_personal_dev_stage3_ai_search.md),
[stage 4A/4B](../evidence/2026-10-11_personal_dev_stage4ab_rag_smoke.md),
[stage 4C/4D](../evidence/2026-10-11_personal_dev_stage4cd_full_evaluation.md)), including the full evaluation (step 6),
then [stage 5B](../evidence/2026-10-11_personal_dev_stage5b_repeat_sync.md) (one repeat sync) and
[stage 5C](../evidence/2026-10-11_personal_dev_stage5c_targeted_reevaluation.md) (targeted RAG-only re-run of four
questions: `jobs run-now` with `eval_question_ids=q01,q09,q12,q13` and `eval_include_baseline=false`, passed as
JSON because `bundle run --params` splits values on commas).

## Targets

| | `personal_dev` | `azure_prod` |
|---|---|---|
| Host | `https://dbc-1750318a-76a9.cloud.databricks.com` | `https://adb-7405618361895520.0.azuredatabricks.net` (Academy `dbr_dev_trial_2`) |
| Auth (local) | CLI profile `personal_lab10` | Azure CLI (`DATABRICKS_AUTH_TYPE=azure-cli`) |
| Bundle mode | development (Job name prefixed with the user) | production |
| Schema | `dbr_dev.parvinbadalov_lab12_rag_dev` | `dbr_dev.parvinbadalov_lab12_rag_prod` |
| AI Search endpoint | `lab12-rag-dev` | `parvinbadalov-lab12-rag-prod` |
| Index | `<schema>.document_chunks_index` | `<schema>.document_chunks_index` |
| Embedding / chat | `databricks-gte-large-en` / `databricks-gpt-oss-20b` | same |
| Bundle state | `/Workspace/Users/<user>/.bundle/lab12_ai_capstone_rag/personal_dev` | `.../azure_prod` |

Wrong-workspace protection, all verified offline or read-only:

1. No default target: `databricks bundle validate` alone fails with `please specify target`.
2. Each target pins its host; a profile for the other workspace fails with "the host in the profile
   doesn't match the host configured in the bundle".
3. The Job parameter `expected_host` defaults to `${workspace.host}`, and every notebook refuses to
   run if the workspace it executes in differs.
4. Catalog, schema, index and source table are always derived from the target, never from
   `config/default.yml`.

File sync: everything Git tracks is uploaded, plus the corpus include; `.venv/**` and
`**/__pycache__/**` are excluded. Bundle include patterns are not anchored to the bundle folder,
so a directory include such as `config/**` also matches inside `.venv` - the static validator
rejects one.

## Read-only checks (already passed on 2026-10-11)

```powershell
Set-Location labs\lab_12_ai_capstone_rag
databricks bundle validate --strict -t personal_dev --profile personal_lab10
databricks bundle plan -t personal_dev --profile personal_lab10      # 1 to add: the Job
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
databricks bundle validate --strict -t azure_prod
databricks bundle plan -t azure_prod                                 # 1 to add: the Job
```

## Personal DEV: first real execution (needs approval)

| Step | Command | Creates / costs |
|---|---|---|
| 1. Deploy - **done** | `databricks bundle deploy -t personal_dev --profile personal_lab10` | Job `463111948320208` and synced files. No compute |
| 2. Dry run - **done** | `databricks bundle run lab12_rag_workflow -t personal_dev --profile personal_lab10` | Run `339422254987549`: 107 s serverless; every flag false, nothing written |
| 3. Ingest - **done** | `... run lab12_rag_workflow ... --params allow_table_writes=true` | Runs `364638550422539` (6 / 30 inserted) and `680000904141062` (rerun: 0 / 0 / 0) |
| 4. AI Search - **done** | `... --params allow_table_writes=false,allow_ai_search_mutation=true` (run `1073900954011225`; index finished after the bounded wait, verified read-only) | Standard endpoint `lab12-rag-dev` (bills while it exists), Delta Sync index, embeddings for every chunk |
| 5. One question | Run `notebooks/03_rag_demo.py` interactively with `run_live_inference=true` | One search + one `gpt-oss-20b` call |
| 6. Evaluation | `... --params allow_table_writes=true,allow_ai_search_mutation=true,run_live_inference=true,eval_question_limit=18` | 18 searches + 36 model calls (baseline + RAG); rows in `evaluation_results` and `query_monitoring` |

Serverless jobs retry failed tasks by default; every task sets `disable_auto_optimization: true`
so a retry can never sync or create AI Search resources unasked. Endpoint capacity can lag the
endpoint's ONLINE state (26 minutes in Personal DEV), so a provisioning timeout is followed by
read-only checks, never by an automatic rerun.

Steps 3-6 can be combined into one run once step 2 succeeds. Re-running is safe: tables are
created with `IF NOT EXISTS`, ingestion is an idempotent snapshot MERGE, and an existing index is
synced rather than recreated.

**Cost note.** Serverless and pay-per-token models bill by use. The Standard AI Search endpoint is
the only resource that bills while idle; delete it after the evaluation evidence is captured if it
is not needed for review. Whether the Personal workspace is quota-limited (Free Edition) or
pay-as-you-go determines the actual charge.

## Promotion to Azure PROD - done (2026-10-11)

1. Committed the validated source, PR [#70](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/70),
   CI green, squash-merged as `92fc130` (tree identical to the reviewed commit).
2. Deployed `azure_prod` from that tree (Job `477888651684662`) and ran the staged sequence: dry run,
   ingest, provision, one bounded smoke test. Details:
   [Azure PROD evidence](../evidence/2026-10-11_azure_prod_deployment.md).
3. AI Search endpoint creation is permitted for this workspace admin (verified by creating
   `parvinbadalov-lab12-rag-prod`).

Running a targeted live check (both targets):

```powershell
databricks jobs run-now --no-wait --json '{"job_id": <JOB_ID>, "job_parameters": {"run_live_inference": "true",
  "allow_table_writes": "true", "eval_question_ids": "q01", "eval_include_baseline": "false"}}'
```

Use `jobs run-now --no-wait` with JSON parameters: `bundle run --params` splits values on commas,
and `run-now` without `--no-wait` gives up after 20 minutes while the run continues.

## Recovery

- **Provisioning timeout:** the endpoint can report ONLINE long before its capacity serves the index
  (26 min in Personal DEV, about 24 min in Azure PROD; the bounded wait is 25 min). Do not rerun with
  `allow_ai_search_mutation=true`; watch the index and its pipeline read-only until ready, then run with
  `allow_ai_search_mutation=false` - the readiness gate verifies it.
- **Retries:** every task sets `disable_auto_optimization: true`; the chat and search clients make no
  hidden retries. A failed run is inspected, never retried automatically.
- **Index out of date after an approved corpus change:** one run with `allow_table_writes=true` then one
  with `allow_ai_search_mutation=true` (it syncs the existing index), or one
  `databricks vector-search-indexes sync-index <index>`.
- **Cleanup (only with approval):** delete the index, then the endpoint
  (`databricks vector-search-endpoints delete-endpoint <name>`); the endpoint is the only resource that
  bills while idle. The Delta tables and Job can stay.

No schedule or trigger exists in the Job. `bundle destroy` and delete commands are intentionally
not part of this runbook.

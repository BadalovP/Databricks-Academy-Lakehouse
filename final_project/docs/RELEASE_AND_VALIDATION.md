# Release and live validation

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_12_COVERAGE.md) · [Cost and performance](COST_AND_PERFORMANCE.md)

**Status:** CI runs on every pull request. **Steps 1–8 have passed in Personal DEV, and step 9 (the PROD-style Azure trial deployment and sample run) passed on 2026-10-13.** On 2026-10-13 that added: the assistant (ingest, AI Search, live evaluation, endpoint deleted), the dashboard queries and the governance demo. Before that, steps 1–5 passed:
- deploy;
- platform sample run;
- strict idempotency;
- CDC inserts, updates, deletes and replay, including the checkpoint across an automatic `OPTIMIZE`;
- Lakeflow.

Earlier failed attempts are kept in the evidence table below. The shared Silver/Gold MERGEs still write one metadata-only commit per table on a replay. Steps 6–9 have not run: no AI Search endpoint, RAG inference or PROD deployment, and no CD workflow release. The CI/CD design and its comparison with Lab 08 are in [CI_CD.md](CI_CD.md).

## Continuous integration (`final_project_ci.yml`)

CI runs on pull requests and on pushes to `main` that touch `final_project/` or the two Final Project workflows. It never authenticates to a workspace.

1. `python scripts/scan_secrets.py`: no credential-shaped value in any tracked Final Project file.
2. `ruff check .` and `black --check .`
3. `pytest`: unit tests, local Spark tests (Java 17), SQL safety, notebook documentation contract, release validators, workflow structure.
4. `python scripts/build_dashboard.py --check`: the committed per-target dashboards equal what the SQL generates.
5. Install the Databricks CLI (schema only), then `python scripts/validate_bundle.py`. This validates `databricks.yml` and every included resource file against the CLI's bundle schema.
6. On pull requests, `scripts/check_change_scope.py`. It fails when the change touches anything outside `final_project/**` and the two Final Project workflows (Demo 3, labs, shared Terraform). It also reports whether the change is deployable: docs, evidence and tests alone are not.

## Continuous delivery (`final_project_cd.yml`)

CD is started manually from **Actions → Final Project · UrbanFlow 2.0 release** with three inputs:

| Input | Meaning |
|---|---|
| `target` | `personal_dev` or `azure_prod` |
| `mode` | `plan_only` (default: read-only), `deploy`, or `deploy_and_sample_run` (deploy, then one bounded platform run on the samples) |
| `confirm_release` | Must equal `RELEASE_URBANFLOW_FINAL_<TARGET>`, for example `RELEASE_URBANFLOW_FINAL_PERSONAL_DEV` |

The workflow runs in four jobs, each starting only if the previous one succeeded:

1. **Static validation** (no cloud access): the typed confirmation, a cost and scope warning for the chosen mode, the secret scan, and the CI checks again.
2. **Plan** (read-only, environment `final-project-<target>`):
   1. Pin the workspace host and refuse the shared Academy workspaces.
   2. Print the acting identity.
   3. Run `bundle validate`.
   4. Run `bundle plan`, checked by `validate_release_plan.py --phase pre`. Only the seven Final Project resources may appear, with create, update or skip actions (never delete), and names must carry the target prefix. No cluster, schedule or trigger is allowed. Job names must not collide.

   The plan and its verdict are uploaded as an artifact and written to the run summary. `plan_only` stops here.
3. **Approval:** the `final-project-approval` environment, with a required reviewer who can read the plan first.
4. **Release** (environment `final-project-<target>`):
   1. Plan again right before deploying.
   2. `bundle deploy --auto-approve --fail-on-active-runs`.
   3. Require a post-deploy plan where every action is `skip` (`--phase post`).
   4. With `deploy_and_sample_run`, start one run with an idempotency token and poll for up to 40 minutes. If the run overruns, cancel it. If the release itself is cancelled, also cancel the run. Then run `validate_job_run.py`.

   The plans, results, run JSON and validation are uploaded as release evidence even when a step fails.

### Identities the CD needs (not created yet)

| Target | Identity | GitHub configuration |
|---|---|---|
| `personal_dev` | A dedicated service principal in the personal workspace, with OAuth M2M | Environment `final-project-personal_dev`: variable `DATABRICKS_CLIENT_ID`, secret `DATABRICKS_CLIENT_SECRET`, variable `DATABRICKS_WAREHOUSE_ID` (CD exports it as `BUNDLE_VAR_warehouse_id`) |
| `azure_prod` | A service principal with a GitHub OIDC federation policy for this repository and environment (no secret) | Environment `final-project-azure_prod`: variables `DATABRICKS_CLIENT_ID`, `DATABRICKS_WAREHOUSE_ID` |

**The CD cannot be started yet.** GitHub dispatches a `workflow_dispatch` workflow only when its file is on the default branch, and the `final-project-*` environments do not exist yet. Until the PR is merged and the environments are configured, DEV is validated with the operator release below, using the owner's own login.

The personal service principal is under the project owner's control. The `azure_prod` identity needs Academy account-level administration (creating a service principal and a federation policy). That is the only Academy-admin dependency of the Final Project, and it is **not** required for a live validation: an operator can deploy `azure_prod` with their own Azure CLI login instead (below).

### Operator release without CD

Run these from `final_project/`, not from the repository root, which has a different bundle. Use the operator's own login, and save the current Job list to `/tmp/jobs.json` (`databricks jobs list --output json`) first:

```bash
cd final_project
databricks bundle validate -t personal_dev -p personal_lab10
databricks bundle plan -t personal_dev -p personal_lab10 --output json > /tmp/plan.json
python scripts/validate_release_plan.py --phase pre --target personal_dev --plan /tmp/plan.json --jobs /tmp/jobs.json
databricks bundle deploy -t personal_dev -p personal_lab10
databricks bundle plan -t personal_dev -p personal_lab10 --output json > /tmp/post.json
python scripts/validate_release_plan.py --phase post --target personal_dev --plan /tmp/post.json
```

`personal_dev` resolves its SQL warehouse by name (`Serverless Starter Warehouse`), so it needs no environment variable. `azure_prod` needs `BUNDLE_VAR_warehouse_id=<approved warehouse id>` and `az login` with access to `dbr_dev_trial_2`. Creating the `parvinbadalov_urbanflow_final_*` schemas in the shared `dbr_dev` catalog is a write to shared Academy infrastructure. Do it only with explicit approval.

## Read-only preflight (2026-10-12, Personal DEV)

Nothing was created, started or changed. Identity `parvinbadalov@yahoo.com` (workspace admin), profile `personal_lab10`, host `https://dbc-1750318a-76a9.cloud.databricks.com`, metastore `metastore_aws_us_east_2`.

| Check | Result |
|---|---|
| Schema creation in `workspace` | Possible: the catalog is owned by the workspace-admins group (the user is a member), and the user already owns schemas created there |
| Name collisions | None: no `urbanflow_final*` schema, no UrbanFlow 2.0 Job, pipeline or bundle folder |
| Serverless Jobs | Available: serverless Jobs with environments have succeeded in this workspace |
| SQL warehouse | `Serverless Starter Warehouse` (`410e5f3b4784e981`), serverless, 2X-Small, auto-stop 10 min, stopped |
| AI Search | No endpoint exists (none billing); creation was proven possible in this workspace earlier |
| Model endpoints | `databricks-gte-large-en` (embeddings) and `databricks-gpt-oss-20b` (chat): both READY |
| `bundle validate` | Passes (one warning: `evidence/**` exclude pattern matches nothing yet) |
| `bundle plan` | 7 creates, 0 updates, 0 deletes: 2 schemas, 1 Volume, 2 Jobs, 1 pipeline, 1 dashboard. `validate_release_plan.py --phase pre` PASS against the real plan |

Two deploy blockers that offline tests could not see were found and fixed:

1. **`${env.DATABRICKS_WAREHOUSE_ID}` is not a bundle interpolation.** `bundle plan` failed with `invalid dependency`. Now `personal_dev` looks the warehouse up by name and `azure_prod` takes `BUNDLE_VAR_warehouse_id`.
2. **`mode: development` renamed the schemas** to `dev_parvinbadalov_urbanflow_final_dev` and `dev_parvinbadalov_urbanflow_final_lakeflow_dev`. The Job parameters, pipeline configuration and dashboard SQL address `urbanflow_final_dev`, so every task would have failed. `personal_dev` now sets its DEV presets explicitly (development pipelines, paused triggers, `environment: dev` tag) without the renaming mode.

Hardening added at the same time: `lifecycle.prevent_destroy` on both schemas and the Volume, and a positive serverless check in the compute gate (`DATABRICKS_RUNTIME_VERSION=client.*`).

## Read-only preflight (2026-10-12, Azure trial `azure_prod`)

Nothing was created. Azure CLI authentication (no stored secret), user `parvinbadalov@softserve.academy` (workspace admins) on `dbr_dev_trial_2` (`https://adb-7405618361895520.0.azuredatabricks.net`). This is an Academy **trial** workspace, not paid production.

| Check | Result |
|---|---|
| Existing objects | No `parvinbadalov_urbanflow_final_*` schema in `dbr_dev`; no UrbanFlow 2.0 Job; no AI Search endpoint; 16 unrelated Jobs |
| Warehouse | `Serverless Starter Warehouse` `d8b8ff9942968cb8`: serverless, Small, stopped, auto-stop 10 min |
| `bundle validate -t azure_prod` | Passes (with `BUNDLE_VAR_warehouse_id=d8b8ff9942968cb8`) |
| `bundle plan -t azure_prod` | 7 creates, 0 updates, 0 deletes: the reviewed resources only. Job names carry `[azure_prod]`; no schedule, trigger or cluster. |
| `validate_release_plan.py --phase pre` | PASS; no collision with the 16 existing Jobs |

Deploying creates two schemas in the **shared** `dbr_dev` catalog. That was approved and done in step 9 on 2026-10-13, and nothing else in the catalog changed ([evidence](../evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md)).

## Next approval request (consolidated: phases D–H)

A, B and C are done (steps 4A, 4C and 5, evidence above). Nothing below has run. **Approve each phase separately.** One phase's approval never covers another, and none covers a retry after a failure. Costs use this workspace's measured usage and list prices (`COST_AND_PERFORMANCE.md`). They are list prices, not invoiced amounts.

Recommended order: **E1 → D → E2–E6 → F → G**. The assistant ingest (E1) creates the empty `rag_evaluation_results` table that the dashboard's assistant widget reads, so D can show a clean empty state before any model call.

### Phase D: dashboard redeploy, live query check and rendering (Personal DEV)

| Item | Value |
|---|---|
| Workspace | `https://dbc-1750318a-76a9.cloud.databricks.com` (Personal DEV) |
| Resources affected | Dashboard `01f1c647728e1ceeb2b5bd10a8700793` only (definition update: the review changes from 2026-10-13). Read-only SELECTs on the Final Project schemas. |
| Plan expectation | 1 update (dashboard `serialized_dashboard`), 0 creates, 0 deletes; post-plan all `skip` |
| Compute | Existing `Serverless Starter Warehouse` `410e5f3b4784e981` (2X-Small, auto-stop 10 min). No new warehouse, no resize. |
| Work | Run each of the 25 datasets once as rendered (bounded results), record row counts and sample values. Open the dashboard once to check rendering. **Publishing:** only if approved, to the owner only, with embedded credentials off. No permission change. |
| Duration | About 15 minutes of warehouse time |
| Cost exposure | About 1–2 DBU of serverless SQL, ≈ $0.70–1.40 at list |
| Verification | Every dataset runs without error. Expected live values include: maintenance 4 open tickets (`MT-1002` `IN_PROGRESS`, `MT-1004`–`1006` `OPEN`); checkpoint 4 / last data change 3 / 11 events; Lakeflow expectations from update `a26b4385`; historical `data_scope` = `40-ROW DEVELOPMENT SAMPLE`. The assistant widget is empty after E1, or "table not found" before it. |
| Cleanup | None. The warehouse auto-stops (verified afterwards). |
| Irreversible or shared effects | None. Publishing is reversible (unpublish). |

### Phase E: assistant (RAG) in Personal DEV, with mandatory endpoint deletion

| Item | Value |
|---|---|
| Workspace | Personal DEV |
| Resources created | Tables `rag_documents`, `rag_document_chunks` (CDF on), `rag_evaluation_results`, `rag_query_monitoring` in `workspace.urbanflow_final_dev`. **AI Search endpoint `urbanflow-final-dev`** (STANDARD) and a Delta Sync index on the chunks (TRIGGERED). Lab 12's endpoint is never reused; none exists now. |
| Compute and models | Serverless Job `887464717518046` (3 tasks); embedding `databricks-gte-large-en`; chat `databricks-gpt-oss-20b` (both READY) |
| Sequence (stop at the first failure) | **E1** ingest 8 documents / 30 chunks (`allow_table_writes=true`). **E2** create the endpoint and index, then sync (`allow_ai_search_mutation=true`). **E3** readiness check: index rows = 30. **E4** smoke test: `eval_question_ids=q01,q17`, `run_live_inference=true` (one answerable, one unanswerable). **E5** full paired evaluation (18 questions × baseline + RAG = 36 chat calls, up to 54 searches), only if E4 cites sources and refuses q17. **E6** delete the endpoint (and with it the index) and verify that `list_endpoints()` is empty. |
| Duration | About 1–2 endpoint-hours (provisioning and sync ≈ 25–30 min in Lab 12; evaluation minutes). Hard stop: the endpoint is deleted within 3 hours of creation, even if a step failed. |
| Cost exposure | Endpoint ≈ 3.3 DBU/hour × $0.07 = ≈ $0.23/hour (measured on Lab 12's endpoint), so ≈ $0.47 for 2 hours. Sync ≈ 0.8 DBU jobs serverless (≈ $0.28). Job runs ≈ 1–2 DBU (≈ $0.50). Chat and embedding tokens: small (36 short prompts of at most 6,000 characters of context). Total expected ≈ $1.5. If forgotten, ≈ $5.6 per day. |
| Verification | Reconciliation documents 8 / chunks 30 with stable `chunk_id`s and hashes. A repeat ingest adds nothing. Index has 30 rows. Results are stored in `rag_evaluation_results` and metrics compared baseline vs RAG (recall, correctness, citation precision, coverage, refusals, errors, latency). Individual answers are reviewed. Nothing is reported as better than measured. |
| Cleanup | E6 is mandatory: delete `urbanflow-final-dev`, confirm no AI Search endpoint remains, and record endpoint-hours from billing. The Delta tables and evidence are kept. |
| Irreversible or shared effects | None shared. Deleting the endpoint is the intended end state. |

### Phase F: governance demonstration (Personal DEV, isolated)

| Item | Value |
|---|---|
| Command | `python scripts/run_governance_demo.py --target personal_dev --execute --confirm RUN_URBANFLOW_FINAL_GOVERNANCE_DEMO --warehouse-id 410e5f3b4784e981 --profile personal_lab10 --evidence evidence/<date>_governance_demo.json --cleanup` |
| Resources | Creates only `workspace.urbanflow_final_dev_governance_demo` (copies of the dimension and 40 trips, a mapping table, three functions). It applies a row filter and three column masks there, inspects them, rolls them back, and drops that schema (`--cleanup`). No GRANT, no change to groups or users, no policy on the data schemas. The dry run was reviewed on 2026-10-13 and the schema does not exist yet. |
| Compute / duration / cost | Starter warehouse, about 10 minutes, ≈ 0.5–1 DBU (≈ $0.35–0.70) |
| Verification | Filtered vs allowed rows for the one identity (mapping row changed). Coordinates rounded and ride IDs hashed. `information_schema` shows the filter and masks during the demo and none on `urbanflow_final_dev`. |
| Limitation | One identity and no `urbanflow_admins` group exist, so multi-principal enforcement and the admin branch of the masks stay **partially unverified**. |
| Irreversible or shared effects | None. The demo schema is dropped. |

### Phase G: Azure PROD-style deployment (Academy trial `dbr_dev_trial_2`)

| Item | Value |
|---|---|
| Workspace | `https://adb-7405618361895520.0.azuredatabricks.net` (Academy **trial**, not paid production). Identity `parvinbadalov@softserve.academy` with Azure CLI login. |
| Resources created | Schemas `dbr_dev.parvinbadalov_urbanflow_final_prod` and `..._lakeflow_prod` **in the shared Academy `dbr_dev` catalog**, 1 Volume, 2 Jobs, 1 pipeline, 1 dashboard (7 creates, 0 updates, 0 deletes, recalculated 2026-10-13). No schedule, cluster or permission change. |
| Pre-checks (read-only) | **Re-run 2026-10-13 00:10 UTC, PASS:**
- the trial workspace is usable;
- `parvinbadalov@softserve.academy` is a workspace admin (Azure CLI login);
- `bundle validate -t azure_prod` passes;
- the recalculated plan is **7 creates, 0 updates, 0 deletes**, and the validator passes with no collision among 16 existing Jobs;
- the target schemas are absent among the 130 `dbr_dev` schemas (Demo 3's `parvinbadalov_urbanflow*` exist and are untouched);
- warehouse `d8b8ff9942968cb8` is serverless, Small, stopped, auto-stop 10 min;
- no AI Search endpoint exists.

Evidence: [`2026-10-13_azure_prod_phase_g_preflight.json`](../evidence/2026-10-13_azure_prod_phase_g_preflight.json). |
| Optional, approved separately | One bounded sample platform run (≈ 8 minutes, ≈ 1.3 DBU) with an independent DEV/PROD comparison |
| Cost exposure | Deploy: none. Sample run: about 1–2 DBU at Azure list prices (to be read from that workspace's `list_prices`). Storage: kilobytes. |
| Cleanup | None planned. Removing it later means dropping the two schemas in a shared catalog, which needs its own approval. |
| Irreversible or shared effects | **Writes to the shared Academy `dbr_dev` catalog** (two new schemas). Nothing else shared is touched. |

### Phase H: optional (each separately)

| Option | Effect | Prerequisite |
|---|---|---|
| Real Federation source | External write: a Final Project schema and table in a PostgreSQL database, a new connection and foreign catalog (never Lab 10's) | External database access; see the contract in `ARCHITECTURE.md` |
| Event Hubs | Publish to an Event Hub and consume with `availableNow` | A Final Project Event Hub, or approval to publish to the shared namespace; Key Vault-backed secret scope |
| Zerobus | At most 30 synthetic records into Final Project Bronze | A dedicated Final Project service principal (owner creates it) |
| Full January 2024 month | About 1.9 million trips, ~0.4 GB landed in a separate namespace; one larger run | Download and upload approval; egress to the Open-Meteo archive API |

## Optional later: full-month January 2024 validation (not executed)

The first test uses the 40-row development sample. That is enough to exercise every code path: landing, Auto Loader, quality split, Gold, the weather join (about 4 of the 40 trips fall inside the 48-hour weather sample), CDC and validation. The full month is a separate, approved run that proves scale, not logic.

1. Download `202401-citibike-tripdata.zip` locally from the official Citi Bike bucket and check its size (369,035,302 bytes, per the sample metadata). Extract only the CSV members.
2. Upload the CSVs to `/Volumes/workspace/urbanflow_final_dev/urbanflow_final_landing/landing/historical_trips_202401-full/`. This is a cloud write and needs approval. It is the sibling namespace the code expects, never inside the sample's directory.
3. Run the platform Job with:
   - `run_historical=true`, `run_full_month=true`, `historical_landing_subdir=202401-full`;
   - a new `historical_execution_id`, for example `urbanflow-final-hist-202401-full-001`;
   - `run_weather=true`, `weather_source=archive_api`, `weather_start_date=2023-12-31`, `weather_end_date=2024-01-31`, and a new `weather_execution_id`.

   The preflight refuses any other combination.
4. Pass criteria:
   - Bronze = valid + quarantine + duplicates for the full-month Bronze table.
   - Every quarantined row names a rule.
   - Weather coverage is reported, not assumed.
5. Before running, confirm that serverless egress to `archive-api.open-meteo.com` is allowed in the workspace.
6. Cost: one larger serverless run (about 1.9 million rows), plus Volume storage of roughly 0.4 GB unzipped. Measure the actual DBUs from `system.billing.usage` afterwards.

## Live validation plan

**Step 2 history:** first attempt failed (2 defects), redeploy + retry failed at weather (1 defect), redeploy + second retry **passed**. The approach recorded below was followed:
1. Redeploy the fixed code (the read-only plan shows 1 update, to the platform Job, plus synced files).
2. Repeat step 2 with `source_execution_id=urbanflow-final-304828008837365`. Landing then reports the files as already present, Auto Loader finds no new files for that execution's checkpoint, and the run continues from the existing Bronze.
3. Step 3 then reuses the same ID.


Every step below needs explicit approval before it runs. The steps are listed in the order that keeps cost lowest. After each step, record its run ID and result in the evidence table.

| # | Step | Command / parameters | Pass criteria | Cost note |
|---|---|---|---|---|
| 1 | Deploy DEV | CD or operator release, `personal_dev` | Post-deploy plan all `skip` | Free (deploy only) |
| 2 | Platform sample run | `run_station_pipeline`, `run_historical`, `run_weather`, `run_federation_cdc` = `true` | `validate_job_run.py` PASS. Bronze = Silver + Quarantine + Duplicates. CDC reconciliation all zero. Checkpoint = 1. | One bounded serverless run |
| 3 | Idempotency rerun | Same parameters plus `source_execution_id=<step 2's execution id>` (the default is `urbanflow-final-{{job.run_id}}`, which would start a new execution instead) | No new Bronze/Silver/Gold rows; landing reports the files as already present. CDC: no new versions, checkpoint unchanged. | One bounded run |
| 4 | CDC increments | `cdc_snapshot=batch_002`, then `batch_003` | Batch 002: 1 insert, 2 updates, 1 delete, history +6 events. Batch 003: no change. | Two short runs |
| 5 | Lakeflow | `run_lakeflow=true` | Update COMPLETED; expectations reported in the dashboard | One triggered update |
| 6 | Assistant ingest + index | `allow_table_writes=true`, `allow_ai_search_mutation=true` | Reconciliation PASS; index ready with 30 rows | **Starts endpoint billing** |
| 7 | Assistant evaluation | `run_live_inference=true`, `allow_table_writes=true` | RAG beats baseline on correctness and recall; unanswerable questions refused; scores stored | 36 chat calls |
| 8 | Clean up the endpoint | Delete the AI Search endpoint (approval required) | Endpoint gone; Delta tables kept | Stops billing |
| 9 | PROD | Repeat 1–2 on `azure_prod` (approval for the shared catalog) | As steps 1–2 | One bounded run |

The optional Zerobus path and the governance demo each need their own approval. Zerobus also needs a Final Project service principal.

## Evidence

| Date | Target | Step | Run / update ID | Result |
|---|---|---|---|---|
| 2026-10-12 | `personal_dev` | 1. Deploy DEV (operator release, commit `4adb9b5`) | Jobs `158911626270744` (platform), `887464717518046` (assistant); pipeline `d5e0e599-c195-4c08-8217-d554ebf2b301`; dashboard `01f1c647728e1ceeb2b5bd10a8700793`; schemas `workspace.urbanflow_final_dev`, `workspace.urbanflow_final_lakeflow_dev`; Volume `urbanflow_final_landing` | PASS: pre-plan 7 creates, post-plan all `skip`, both validators PASS, nothing started. [Evidence](../evidence/2026-10-12_personal_dev_step1_deploy.md) |
| 2026-10-12 | `personal_dev` | 2. Platform sample run | Run `304828008837365`; station execution `urbanflow-final-304828008837365` | **FAILED** after 141 s: `station_source_check` expected a fixed Demo 3 row count (40 landed correctly); `historical_trips` ran before `station_gold` built the station dimension. Only the two Bronze tables were written (40 rows each). CDC never ran. Fixed in code, not redeployed. [Evidence](../evidence/2026-10-12_personal_dev_step2_sample_run.md) |
| 2026-10-12 | `personal_dev` | Redeploy of `98cf63e` | Platform Job `158911626270744` updated | PASS: 1 update (dependency + parameter), post-plan all `skip` |
| 2026-10-12 | `personal_dev` | 2 (retry). Platform sample run | Run `896235701785511`; same station execution `urbanflow-final-304828008837365` | **FAILED at `weather_enrichment`** (sample file never landed). Six tasks passed and were verified independently: station Bronze 40 = Silver 40 + 0 + 0, fact 40, shortages = priorities = 13 matching the sample profile; historical 40 valid. **Bronze and checkpoints unchanged, no duplication.** Validation and CDC did not run. [Evidence](../evidence/2026-10-12_personal_dev_step2_retry.md) |
| 2026-10-12 | `personal_dev` | Redeploy of `2a3c88d` | Platform Job `158911626270744` updated | PASS: 1 update (3 weather parameters on `land_sources`), post-plan all `skip` |
| 2026-10-12 | `personal_dev` | **2. Platform sample run: PASS** | Run `896549012431425`; station execution `urbanflow-final-304828008837365` | **PASS.** All tasks succeeded and were verified independently. Station 40/40/40, shortages = priorities = 13. Historical 40. Weather 48 hours, 4 of 40 trips with weather (coverage 0.10). `validate_medallion` PASS. CDC `batch_001`: 5 inserts, checkpoint 1, zero mismatches in keys and values. Bronze and checkpoints unchanged; reruns changed no business value. [Evidence](../evidence/2026-10-12_personal_dev_step2_pass.md) |
| 2026-10-12 | `personal_dev` | Code-only deploy of `aab6836` (change-aware MERGE) | Platform Job unchanged | PASS: plan and post-plan all `skip` (file sync only) |
| 2026-10-12 | `personal_dev` | **3. Idempotency rerun: FAIL (1 of 19 criteria)** | Run `605056708110586`; same execution IDs as step 2 | Logical PASS (no business value changed anywhere). Audit PASS (all `ingested_at` / `retrieved_at` kept). Every data MERGE 0 inserted, updated or copied, and 0 files. No new CDF events, CDC reconciliation 0. **FAIL: CDC checkpoint 1 → 2**, because the unchanged snapshot MERGE left an empty source commit. Every MERGE still writes a metadata-only commit. [Evidence](../evidence/2026-10-12_personal_dev_step3_idempotency.md) |
| 2026-10-12 | `personal_dev` | Code-only deploy of `f869bbb` (CDC no-op guard) | Platform Job unchanged | PASS: plan and post-plan all `skip` |
| 2026-10-12 | `personal_dev` | **3 (retry). Idempotency rerun: PASS (19/19)** | Run `412521206243476` | All tasks succeeded. CDC `source_merged: false`, source version 2 unchanged, no new CDF events, history 5, Silver 5, checkpoint 2 unchanged, reconciliation 0. Every table: 0 rows changed, 0 files; audit stamps kept. Remaining: 16 shared tables wrote one metadata-only commit each. [Evidence](../evidence/2026-10-12_personal_dev_step3_retry_pass.md) |
| 2026-10-12 | `personal_dev` | 4A. Redeploy of `273a301` (preflight branches, dashboard `GRID_V1`) | Platform Job and dashboard updated | PASS: exactly 2 updates, 0 creates, 0 deletes; post-plan all `skip`; dashboard not published, no dataset SQL run. [Evidence](../evidence/2026-10-12_personal_dev_step4_redeploy.md) |
| 2026-10-12 | `personal_dev` | **4B. CDC `batch_002`: CDC PASS, stopped** | Run `343513312512955` (CDC only, 294 s) | 1 insert, 2 updates, 1 delete (6 CDF events at v3); history 5 → 11; Silver = `batch_002`, `MT-1003` deleted with its delete event kept; checkpoint 2 → 3 after reconciliation 0; the 16 other tables unchanged. **Unexpected:** an automatic `OPTIMIZE` made source version 4 (no data change). The deployed code would have moved the checkpoint to 4 on `batch_003`, so `batch_003` and Lakeflow were not started. Fixed locally (decide from CDF rows). [Evidence](../evidence/2026-10-12_personal_dev_step4_batch002.md) |
| 2026-10-12 | `personal_dev` | 4C. Code-only redeploy of `8220aaa` (CDC checkpoint = newest scanned and reconciled version) | Platform Job unchanged | PASS: plan and post-plan all `skip`; deployed notebook verified |
| 2026-10-12 | `personal_dev` | **4C. CDC `batch_003`: PASS** | Run `500336374896739` (CDC only, 220 s) | `source_merged: false`; scanned range [4, 4] (the `OPTIMIZE`) with no CDF rows, so no history or Silver MERGE; history 11; Silver 5 = `batch_003`; reconciliation 0; checkpoint **3 → 4**; `last_data_change_version` 3. Only the checkpoint table committed. [Evidence](../evidence/2026-10-12_personal_dev_step4_batch003.md) |
| 2026-10-12 | `personal_dev` | **5. Lakeflow: PASS** | Run `441981394008476`; update `a26b4385-f0b8-454a-9aad-d9a3424151c7` | Update COMPLETED in 129 s. Bronze 40, Silver 40, quarantine 0, duplicates 0, dimension 40, fact 40, summary 40, shortages 13, priorities 13. 22 expectations, 0 failed records; no event-log errors or warnings. Row-for-row parity with the imperative tables. Main schema unchanged; pipeline idle. [Evidence](../evidence/2026-10-12_personal_dev_step5_lakeflow.md) |
| 2026-10-13 | `personal_dev` | **E1. Assistant ingest: PASS** | RAG Job run `870039412110159` | 8 documents, 30 chunks; IDs, hashes and section references equal the corpus; CDF on chunks; evaluation and monitoring tables empty; no AI Search created. [Evidence](../evidence/2026-10-13_personal_dev_e1_rag_ingestion.md) |
| 2026-10-13 | `personal_dev` | **E2–E4. AI Search + smoke test: PASS** | Run `12773923439415`; endpoint created 00:09:15.720 UTC | A temporary cleanup guard Job (deletes at 150 min, tested) armed before creation. Index ready 30/30 at 00:27:37. q01 grounded and cited (keyword metric 0.0, a lexical-metric artefact); q17 refused. No raw text in monitoring. [Evidence](../evidence/2026-10-13_personal_dev_e2_e4_ai_search_smoke.md) |
| 2026-10-13 | `personal_dev` | **E5. Live evaluation: PASS** | Run `712590508231976`, evaluation `d9e264ca…` | 36 chat calls. RAG correctness 0.685 vs baseline 0.148; recall 1.0; citation precision 0.967; coverage 0.938; refusal decisions 18/18 vs 13/18; 0 RAG errors vs 1 truncated baseline answer; median 1.5 s vs 2.6 s. Manual review: 0 unsupported RAG claims. [Evidence](../evidence/2026-10-13_personal_dev_e5_rag_evaluation.md) |
| 2026-10-13 | `personal_dev` | **E6. Endpoint cleanup: PASS** | — | Index and endpoint deleted at 00:34:59 UTC (lifetime 25.7 min); `list_endpoints()` empty; tables intact; guard Job deleted. [Evidence](../evidence/2026-10-13_personal_dev_e6_ai_search_cleanup.md) |
| 2026-10-13 | `personal_dev` | **D. Dashboard: PASS, one rendering defect** | Deploy of `d93f2f4`; dashboard `01f1c647…` | 1 update, post-plan all `skip`; 25/25 datasets ran (40 s), 0 widget-field mismatches; expected values present. Defect: page titles render merged (Lakeview concatenates text lines); fixed in code, redeploy needs approval; not published. [Evidence](../evidence/2026-10-13_personal_dev_dashboard_validation.md) |
| 2026-10-13 | `personal_dev` | **F. Governance demo: PASS** | Isolated schema, 49 s | Filter: 0 → 28 → 40 rows by mapping; coordinates rounded; ride IDs hashed; policy metadata observed; rolled back; schema dropped; no GRANT; 0 masks/filters in the catalog afterwards. One identity only. [Evidence](../evidence/2026-10-13_personal_dev_governance_demo.md) |
| 2026-10-13 | `personal_dev` | **Dashboard title fix: PASS** | Deploy of `625916f`; dashboard `01f1c647…` | 1 update, post-plan all `skip`; the five titles render heading + paragraph; published revision 08:14:28 UTC, embedded credentials off, owner-only access. [Evidence](../evidence/2026-10-13_personal_dev_dashboard_title_fix.md) |
| 2026-10-13 | `azure_prod` (Academy trial) | **9. Deploy: PASS** | Jobs `456376825048239` (platform), `1075074961516662` (RAG); pipeline `f5961757-69be-4800-8afe-dae6987d8bc0`; dashboard `01f1c6ddd47616e8bb703a5342a603ba`; schemas `dbr_dev.parvinbadalov_urbanflow_final_prod`, `..._final_lakeflow_prod`; Volume `urbanflow_final_landing` | Plan 7 creates / 0 updates / 0 deletes, validator PASS; post-plan all `skip`; `dbr_dev` gained only the two schemas. [Evidence](../evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md) |
| 2026-10-13 | `azure_prod` (Academy trial) | **9. Sample run: PASS** | Run `240527908342651` (473 s) | Every task succeeded; Lakeflow excluded. Independently verified: station 40/40/40, facts 40, shortages = priorities = 13, trips 40, weather 48 hours with 4 matched trips, quarantine and duplicates 0, reconciliation identities true; CDC 5 inserts, Silver 5, history 5, reconciliation 0, checkpoint −1 → 1. Identical to DEV. [Evidence](../evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md) |
| — | — | GitHub CD workflow run, Zerobus, Event Hubs, full month, external Federation | — | No live run yet (optional, or waiting for identities) |

When a step passes, record it here. Add the notebook to `VALIDATED_LIVE_CLAIMS` in `tests/test_notebook_documentation.py` and update its **Actual validation** line with the run ID. Update the coverage matrix status to **Validated live**.

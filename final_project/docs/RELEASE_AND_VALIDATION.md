# Release and live validation

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_12_COVERAGE.md) · [Cost and performance](COST_AND_PERFORMANCE.md)

**Status:** CI runs on every pull request. **Steps 1, 2 and 3 have passed in Personal DEV.** Step 3 (strict idempotency) failed one criterion in run `605056708110586`. After the CDC no-op fix, it passed all 19 criteria in run `412521206243476`: logical, audit and physical-data idempotency, with no CDC commit and the checkpoint unchanged. The shared Silver/Gold MERGEs still write one metadata-only commit per table on a replay. Steps 4–9 have not run. There has been no Lakeflow update, AI Search endpoint, RAG inference or CD release.

## Continuous integration (`final_project_ci.yml`)

CI runs on pull requests and on pushes to `main` that touch `final_project/` or the two Final Project workflows. It never authenticates to a workspace.

1. `ruff check .` and `black --check .`
2. `pytest`: unit tests, local Spark tests (Java 17), SQL safety, notebook documentation contract, release validators.
3. `python scripts/build_dashboard.py --check`: the committed per-target dashboards equal what the SQL generates.
4. Install the Databricks CLI (schema only), then `python scripts/validate_bundle.py`. This validates `databricks.yml` and every included resource file against the CLI's bundle schema.

## Continuous delivery (`final_project_cd.yml`)

CD is started manually from **Actions → Final Project · UrbanFlow 2.0 release** with three inputs:

| Input | Meaning |
|---|---|
| `target` | `personal_dev` or `azure_prod` |
| `confirm_release` | Must equal `RELEASE_URBANFLOW_FINAL_<TARGET>`, for example `RELEASE_URBANFLOW_FINAL_PERSONAL_DEV` |
| `run_platform` | `false` by default. When `true`, after the deploy CD runs the platform Job once on the samples (station, historical, weather and CDC in fixture mode). |

The workflow runs in three stages:

1. **Static validation:** the CI checks again.
2. **Approval:** the `final-project-approval` environment, with a required reviewer.
3. **Release:** in the `final-project-<target>` environment:
   1. Pin the workspace host and refuse the shared Academy workspaces.
   2. Print the deploying identity.
   3. Run `bundle validate`, then `bundle plan` checked by `validate_release_plan.py --phase pre`. Only the seven Final Project resources may appear, with create, update or skip actions only (never delete), and names must carry the target prefix. No cluster, schedule or trigger is allowed.
   4. `bundle deploy --auto-approve --fail-on-active-runs`.
   5. Run `bundle plan` again and require every action to be `skip` (`--phase post`). This proves the deploy converged.
   6. If `run_platform=true`, start the run with an idempotency token, poll for up to 40 minutes, then run `validate_job_run.py`. It requires every task to succeed (`refresh_lakeflow` may be excluded) and the final validation to report PASS.

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

Deploying would create two schemas in the **shared** `dbr_dev` catalog, so it needs explicit approval (step 9).

## Next approval request (consolidated, bounded, DEV first)

Each item lists its exact action, bound, verification and cleanup. Approve items individually.

**Status (2026-10-12):**
- A: done (PASS).
- B: `batch_002` done. CDC passed, but an automatic `OPTIMIZE` added source version 4, so the run stopped before `batch_003`.
- C: not run.
- D–G: not approved.

**Revised B2 (needs approval):**
1. Redeploy the CDF-row decision fix. Code sync only: the plan should be all `skip`, with `11_federation_cdc.py` and `cdc_sql.py` synced.
2. Run `batch_003`. Expected: `source_merged: false`, no new source commit (the source stays at 4 unless Databricks optimizes again), no CDF rows in the scanned range, history 11, checkpoint stays 3 with its earlier `processed_at`, reconciliation 0.
3. Then run C.

**A. Redeploy DEV (code only).**
- The read-only plan shows 2 updates, no create or delete:
  - the platform Job gains `run_federation_cdc` and `run_lakeflow` on the preflight task, so CDC-only and Lakeflow-only runs are possible;
  - the dashboard gets the 12-column `GRID_V1` layout.
- The dashboard update is included because a bundle deploys everything together.
- Cost: none (no compute).

**B. Step 4: CDC update and delete batches.** Two runs of Job `158911626270744` with only `run_federation_cdc=true` (every other branch false):
1. `cdc_snapshot=batch_002`. Expected:
   - snapshot differs, source MERGE runs, source v2 → v3;
   - CDF v3 has 6 events: 1 insert (`MT-1006`), 2 update pre-images and 2 post-images (`MT-1001` → `CLOSED`, `MT-1002`), 1 delete (`MT-1003`);
   - history 5 → 11 events with 11 distinct IDs;
   - Silver 5 rows (`MT-1001`, `MT-1002`, `MT-1004`, `MT-1005`, `MT-1006`) equal to `batch_002`;
   - reconciliation 0; checkpoint 2 → 3.
2. `cdc_snapshot=batch_003` (same business state as `batch_002`). Expected: `source_merged: false`, no new source version, no CDF events, history 11, checkpoint stays 3.

Bound: 40 minutes per run, no retry; about 2–4 minutes each at measured task times. Verification: independent SQL as in step 3. Cleanup: none.

**C. Step 5: one Lakeflow update.** One run with only `run_lakeflow=true` (the Job triggers `d5e0e599-…`, a triggered serverless update in development mode).
- Expected: update COMPLETED; pipeline Bronze 40, Silver 40, quarantine 0, duplicates 0, dimension 40, fact 40, summary 40, shortage 13, priority 13 (the imperative results); every expectation passes.
- Bound: task timeout 40 minutes.

**D. Dashboard: read-only live query check.**
- Run each of the 25 dashboard datasets once on `Serverless Starter Warehouse` (2X-Small, auto-stop 10 min), as the published SQL with `LIMIT 1000`.
- Pass criteria: every dataset runs without error, and the CDC / assistant datasets return the expected empty-state or live rows.
- Then open the dashboard once to check rendering.

**E. Steps 6–8: assistant**, in order, stopping at the first failure:
1. ingest the 8 documents / 30 chunks;
2. create one STANDARD AI Search endpoint `urbanflow-final-dev` and the Delta Sync index;
3. sync and check readiness (30 rows);
4. a 2-question smoke test (`eval_question_ids=q01,q17`, baseline off);
5. the full 18-question paired evaluation (36 chat calls, up to 54 searches), only if the smoke test answers with citations;
6. **delete the endpoint** and verify with `list_endpoints()` that none remains.

Exposure: the endpoint bills per hour while it exists. The planned window is about 1–2 endpoint-hours (Lab 12 provisioning took about 25–30 minutes). An earlier review cited about 4 DBU/hour, roughly $0.28/hour, for a Standard unit (a US-East public reference example). That figure is **unverified for this account**; read `system.billing.list_prices` before creation. Lab 12's objects are never reused.

**F. Governance demo.** `scripts/run_governance_demo.py --target personal_dev --execute --confirm RUN_URBANFLOW_FINAL_GOVERNANCE_DEMO --warehouse-id 410e5f3b4784e981 --profile personal_lab10 --evidence evidence/<date>_governance_demo.json --cleanup`.
- It creates only `workspace.urbanflow_final_dev_governance_demo`, applies and rolls back the row filter and masks, and drops that schema.
- One identity only, so it demonstrates mechanism, not multi-principal enforcement. No GRANT.

**G. Step 9: PROD.** Deploy to `azure_prod`, which creates the schemas in the shared `dbr_dev` catalog, and run step 2 there.

**Optional (each separate):** full month; Zerobus (needs a Final Project service principal); a real Federation source (needs a Final-Project table in an external database).

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
| — | — | 4 (`batch_003`) – 9 | — | No live run yet (each step needs its own approval) |

When a step passes, record it here. Add the notebook to `VALIDATED_LIVE_CLAIMS` in `tests/test_notebook_documentation.py` and update its **Actual validation** line with the run ID. Update the coverage matrix status to **Validated live**.

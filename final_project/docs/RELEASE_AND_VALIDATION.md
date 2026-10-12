# Release and live validation

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_12_COVERAGE.md) · [Cost and performance](COST_AND_PERFORMANCE.md)

**Status:** CI runs on every pull request. **No CD release and no live run has happened yet.** This page describes how to release and how to validate, and it is where the evidence of each approved run will be recorded.

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
| — | — | — | — | No live run yet |

When a step passes, record it here. Add the notebook to `VALIDATED_LIVE_CLAIMS` in `tests/test_notebook_documentation.py` and update its **Actual validation** line with the run ID. Update the coverage matrix status to **Validated live**.

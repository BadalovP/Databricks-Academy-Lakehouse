# The remaining Pending live requirements, and how to clear them

Eight rows remain `Pending live` after this round (two of the previous ten moved to
`Implemented locally`: the 1,000-file generator and the Great Expectations suite). Each is analysed
below, then grouped into the smallest number of approval batches.

## Two that just moved

| Requirement | What changed |
|---|---|
| Lab 3 - Approximately 1,000 files | `scripts/generate_many_small_files.py` generates 1,000 tiny deterministic CSVs (under 1.5 MB total), with cleanup and 14 tests. Local only; the upload is a separate approval |
| Lab 7 - Great Expectations or Soda | GE 1.23.2 chosen by measurement (Soda requires pyspark below 4 and resolves 3.5.9 in this environment), suites in `expectations.py`, 11 tests. See [QUALITY_FRAMEWORK.md](QUALITY_FRAMEWORK.md) |

## The eight remaining

### 1. Lab 2 - Legacy mounts exercise

- **Requirement:** demonstrate the deprecated DBFS mount pattern and explain why Unity Catalog
  Volumes replaced it.
- **Current:** the architecture document explains the security boundary; no mount exists.
- **Live action needed:** `dbutils.fs.mount()` against a storage container, or a written
  demonstration.
- **GP1 enough?** Yes for the mount call itself, but it needs storage credentials this project does
  not hold, and a mount is **workspace-wide** - it would be visible to every user of a shared
  academy workspace.
- **Changes data?** No. **Destructive?** No, but it is workspace-scoped and therefore not isolated.
- **Cost class:** free.
- **Recommendation:** satisfy this with documentation, not a mount. Creating a workspace-wide mount
  on a shared academy workspace to demonstrate a deprecated pattern is poor judgment regardless of
  authorization, and the lesson - that mounts bypass Unity Catalog's permission model, which is
  exactly why they are deprecated - is better made in prose than by doing it.

### 2. Lab 6 - Alerts / email

- **Requirement:** an alert on a quality or volume condition, delivered somewhere.
- **Current:** the conditions are written as SQL in `sql/13_dashboard_data_quality.sql`.
- **Live action needed:** a Databricks SQL Alert, which requires a **SQL warehouse** to evaluate
  its query on a schedule.
- **GP1 enough?** No. **SQL warehouse required?** Yes. **Serverless?** Typically, yes.
- **Changes data?** No. **Destructive?** No.
- **Cost class:** **billable and recurring** - a scheduled alert starts a warehouse on every
  evaluation, which is the one pattern here that costs money while nobody is watching.
- **Recommendation:** batch with the dashboard, and choose the longest acceptable schedule. A daily
  alert on a one-snapshot table is nearly meaningless anyway; it becomes meaningful once the
  monthly archive is loaded.

### 3. Lab 7 - Databricks Connect / monitoring

- **Requirement:** show local development against remote compute.
- **Current:** untested; the project runs notebooks via Jobs instead.
- **Live action needed:** `pip install databricks-connect` matching the cluster's DBR, then a
  local session against GP1.
- **GP1 enough?** Yes - this is exactly what it is for.
- **Changes data?** No, if restricted to reads. **Destructive?** No.
- **Cost class:** cluster time only, and it reuses a cluster that is already running.
- **Caveat worth stating:** `databricks-connect` must match the cluster's DBR version and it
  **conflicts with a local `pyspark` install** - it replaces it. Installing it in this project's
  environment would break the 100+ local Spark tests, so it needs a separate virtualenv.
- **Recommendation:** cheapest remaining item. Do it in an isolated venv, run one read-only query,
  record the output.

### 4. Lab 8 - Idempotent deployment / approvals

- **Requirement:** prove a redeploy is idempotent and gated by approval.
- **Current:** four Jobs deploy via DAB; `bundle plan` already reported `1 unchanged` for an
  unchanged Job, which is the idempotency evidence in miniature.
- **Live action needed:** a GitHub Actions deploy job gated by a protected environment.
- **GP1 enough?** Yes - the deploy touches the control plane, not compute.
- **Changes data?** No. **Destructive?** No, though a careless `bundle destroy` would be; that
  command should not be in the workflow at all.
- **Cost class:** free.
- **Recommendation:** add a `workflow_dispatch` deploy job requiring a protected environment, and
  run it once. Needs a workspace token in GitHub secrets - the only new credential in this list.

### 5. Lab 8 - DEV-to-PROD promotion

- **Requirement:** promote an artifact from one environment to another.
- **Current:** two bundle targets exist, but both point at **the same workspace**; the dev target
  differs only by schema suffix.
- **Live action needed:** a genuinely separate workspace, or an honest account of why this is
  simulated.
- **GP1 enough?** No - this is not a compute question.
- **Cost class:** a second workspace is **new paid infrastructure**.
- **Recommendation:** do **not** create a workspace. Document the schema-level separation as what
  it is - a simulation - and say plainly that two targets in one workspace sharing one metastore do
  not constitute isolated environments. That honesty is worth more than a fabricated promotion.

### 6. Lab 8 - Post-deploy validation

- **Requirement:** automated validation after a deployment.
- **Current:** success criteria are written; nothing runs them automatically.
- **Live action needed:** a post-deploy step that triggers a Job and asserts on its result.
- **GP1 enough?** Yes.
- **Changes data?** Only if it runs a writing Job. Point it at a **dry-run** (`run_transform=false`)
  so it validates deployment without writing.
- **Cost class:** minutes of cluster time.
- **Recommendation:** batch with item 4 - they are the same workflow change.

### 7. Lab 9 - Jobs API / pipeline trigger

- **Requirement:** create, reset and trigger a Job through the API rather than the UI.
- **Current:** `automation.py` has the polling primitives and the mocked negative paths; the
  orchestration is unexercised. Note that `jobs submit` **has** now been used twice for the
  read-only verification runs, so part of this is closer than the matrix suggests.
- **GP1 enough?** Yes.
- **Changes data?** Only if the triggered Job writes; use a dry run.
- **Cost class:** minutes of cluster time.
- **Recommendation:** cheap, and it pairs naturally with item 3 in one GP1 batch.

### 8. Lab 9 - CI integration

- **Requirement:** CI that integrates with the workspace, not just static checks.
- **Current:** CI runs Ruff, Black, pytest and offline bundle validation - no workspace contact.
- **Live action needed:** an authenticated CI step, e.g. `bundle validate` against the real target.
- **GP1 enough?** Not needed at all - validation touches no compute.
- **Changes data?** No. **Destructive?** No.
- **Cost class:** free.
- **Recommendation:** batch with items 4 and 6; all three are one workflow file change plus one
  secret.

## Approval batches, smallest first

The eight pending rows map to the execution batches as follows. A2 and B are still useful final
demonstrations, although their matrix rows already say `Implemented locally` rather than
`Pending live`.

| Pending row | Batch | Reason |
|---|---|---|
| Legacy mounts exercise | E / document only | Workspace-wide deprecated mount; poor practice in a shared academy workspace |
| Alerts / email | C | Requires a scheduled SQL warehouse query |
| Databricks Connect / monitoring | A | Read-only remote session on an already-running GP1 |
| Idempotent deployment / approvals | D | Protected GitHub environment and authenticated control-plane deployment |
| DEV-to-PROD promotion | D / document only | A real second environment would require new paid infrastructure |
| Post-deploy validation | D | CI control-plane step, with any Job trigger explicitly treated as a write action |
| Jobs API / pipeline trigger | A for the dry-run Job; B for Lakeflow | A Job trigger creates a run record; a Lakeflow trigger also writes isolated pipeline tables |
| CI integration | D | Authenticated bundle validation, with no compute required |

| Batch | Contents | Resource | Data change | Cost | Duration |
|---|---|---|---|---|---|
| **A. GP1 validation** | Databricks Connect read-only session (3), Jobs API trigger of a dry run (7), GE suites against the live tables, 1,000-file Auto Loader discovery test | GP1, already running | **Mixed:** Connect and GE are reads; a Job trigger creates a run; the 1,000-file test uploads files and writes Auto Loader state and Delta rows | Cluster time | ~30 min |
| **A2. Monthly archive** | Full January 2024 ingestion, see [MONTHLY_ARCHIVE_PLAN.md](MONTHLY_ARCHIVE_PLAN.md) | GP1 + local download | Adds a new `execution_id`; the sample's rows untouched | Cluster time | ~45-60 min |
| **B. Lakeflow bounded** | One triggered update of the isolated pipeline | **Serverless** | Writes to the isolated `..._lakeflow` schema only | **Billable** | ~15 min |
| **C. SQL warehouse** | Publish the dashboard (pages 1 and 4 now), one alert (2) | **SQL warehouse** | None | **Billable, recurring for the alert** | ~45 min |
| **D. CI and promotion** | Authenticated CI validation (8), gated deploy (4), post-deploy validation (6), DEV→PROD written up honestly (5) | GitHub secret | None | Free | ~30 min |
| **E. Governance and maintenance** | RLS, masks, grants, ABAC tags, OPTIMIZE, deletion vectors, CDF, and VACUUM **last** | GP1 | **Yes - changes visibility and rewrites files** | Cluster time | ~45 min |

### Recommended order, and why

**A → A2 → D → B → C → E.**

- **A first** because it costs nothing new, uses a cluster that is already warm, and clears three
  requirements.
- **A2 next** because the monthly archive unlocks the most downstream value: pages 2 and 3 of the
  dashboard stop being 40-row curiosities, and the quality rules finally meet real defects.
- **D before B and C** because it is free and touches no data, so there is no reason to pay first.
- **B before C** because Lakeflow's isolated tables are worth comparing against the notebook
  path's before a dashboard is published off either.
- **E last, and VACUUM last within E.** Everything in E changes visibility or rewrites files, and
  VACUUM permanently destroys the time travel that every earlier rollback plan depends on. It
  should be the final action taken, after all evidence is captured.

Item 1 (legacy mounts) and item 5 (DEV→PROD) are recommended as **documented rather than executed**,
for reasons given above. If that is accepted, the realistic ceiling for `Validated live` is 6 of
the 8 remaining rows.

## Batch A command-level preflight - prepared, not executed

Every command below first depends on a read-only `clusters get` result showing GP1
`0702-132442-toro5spu` is already `RUNNING`. A terminated result stops the batch; none of these
steps may start or restart the shared cluster.

| Action | Exact command or operation | Classification | Expected duration and evidence | Rollback |
|---|---|---|---|---|
| Databricks Connect validation | In a separate venv with Databricks Connect matching DBR 17.3, build `DatabricksSession.builder.profile("dev").clusterId("0702-132442-toro5spu").getOrCreate()`, then collect `SELECT current_user(), current_catalog(), current_schema()` and `SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status` | **READ**: attaches to already-running GP1 and issues two SELECTs | 2-5 min; record identity, catalog/schema and the measured row count | Stop the local session only; no Azure data rollback |
| Jobs API trigger demonstration | `databricks bundle run urbanflow_silver_gold_test -t azure --profile dev --params run_transform=false` | **WRITE/control plane**: creates and executes a Job run even though the notebook exits before a data write | 1-3 min; Job run URL, terminal `SUCCESS`, and `DRY_RUN` task output | No data rollback; the immutable run record remains as evidence |
| GE against live tables | Through the same isolated Databricks Connect session, call `validate_frame(spark.table("dbr_dev.parvinbadalov_urbanflow.silver_station_status"), suite_name=SILVER_SUITE, expectations=silver_expectations())`; validate `silver_historical_trips` only after filtering to `execution_id = 'urbanflow-hist-devsample40-20261002T0010Z'` | **READ**: GE uses an ephemeral context and Spark actions only | 3-8 min; two JSON reports with per-expectation results and no persisted GE state | Stop the local session; no Azure rollback |
| 1,000-file Auto Loader discovery | Generate locally with `python scripts/generate_many_small_files.py <temp> --files 1000 --rows-per-file 5`; upload only `synthetic_trips_*.csv` to `dbfs:/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing/landing/historical_trips/synthetic-1000/`; then run `databricks bundle run urbanflow_historical_trips_test -t azure --profile dev --params run_ingest=true,execution_id=urbanflow-hist-synthetic1000-<UTC>,landing_subdir=synthetic-1000,stream_timeout_seconds=900` | **WRITE**: 1,000 Volume uploads, Auto Loader schema/checkpoint state, Delta MERGEs and a report | 8-15 min; 1,000 discovered files, 5,000 reconciled rows, bounded termination, and an evidence path | Delete only that execution id from the four historical tables, then remove the `synthetic-1000` landing/schema/checkpoint paths and locally run the generator's `--cleanup` |

The local generator step is safe to run independently. The upload, both Job triggers, Auto Loader
state, Delta writes and evidence report are Azure writes and remain unexecuted until separately
authorized.

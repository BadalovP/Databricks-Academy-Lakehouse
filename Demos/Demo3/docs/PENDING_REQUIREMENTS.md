# The remaining Pending live requirements, and how to clear them

Eight rows remain `Pending live` after this round (two of the previous ten moved to
`Implemented locally`: the 1,000-file generator and the Great Expectations suite). Each is analysed
below, then grouped into the smallest number of approval batches.

## Two that just moved

| Requirement | What changed |
|---|---|
| Lab 3 - Approximately 1,000 files | `scripts/generate_many_small_files.py` generates 1,000 tiny deterministic CSVs (under 1.5 MB total), with cleanup and 14 tests. Local only; the upload is a separate approval |
| Lab 7 - Great Expectations or Soda | GE 1.23.2 chosen by measurement (Soda would pin pyspark to 3.5.9), suites in `expectations.py`, 11 tests. See [QUALITY_FRAMEWORK.md](QUALITY_FRAMEWORK.md) |

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

| Batch | Contents | Resource | Data change | Cost | Duration |
|---|---|---|---|---|---|
| **A. GP1 non-destructive** | Databricks Connect read-only session (3), Jobs API trigger of a dry run (7), GE suites against the live tables, 1,000-file Auto Loader discovery test | GP1, already running | **None** - reads and dry runs only | Cluster time | ~30 min |
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

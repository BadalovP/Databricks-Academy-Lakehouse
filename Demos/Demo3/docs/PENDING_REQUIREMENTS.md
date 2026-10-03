# The remaining Pending live requirements, and how to clear them

> **Updated 2026-10-03.** The CI/CD batch cleared **four** of these rows - idempotent deployment
> and approvals, post-deploy validation, the Jobs API trigger, and `CI integration` - leaving
> **3** `Pending live`. `CI integration` was unblocked by four additive, read-only `CAN_VIEW` Job
> ACL patches, after which workflow run `37084965415` completed SUCCESS end to end. See
> [CICD_BATCH_EVIDENCE.md](CICD_BATCH_EVIDENCE.md) section 9 for the pre- and post-change ACLs and
> the proof that `IS_OWNER` and `admins CAN_MANAGE` survived.
>
> **Final reconciliation, same day.** **One** row now remains `Pending live`: **Lab 6 alerts and
> email**, which needs a billable SQL warehouse to evaluate its query on a schedule. That is real
> outstanding work rather than a documentation gap, so it stays pending.
>
> The legacy mount and DEV-to-PROD promotion moved to `Implemented locally` rather than staying
> pending, because for both the documentation **is** the deliverable and execution is a settled
> decision, not outstanding work. Both are now backed by real sections in
> [ARCHITECTURE.md](ARCHITECTURE.md). Two documentation defects were fixed in the process: the
> mount row cited an architecture section that had never been written, and the two-workspace
> section called the trial workspace "a genuine second environment for CI/CD promotion" while
> justifying it with the fact that it reaches the same data - which refutes the claim rather than
> supporting it.
>
> **A correction worth recording.** An earlier status report listed Databricks Connect as still
> pending and proposed re-running it as the next batch. That was wrong: it had been
> `Validated live` since PR #49, as the paragraph immediately below this blockquote already stated,
> and acting on the recommendation would have repeated proven work. The pending set was Lab 2,
> Lab 6 and Lab 8 - not Lab 2, Lab 7 and Lab 8.

The analysis below was written when seven rows were pending, and is kept because the
resource/cost/destructiveness assessment for each remains accurate and useful. Read it against the
blockquote above for current status: Databricks Connect and Great Expectations were closed by the
Batch A read validation, four more by the CI/CD batch, and two by the documentation reconciliation.

## Completed preparation and read validation

| Requirement | What changed |
|---|---|
| Lab 3 - Approximately 1,000 files | `scripts/generate_many_small_files.py` generates 1,000 tiny deterministic CSVs (under 1.5 MB total), with cleanup and 14 tests. Local only; the upload is a separate approval |
| Lab 7 - Great Expectations or Soda | GE 1.23.2 chosen by measurement and run against existing tables on GP1: Silver 10/10 and historical sample 10/10, zero failures. See [the evidence](../evidence/BATCH_A_READ_VALIDATION.md) |
| Lab 7 - Databricks Connect / monitoring | Databricks Connect 17.3.13 attached to already-running GP1 and reproduced the expected table counts and status distributions through read-only Spark operations. See [the evidence](../evidence/BATCH_A_READ_VALIDATION.md) |

## The seven remaining

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

### 3. Lab 8 - Idempotent deployment / approvals

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

### 4. Lab 8 - DEV-to-PROD promotion

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

### 5. Lab 8 - Post-deploy validation

- **Requirement:** automated validation after a deployment.
- **Current:** success criteria are written; nothing runs them automatically.
- **Live action needed:** a post-deploy step that triggers a Job and asserts on its result.
- **GP1 enough?** Yes.
- **Changes data?** Only if it runs a writing Job. Point it at a **dry-run** (`run_transform=false`)
  so it validates deployment without writing.
- **Cost class:** minutes of cluster time.
- **Recommendation:** batch with item 3 - they are the same workflow change.

### 6. Lab 9 - Jobs API / pipeline trigger

- **Requirement:** create, reset and trigger a Job through the API rather than the UI.
- **Current:** `automation.py` has the polling primitives and the mocked negative paths; the
  orchestration is unexercised. The Connect validation deliberately created no Job run, so it did
  not satisfy this requirement accidentally.
- **GP1 enough?** Yes.
- **Changes data?** Only if the triggered Job writes; use a dry run.
- **Cost class:** minutes of cluster time.
- **Recommendation:** cheap, and it pairs naturally with item 3 in one GP1 batch.

### 7. Lab 9 - CI integration

- **Requirement:** CI that integrates with the workspace, not just static checks.
- **Current:** CI runs Ruff, Black, pytest and offline bundle validation - no workspace contact.
- **Live action needed:** an authenticated CI step, e.g. `bundle validate` against the real target.
- **GP1 enough?** Not needed at all - validation touches no compute.
- **Changes data?** No. **Destructive?** No.
- **Cost class:** free.
- **Recommendation:** batch with items 3 and 5; all three are one workflow file change plus one
  secret.

## Approval batches, smallest first

The seven pending rows map to the execution batches as follows. A2 and B are still useful final
demonstrations, although their matrix rows already say `Implemented locally` rather than
`Pending live`.

| Pending row | Batch | Reason |
|---|---|---|
| Legacy mounts exercise | E / document only | Workspace-wide deprecated mount; poor practice in a shared academy workspace |
| Alerts / email | C | Requires a scheduled SQL warehouse query |
| Idempotent deployment / approvals | D | Protected GitHub environment and authenticated control-plane deployment |
| DEV-to-PROD promotion | D / document only | A real second environment would require new paid infrastructure |
| Post-deploy validation | D | CI control-plane step, with any Job trigger explicitly treated as a write action |
| Jobs API / pipeline trigger | A for the dry-run Job; B for Lakeflow | A Job trigger creates a run record; a Lakeflow trigger also writes isolated pipeline tables |
| CI integration | D | Authenticated bundle validation, with no compute required |

| Batch | Contents | Resource | Data change | Cost | Duration |
|---|---|---|---|---|---|
| **A0. GP1 reads - COMPLETE** | Databricks Connect and GE against existing corrected tables | GP1, already running | None; `SHOW` and `SELECT` only | Cluster time | Completed 2026-10-03 |
| **A. GP1 writes - approval required** | Jobs API trigger of a dry run; 1,000-file Auto Loader discovery test | GP1, already running | Job run record; then uploads, Auto Loader state, Delta rows and evidence for the file test | Cluster time | ~15-20 min |
| **A2. Monthly archive** | Full January 2024 ingestion, see [MONTHLY_ARCHIVE_PLAN.md](MONTHLY_ARCHIVE_PLAN.md) | GP1 + local download | Adds a new `execution_id`; the sample's rows untouched | Cluster time | ~45-60 min |
| **B. Lakeflow bounded** | One triggered update of the isolated pipeline | **Serverless** | Writes to the isolated `..._lakeflow` schema only | **Billable** | ~15 min |
| **C. SQL warehouse** | Publish the dashboard (pages 1 and 4 now), one alert (2) | **SQL warehouse** | None | **Billable, recurring for the alert** | ~45 min |
| **D. CI and promotion** | Authenticated CI validation (7), gated deploy (3), post-deploy validation (5), DEV→PROD written up honestly (4) | GitHub secret | None | Free | ~30 min |
| **E. Governance and maintenance** | RLS, masks, grants, ABAC tags, OPTIMIZE, deletion vectors, CDF, and VACUUM **last** | GP1 | **Yes - changes visibility and rewrites files** | Cluster time | ~45 min |

### Recommended order, and why

**A0 complete; recommended remaining order: D → A → A2 → B → C → E.**

- **A0 is complete.** The two read-only checks passed without a Databricks mutation.
- **D next** because authenticated CI and its approval gate are the remaining no-data-change work.
- **A after separate approval** because both remaining actions create persistent control-plane or
  data-plane evidence even when the Job notebook itself uses its dry-run gate.
- **A2 next** because the monthly archive unlocks the most downstream value: pages 2 and 3 of the
  dashboard stop being 40-row curiosities, and the quality rules finally meet real defects.
- **D before B and C** because it touches no lakehouse data, so there is no reason to pay first.
- **B before C** because Lakeflow's isolated tables are worth comparing against the notebook
  path's before a dashboard is published off either.
- **E last, and VACUUM last within E.** Everything in E changes visibility or rewrites files, and
  VACUUM permanently destroys the time travel that every earlier rollback plan depends on. It
  should be the final action taken, after all evidence is captured.

Item 1 (legacy mounts) and item 4 (DEV→PROD) are recommended as **documented rather than executed**,
for reasons given above. If that is accepted, the realistic ceiling for `Validated live` is 5 of
the 7 remaining rows.

## Batch A command-level record

Every command below first depends on a read-only `clusters get` result showing GP1
`0702-132442-toro5spu` is already `RUNNING`. A terminated result stops the batch; none of these
steps may start or restart the shared cluster.

| Action | Exact command or operation | Classification | Expected duration and evidence | Rollback |
|---|---|---|---|---|
| Databricks Connect validation - **PASS 2026-10-03** | In the existing isolated venv with Databricks Connect 17.3.13, build `DatabricksSession.builder.profile("dev").clusterId("0702-132442-toro5spu").getOrCreate()`, then issue only `SHOW` and `SELECT` statements | **READ**: attached to already-running GP1 | Recorded identity, catalog/schema, exact 16-table inventory, five required counts and two status distributions in [evidence](../evidence/BATCH_A_READ_VALIDATION.md) | Local session stopped; no Azure rollback |
| Jobs API trigger demonstration | `databricks bundle run urbanflow_silver_gold_test -t azure --profile dev --params run_transform=false` | **WRITE/control plane**: creates and executes a Job run even though the notebook exits before a data write | 1-3 min; Job run URL, terminal `SUCCESS`, and `DRY_RUN` task output | No data rollback; the immutable run record remains as evidence |
| GE against live tables - **PASS 2026-10-03** | Through the same Connect session, validate Silver directly and call `prepare_trip_validation_frame` on `silver_historical_trips` filtered to `execution_id = 'urbanflow-hist-devsample40-20261002T0010Z'` | **READ**: GE used an ephemeral context and Spark actions only | Silver 10/10 and historical 10/10, zero failures and zero unexpected rows; [JSON evidence](../evidence/2026-10-03_batch_a_read_validation.json) | Context and local session stopped; no Azure rollback |
| 1,000-file Auto Loader discovery | Generate locally with `python scripts/generate_many_small_files.py <temp> --files 1000 --rows-per-file 5`; upload only `synthetic_trips_*.csv` to `dbfs:/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing/landing/historical_trips/synthetic-1000/`; then run `databricks bundle run urbanflow_historical_trips_test -t azure --profile dev --params run_ingest=true,execution_id=urbanflow-hist-synthetic1000-<UTC>,landing_subdir=synthetic-1000,stream_timeout_seconds=900` | **WRITE**: 1,000 Volume uploads, Auto Loader schema/checkpoint state, Delta MERGEs and a report | 8-15 min; 1,000 discovered files, 5,000 reconciled rows, bounded termination, and an evidence path | Delete only that execution id from the four historical tables, then remove the `synthetic-1000` landing/schema/checkpoint paths and locally run the generator's `--cleanup` |

### Exact commands prepared for the two remaining Batch A writes

These commands are recorded for a future separately approved run. They were **not** executed in
the 2026-10-03 read batch.

```powershell
# A. Jobs API control-plane write: creates an immutable Job run record.
databricks bundle run urbanflow_silver_gold_test -t azure --profile dev `
  --params run_transform=false
```

Expected evidence: a run URL, terminal `SUCCESS`, and `DRY_RUN` output from both tasks; no Delta
row change.

```powershell
# B. 1,000-file data-plane write: upload plus Auto Loader/Delta evidence.
$syntheticDir = 'C:\tmp\urbanflow-synthetic-1000'
$landing = 'dbfs:/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing/landing/historical_trips/synthetic-1000'
$executionId = 'urbanflow-hist-synthetic1000-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')

python scripts/generate_many_small_files.py $syntheticDir --files 1000 --rows-per-file 5
Get-ChildItem -LiteralPath $syntheticDir -Filter 'synthetic_trips_*.csv' -File | ForEach-Object {
  databricks fs cp $_.FullName "$landing/$($_.Name)" --profile dev
}
databricks bundle run urbanflow_historical_trips_test -t azure --profile dev `
  --params "run_ingest=true,execution_id=$executionId,landing_subdir=synthetic-1000,stream_timeout_seconds=900"
```

Expected evidence: exactly 1,000 discovered files and 5,000 rows reconciled, a bounded terminal
state, execution-scoped Delta results, and a distinct evidence report.

The local generator step is safe to run independently. The Job trigger, upload, Auto Loader state,
Delta writes and evidence report are Databricks/Azure writes and remain unexecuted until separately
authorized.

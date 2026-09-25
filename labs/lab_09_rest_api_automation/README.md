# LAB 09 · Databricks REST API / Python SDK Automation

## 1. Goal

Build a production-like automation package that drives a Databricks
lakehouse end to end through the **Databricks Python SDK / REST API**
rather than through a Databricks Asset Bundle (the approach Lab 8 used).
Where Lab 8 demonstrates declarative, bundle-driven CI/CD, Lab 9
demonstrates *imperative*, API-driven orchestration: authenticating,
discovering/creating resources, provisioning compute, triggering and
polling long-running work, and reconciling the result -- all from ordinary
Python code calling the SDK, wired into a GitHub Actions workflow that is
deliberately separate from Lab 8's.

## 2. Completion requirements (what this PR delivers)

- A `lab09` Python package (`src/lab09/`) covering authentication,
  preflight capability checks, Volume creation/discovery, Files API
  upload, idempotent incremental ingestion, workspace file upload,
  Lakeflow pipeline management, temporary compute provisioning, persistent
  Job management, explicit polling, and JSON reporting.
- Three Lakeflow pipeline source files (`pipeline/bronze.py`,
  `silver.py`, `gold.py`) implementing Bronze/Silver/Quarantine/Gold with a
  provable reconciliation invariant.
- A reconciliation notebook (`notebooks/01_reconcile_counts.py`) that
  returns machine-readable JSON via `dbutils.notebook.exit()`.
- An argparse CLI (`python -m lab09.cli ...`).
- A pytest suite covering the high-value logic with a mocked
  `WorkspaceClient` -- no live Databricks resources in PR tests.
- A separate GitHub Actions workflow
  (`.github/workflows/lab09_api_automation.yml`) with PR-safe static gates
  and a manually-dispatched, approval-gated live path.
- This README, documenting exactly what has and has not been proven live
  (see "Known limitations").

## 3. Dataset: why official NYC TLC monthly Parquet files, not `samples.nyctaxi.trips`

The task explicitly disallows `samples.nyctaxi.trips` (a pre-loaded sample
table with no real ingestion story) in favor of the **official NYC Taxi and
Limousine Commission (TLC) Yellow Taxi monthly trip record Parquet files**,
published at `nyc.gov/site/tlc/about/tlc-trip-record-data.page`. This
matters for what Lab 9 is trying to demonstrate: a *real* incremental
ingestion pattern (one file lands per month, the pipeline picks it up
incrementally via Auto Loader) rather than a single static table that is
already fully materialized inside the workspace. Using `samples.nyctaxi`
would skip the entire download/validate/upload/incremental-landing story
this lab exists to exercise.

The exact download URLs were verified directly against the official TLC
page (not guessed or reused from memory) before being written into
`config/dev.yml`:

- Monthly trips: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY-MM}.parquet`
- Zone lookup reference: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

2024 months only, in order, are configured in `config/dev.yml`'s `months:`
list.

**The Databricks workspace never needs outbound internet access.** The
GitHub runner (or a local operator's machine) downloads the file over the
public internet; the bytes are then pushed into the workspace over the
Files API. Nothing inside the Databricks workspace ever calls out to
`cloudfront.net`.

## 4. Architecture

```text
                    +-------------------------------------------------+
                    |         GitHub Actions runner / local CLI       |
                    |  (downloads NYC TLC Parquet over the internet)  |
                    +-------------------------+-------------------------+
                                              |
                                Databricks Python SDK / REST API
                                              |
              +-------------------------------v-------------------------------+
              |                      Databricks workspace                     |
              |                                                               |
              |  Files API                                                    |
              |  /Volumes/dbr_dev/parvinbadalov/lab09_landing/                |
              |    trips/yellow_tripdata_2024-01.parquet ...                  |
              |    reference/taxi_zone_lookup.csv                             |
              |                        |                                     |
              |                        v  (Auto Loader, cloudFiles)          |
              |  +------------------------------------------------------+    |
              |  |  Lakeflow pipeline (separate, pipeline-managed        |    |
              |  |  compute -- serverless by default, classic fallback)  |    |
              |  |    lab09_taxi_bronze                                  |    |
              |  |    lab09_taxi_silver / lab09_taxi_quarantine          |    |
              |  |    lab09_taxi_daily_summary                           |    |
              |  +------------------------------------------------------+    |
              |                        |                                     |
              |                        v                                     |
              |  +------------------------------------------------------+    |
              |  | Temporary notebook-job cluster (separate compute --   |    |
              |  | single-node, autotermination_minutes=20)              |    |
              |  |   persistent Job "lab09_taxi_reconciliation_job"      |    |
              |  |     -> notebooks/01_reconcile_counts.py               |    |
              |  |     -> dbutils.notebook.exit(json...)                 |    |
              |  +------------------------------------------------------+    |
              +-------------------------------------------------------------+
                                              |
                                get_run_output(task_run_id)
                                              |
                                              v
                              evidence/lab09_report.json
```

### Files/Volume vs. Lakeflow compute vs. temporary notebook-job compute

These are three genuinely separate concerns, and the code keeps them
separate rather than assuming any two are interchangeable:

| Concern | Where it lives | Lifecycle |
|:--|:--|:--|
| **Files/Volume** | Unity Catalog managed Volume `dbr_dev.parvinbadalov.lab09_landing` | Permanent; only `cleanup --reset-landing` touches it, and only under its own path |
| **Lakeflow pipeline compute** | Managed by the pipeline itself (serverless by default; classic cluster definition if serverless is rejected) | Managed by Lakeflow, not by this code -- `pipelines.py` never assumes it can reuse this compute for anything else |
| **Temporary notebook-job compute** | A single-node cluster created by `compute.py` specifically to run the reconciliation Job | Created fresh each `run-all`, `autotermination_minutes=20`, explicitly terminated in the outer `finally`-equivalent safety net every single run |

### Workspace object types: pipeline source FILEs vs. the reconciliation NOTEBOOK

`bronze.py`/`silver.py`/`gold.py` and `01_reconcile_counts.py` are uploaded
through the same Workspace Import API but must become two genuinely
different Workspace object types, and `workspace.py` uses different
`import_()` parameters for each:

- `01_reconcile_counts.py` has a `# Databricks notebook source` header and
  is imported with `format=SOURCE, language=PYTHON`, producing an
  `ObjectType.NOTEBOOK` -- required for `NotebookTask`.
- `bronze.py`/`silver.py`/`gold.py` have no notebook header and must
  become plain `ObjectType.FILE` objects, matching what
  `pipelines.py`'s glob-include `PipelineLibrary` expects. Per the SDK's
  own `import_()` docstring, `language` "is set only if the object type is
  NOTEBOOK", and Databricks' own CLI/REST docs state that importing a
  single file as `SOURCE` requires (and therefore produces) a notebook --
  so `format=RAW` is used instead, with no `language` set, which imports
  the bytes as-is.

This was verified against this repository's actually-installed
`databricks-sdk` package: the correct method is `workspace.import_()`
(there is no `workspace.upload()` method, and an earlier version of this
code called that non-existent method), and its `content` parameter must
be base64-encoded text, not raw bytes -- both were real bugs, now fixed.
**What has not been verified is live Lakeflow behavior**: whether a
pipeline's glob/file library actually resolves a `RAW`-imported file as
valid pipeline source has not been confirmed against a real workspace.
See "Known limitations".

## 5. API/SDK operations used

| Module | Databricks SDK / REST surface |
|:--|:--|
| `client.py` | `WorkspaceClient` construction (never guesses a profile -- see Security model) |
| `preflight.py` | `current_user.me`, `clusters.spark_versions`, `clusters.list_node_types`, `cluster_policies.list`, `catalogs.get`, `schemas.get`, `volumes.read`, `files.upload`/`list_directory_contents`/`delete`, `pipelines.list_pipelines`, plus the real cluster-create probe below |
| `volumes.py` | `volumes.read`, `volumes.create` |
| `landing.py` | `files.list_directory_contents`, `files.create_directory`, `files.upload`, `files.get_metadata`, `files.delete` |
| `workspace.py` | `workspace.mkdirs`, `workspace.import_` (`ImportFormat.RAW` for pipeline source files, `ImportFormat.SOURCE`/`Language.PYTHON` for the reconciliation notebook -- see below) |
| `compute.py` | `clusters.spark_versions`, `clusters.list_node_types`, `cluster_policies.list`, `clusters.create`, `clusters.get`, `clusters.delete` |
| `pipelines.py` | `pipelines.list_pipelines`, `pipelines.create`, `pipelines.update`, `pipelines.start_update` |
| `jobs.py` | `jobs.list`, `jobs.create`, `jobs.reset`, `jobs.run_now`, `jobs.get_run`, `jobs.get_run_output` |
| `monitoring.py` | `clusters.get`, `pipelines.get_update`, `jobs.get_run` -- called in an explicit loop, never via `.result()` |

Every SDK call signature and enum used above was cross-checked against the
actually-installed `databricks-sdk` package in this repository's local
Python environment before being written (method signatures,
dataclass field names, and enum values), specifically to avoid writing
code against a remembered-but-wrong API shape.

## 6. Phase 0 / preflight capability checks

`clusters.list()` (or, here, `clusters.list_node_types()`) alone does **not**
prove cluster-create permission -- it only proves the identity can see
workspace metadata. `preflight.py` runs the following, independently, so
one failure never hides another:

Every check reports one of three states -- `PASS`, `FAIL`, or `NOT_TESTED`
-- never just a boolean. A `NOT_TESTED` result is never described as
passed: `PreflightReport.passed` (used to gate `run-all` and the
`preflight` CLI's exit code) only requires that no check actually `FAIL`ed,
but each individual check's real state stays visible in the report.

1. `authenticated_identity` -- `current_user.me()`
2. `spark_runtimes_listed` -- `clusters.spark_versions()`
3. `node_types_listed` -- `clusters.list_node_types()`
4. `cluster_policies_listed` -- `cluster_policies.list()`
5. `catalog_access` -- `catalogs.get(catalog)`
6. `schema_access` -- `schemas.get(catalog.schema)`
7. `volume_access` -- two modes:
   - lightweight (`run-all`'s own internal gate, and plain `preflight`):
     read-only `volumes.read(...)`; a genuine `NotFound` here is reported
     as **`PASS`** ("does not exist yet, will be created") -- getting an
     authoritative "it doesn't exist" answer is a fully executed,
     successful check of volume access, since the volume is created later
     in the execution order, not before preflight.
   - full live probe (`preflight --probe-cluster-create`): actually
     `volumes.ensure_volume(...)` -- create-or-get, scoped to only this
     config's own `catalog.schema.volume`, never any other resource --
     reported `PASS` as "ensured (created-or-existing)".
   Either way, any other error (permission, auth, service) is a real
   `FAIL`, using the SDK's typed `NotFound` exception rather than
   string-matching an error message.
8. `files_api_roundtrip` -- upload/list/delete a tiny probe file under
   `_preflight/` (the directory is created explicitly first via
   `files.create_directory()`).
   - lightweight mode: reported as **`NOT_TESTED`**, not `PASS`, when the
     volume doesn't exist yet -- the round trip was not actually
     exercised, and this report never claims otherwise.
   - full live probe: since `volume_access` just ensured the volume
     exists, this always genuinely runs and reports `PASS`/`FAIL` --
     never `NOT_TESTED` -- unless the volume ensure itself failed, in
     which case it correctly falls back to the lightweight (`NOT_TESTED`)
     behavior rather than crashing.
9. `pipeline_list_permission` -- `pipelines.list_pipelines()`
10. `cluster_create_probe` -- **opt-in only** (`--probe-cluster-create`),
    `NOT_TESTED` otherwise: creates a real tiny single-node cluster
    (`autotermination_minutes=20`), polls it explicitly to `RUNNING`, and
    immediately terminates it in a `finally` block regardless of outcome.
    This is the only check in this list that provisions real compute, and
    it is the only reliable way to confirm create permission --
    `clusters.list_node_types()` succeeding proves nothing about create
    permission on its own, and this probe is never weakened to that
    cheaper check.

### Cluster fallback behavior

The reconciliation Job task can be configured in exactly one of three
mutually exclusive ways (`jobs.py`'s `_task_settings()` validates that
precisely one is selected; confirmed against the installed
`databricks-sdk==0.133.0`'s `Task` field set before coding):

| `compute_mode` | Job task fields set | Compute Databricks uses |
|:--|:--|:--|
| `explicit_cluster` | `existing_cluster_id` | A standalone cluster this run created via `compute.try_start_cluster_create()`, polled, then explicitly terminated |
| `job_cluster` | `new_cluster` | A job-managed **classic** cluster Databricks provisions and tears down itself as part of the run -- still classic compute under the hood, just job-scoped instead of standalone |
| `serverless_job` | none of `existing_cluster_id` / `new_cluster` / `job_cluster_key` | Databricks-managed **serverless** job compute -- a genuinely separate infrastructure path from the other two |

`run-all --compute-mode auto` (the default) resolves which of these to use
from `config/dev.yml`'s `compute.preferred_mode`, a deliberate,
evidence-backed per-workspace choice, rather than hardcoding this
workspace's hostname into business logic. If `preferred_mode` is unset,
`auto` falls back to the legacy behavior: attempt `explicit_cluster` via
`compute.try_start_cluster_create()`, and fall back to `job_cluster` only
on a genuine permission/policy/unsupported-compute rejection (the SDK's
typed `PermissionDenied` or `InvalidParameterValue` -- deliberately
**not** a bare `except Exception`, and deliberately **not**
`TimeoutError`, so an unrelated or ambiguous failure fails the run instead
of silently switching modes). `--compute-mode explicit_cluster` /
`job_cluster` / `serverless_job` forces that exact mode with no
attempt/fallback at all. Whichever mode is used, the exact same persistent
Lab 9 job is reused (never a new duplicate), and the report's
`compute_mode` field records which one actually ran.

**Live Phase 0 testing against the confirmed-safe `personal-yahoo`
workspace (`dbc-1750318a-76a9.cloud.databricks.com`) found that explicit
`clusters.create()` does not work in this specific
workspace/organization**: `preflight --probe-cluster-create` (see
`evidence/phase0_2026-09-24.json`) failed with `TimeoutError: Timed out
after 0:05:00 | caused by BadRequest: Current organization
7474653929863069 does not have any associated worker environments`. This
is a genuine backend/organization-level limitation (this Personal
workspace has no "worker environment" provisioned for classic compute),
not a permission problem (the identity has the `allow-cluster-create`
entitlement) and not a `ClusterSpec` defect -- an earlier, separate bug in
`resolve_lts_spark_version`/`resolve_node_type` (both ignored CPU
architecture, so an `aarch64` runtime could be paired with an x86_64 node
type) was found and fixed during this same investigation, and the
corrected, architecturally-compatible spec still failed identically,
confirming the worker-environment limitation is the real, separate cause.

**Recommendation for this specific workspace: `compute_mode =
"serverless_job"` (`config/dev.yml`'s `compute.preferred_mode`), not
`job_cluster`.** An earlier version of this document recommended
`job_cluster` -- that was a mistake, corrected here: a Jobs `new_cluster`
is **still classic jobs compute**, provisioned through the same
cluster-manager/worker-environment infrastructure as a standalone
cluster, just job-scoped instead of standalone. It would be expected to
fail identically to the `explicit_cluster` attempt above, for the same
organization-level reason, so it is not a meaningfully different
infrastructure path and not the preferred next thing to test here.
`serverless_job` is the genuinely distinct, Databricks-managed compute
path that does not depend on this organization's classic-compute
worker-environment infrastructure at all. **This has not yet been
confirmed by an actual successful serverless job run** -- see "Known
limitations".

Note that because the classic-compute failure surfaces as `TimeoutError`
(via the SDK's own internal HTTP retry-and-give-up behavior -- see below),
not as `PermissionDenied`/`InvalidParameterValue`,
`try_start_cluster_create()` does **not** and should **not**
automatically trigger `job_cluster` or `serverless_job` for this specific
failure mode -- this task's explicit instruction was not to broadly catch
`TimeoutError` as if it were a permission-style rejection, since an
ordinary timeout is not the same thing as "forbidden" and could just as
easily mean something else entirely. That is exactly why this workspace's
choice is recorded **deliberately** in `config/dev.yml`'s
`compute.preferred_mode` instead of being detected automatically at
runtime -- `--compute-mode auto` reads that recorded choice directly
rather than rediscovering the same 5-minute failure on every single run.

**Where the "5 minutes" actually comes from** (verified by reading the
installed `databricks-sdk==0.133.0` source, not assumed): it is
`databricks.sdk._base_client.BaseClient.__init__`'s
`self._retry_timeout_seconds = retry_timeout_seconds or 300` -- a default
retry-timeout wrapper the SDK applies to **every** HTTP call
(`databricks.sdk.retries.retried`), including the single `POST
/api/2.1/clusters/create` request. It is completely independent of, and
unrelated to, this project's own `cluster_timeout_seconds` polling config
in `config/dev.yml` (`monitoring.poll_cluster_state`'s own loop never even
started here, since `clusters.create()` itself never returned a
`cluster_id`). `Wait.__getattr__` (confirmed by reading
`databricks/sdk/service/_internal.py`) resolves `waiter.cluster_id` as a
synchronous dict lookup with no implicit wait, so LAB 09's own
`start_cluster_create()` was never the source of any hidden blocking
either.

### Lab requirement vs. Personal workspace reality

The Lab 9 task literally asks the automation to **create clusters**. The
Personal workspace this project has actually tested against
(`personal-yahoo`) cannot do that for classic compute -- proven live, not
assumed (see above). This is a genuine gap between the literal task
wording and what this specific workspace supports, and it is worth
stating plainly rather than papering over with a fallback that quietly
changes what was actually demonstrated. Two compliance interpretations
are possible, and **this PR does not decide between them**:

- **Option A -- run the Personal workspace end-to-end on
  `serverless_job`.** This proves API/SDK automation, Volume/Files API
  operations, Lakeflow pipeline triggering, persistent-Job
  find-or-reset-or-create, job execution, explicit monitoring, and
  reconciliation -- everything Lab 9 asks for **except** a successful
  `clusters.create()` call, which this workspace cannot do.
- **Option B -- additionally run the cluster-create portion in a
  separate, approved academy/Azure DEV workspace** that is confirmed to
  support classic clusters, if the "create clusters" requirement must be
  satisfied literally and cannot be waived for a Personal workspace's
  infrastructure limitation.

**Until a mentor/reviewer approves one of these interpretations (or a
successful classic cluster creation is demonstrated somewhere), the
"create clusters" portion of the Lab 9 requirement should not be
described as fully satisfied.** Everything else Lab 9 asks for remains
demonstrable, and is tracked separately in "Known limitations" by what
has and has not actually been run live.

### Landing schema vs. pipeline output schema

Confirmed live (`personal-yahoo`, 2026-09-23): `lab09_taxi_pipeline`
(`pipeline_id=9fcf88d2-8dac-4e2a-91e6-e89407c4fe92`) is correctly
configured with its source glob at
`/Workspace/Users/parvinbadalov@yahoo.com/lab09/pipeline/**` -- this is
**not** a workspace-path problem, and Lab 9's runtime files were never
moved into the Git checkout. Every one of 6 pipeline update attempts
reached `runtime_details` (confirming the glob resolved bronze.py /
silver.py / gold.py correctly each time) before failing identically with:

```text
[UNITY_CATALOG_INITIALIZATION_FAILED] ErrorClass=QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED
Cannot create 1 Table(s) in Schema 16e388e4-4f7d-4ea7-9a83-26c44917aced
(estimated count: 112, limit: 100).
```

That schema ID is `dbr_dev.parvinbadalov` (confirmed via `schemas get`) --
the same schema `config/dev.yml`'s top-level `schema:` names, which is
shared across every other lab and demo in this Personal workspace and has
hit Unity Catalog's per-schema table-count quota.

**Non-destructive fix (no existing tables/schemas/volumes/pipelines/jobs
were deleted):** Lab 9's pipeline OUTPUT tables now live in their own
dedicated schema, `dbr_dev.lab09` (`config/dev.yml`'s
`pipeline.target_schema`), instead of `dbr_dev.parvinbadalov`. The landing
Volume is **not** moved -- input data stays exactly where it was:

- Landing/input schema (`schema:` in `config/dev.yml`): `dbr_dev.parvinbadalov`
  -- holds `/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips/` and
  `.../reference/taxi_zone_lookup.csv`. Unchanged.
- Pipeline output schema (`pipeline.target_schema` in `config/dev.yml`):
  `dbr_dev.lab09` -- will hold `lab09_taxi_bronze`, `lab09_taxi_silver`,
  `lab09_taxi_quarantine`, `lab09_taxi_daily_summary`. New.

`volumes.ensure_output_schema()` (same create-or-get-only-if-`NotFound`
discipline as `ensure_volume()`) creates `dbr_dev.lab09` idempotently if it
doesn't already exist; `run-all` calls it right after `ensure_volume()`.

**Retargeting the existing pipeline in place turned out to be impossible --
confirmed live, 2026-09-24, not assumed.** Attempting to update
`lab09_taxi_pipeline` (`pipeline_id=9fcf88d2-8dac-4e2a-91e6-e89407c4fe92`)
to `target=lab09` failed with:

```text
InvalidParameterValue: Changing target schema is not allowed. Reason: DLT
does not yet support changing target schema of a pipeline that uses an
Default Storage catalog. Please create a new pipeline if you need to
change the target schema.
```

This is a genuine Databricks platform restriction -- a Default-Storage-
catalog DLT pipeline's target schema can only be chosen at creation time,
never changed afterward -- not a bug in this project's fallback logic (the
code's existing serverless->classic fallback correctly retried the update
as classic compute, since `InvalidParameterValue` is its rejection signal,
but the classic attempt failed identically, since the restriction is about
changing target schema at all, independent of compute type). The fix: a
**new** pipeline, `lab09_taxi_pipeline_v2` (`config/dev.yml`'s
`pipeline.name`), created fresh targeting `dbr_dev.lab09` from the start
(creation has no such restriction). The original `lab09_taxi_pipeline`
(`9fcf88d2-...`) is **left permanently untouched** -- nothing was deleted,
renamed, or retargeted. `ensure_pipeline()` itself never migrates an
existing pipeline's target schema; see its docstring.

Because that failure has nothing to do with landing a new month's data,
`run-all`'s step order is now:

```text
preflight -> ensure volume -> ensure dbr_dev.lab09 -> upload bronze/silver/gold
-> upload reconciliation notebook -> ensure/create lab09_taxi_pipeline_v2
-> land_next_month -> ensure reference CSV -> start_update
```

Pipeline source files and the notebook are uploaded *before* the pipeline
resource is created/updated, so its source glob always points at real,
already-uploaded content the moment the pipeline exists. `ensure_pipeline()`
then runs *before* `land_next_month()`, so a pipeline-side failure is
caught before wastefully downloading/uploading a new month -- exactly what
happened on the live attempt that surfaced this restriction (2024-03 was
landed, then the pipeline step failed). `land_next_month()` still runs
before `pipelines.start_update()`, since the pipeline needs real landed
data to process.

This also exposed a real latent defect in the serverless->classic fallback
itself: it treated **any** `InvalidParameterValue` as "serverless was
rejected, retry as classic," which meant it wastefully retried this exact
target-schema-change error as classic compute (and failed identically)
instead of surfacing the real error immediately. Excluding only that one
observed message would still be wrong, though -- the Pipelines/Jobs API
can raise `InvalidParameterValue` for many other unrelated reasons (a bad
catalog name, a malformed library path, ...) that also have nothing to do
with serverless support. `pipelines.py`'s `_is_serverless_capability_rejection()`
is therefore a **positive allow-list**, not a denylist: an
`InvalidParameterValue` is only ever treated as a genuine
serverless-capability rejection if its message unambiguously says
serverless itself is unavailable/unsupported (e.g. contains "serverless is
not enabled"); every other `InvalidParameterValue` -- the
target-schema-change rejection included, but not limited to it --
propagates immediately instead of triggering a second, likely-doomed API
call.

This alone is not sufficient, though: the reconciliation notebook
(`notebooks/01_reconcile_counts.py`) has its own hardcoded widget defaults
(`dbutils.widgets.text("schema", "parvinbadalov")`) that would silently
keep querying the *old* schema if left unpassed. `jobs.py`'s
`_task_settings()` now builds `NotebookTask.base_parameters` (confirmed
against the installed `databricks-sdk==0.133.0`: `NotebookTask` has a
typed `base_parameters: Optional[Dict[str, str]]` field) with
`catalog`/`schema`/all four table names sourced from `config/dev.yml`, so
the job always overrides the notebook's defaults with the pipeline's
actual output schema regardless of which of the three compute modes is
selected.

### CI identity vs. local identity

**Phase 0, if and when it is run, only proves the permissions of the
identity that ran it.** If GitHub Actions CI later authenticates as a
different identity (its own secret-backed token, as this repo's Lab 8
workflow already does for its own targets), a local preflight run does
**not** prove CI has the same permissions. `PreflightReport` records this
caveat verbatim in every report it produces.

## 7. Incremental month logic

`land_next_month()` (Phase 4 in the task spec):

1. Lists already-landed months by calling
   `files.list_directory_contents()` on the `trips/` Volume path and
   regex-parsing `yellow_tripdata_(\d{4}-\d{2})\.parquet` out of each
   entry's filename.
2. Picks the **first** month in the configured, ordered `months:` list
   (from `config/dev.yml`) that is not already landed.
3. If every configured month is already landed, returns `NO_NEW_DATA`
   without downloading anything -- **this is not a failure**. `run-all`
   still proceeds through pipeline execution, the reconciliation notebook,
   and report generation exactly as it would for a real new month, since
   those steps operate on whatever data already exists in the Volume.
4. Otherwise downloads that one month, retrying transient failures
   (default: 3 attempts, linear backoff), and validates the response
   before ever uploading it: non-zero size, and Parquet magic bytes
   (`PAR1`) at both the start and the end of the file. An invalid or
   truncated download raises `DownloadValidationError` and is never
   passed to `files.upload()`.
5. Uploads with `overwrite=False` -- an already-landed month is
   structurally impossible to overwrite through this code path, since step
   2 only ever selects a month that step 1 confirmed is absent.

`cleanup --reset-landing` deletes only files under this config's own
`trips/` (and, with `--reset-reference`, `reference/`) Volume path --
`reset_landing()` asserts every path it deletes starts with that exact
prefix before deleting it, and touches nothing else in Unity Catalog.

## 8. Data quality / quarantine design

Silver hard-validity rules (evaluated once per row, tagged into a
`failed_rules` array rather than silently dropped):

| Rule | Condition |
|:--|:--|
| `INVALID_FARE` | `fare_amount` is null or `<= 0` |
| `INVALID_DISTANCE` | `trip_distance` is null or `<= 0` |
| `INVALID_DATETIME_ORDER` | `tpep_dropoff_datetime <= tpep_pickup_datetime` (or either is null) |
| `INVALID_MONTH` | the pickup month does not match the month encoded in the source filename, derived from `_metadata.file_path` (captured by `bronze.py` as `_lab09_source_file`) |

`passenger_count` is **deliberately not a hard-validity rule**: a null or
non-positive value only sets `passenger_count_warning = true` and never
contributes to `failed_rules` or quarantine.

Pickup/dropoff location IDs are left-joined against
`reference/taxi_zone_lookup.csv`. A location ID can fail to be "known" two
different ways, and both are **kept**, never dropped, with `pickup_zone_known` /
`dropoff_zone_known` set to `false` and the borough/zone normalized to the
literal string `"UNKNOWN"`:

- **unmatched**: no row in the lookup file has this LocationID at all.
- **TLC's own semantic placeholders**: the lookup file *does* have a
  matching row (so a naive "did the join succeed" check alone would call
  it known), but its Borough or Zone value is itself TLC's own
  "Unknown"/"N/A" marker. Confirmed directly by downloading the real
  `taxi_zone_lookup.csv`: LocationID 264 = Borough `Unknown`, Zone `N/A`;
  LocationID 265 = Borough `N/A`, Zone `Outside of NYC`. An earlier version
  of this logic used "did the join match" as the sole test, which
  incorrectly reported 264/265 as *known* zones merely because they exist
  in the lookup file.

No hardcoded numeric ID range is used for any of this -- only the lookup
join result and the returned values. The classification is mirrored in
plain Python (`src/lab09/zone_lookup.py`, unit tested in
`tests/test_zone_lookup.py`) since `pipeline/silver.py` itself cannot be
unit tested outside a live Spark session.

### Why not just `@dp.expect_all_or_drop`

`@dp.expect_all_or_drop` (used elsewhere in this repository, e.g. Lab 8's
`pipeline/silver.py`) silently drops failing rows -- there is no table
recording *how many* rows were dropped or *why*. `silver.py` instead reads
bronze once (`_tagged_and_zoned_bronze()`), tags every row with its
`failed_rules` array, and materializes two disjoint views of the exact same
tagged data:

- `lab09_taxi_silver` = rows where `size(failed_rules) == 0`
- `lab09_taxi_quarantine` = rows where `size(failed_rules) > 0`, retaining
  the `failed_rules` array so a rejected row's reason(s) are always
  queryable

### The reconciliation invariant

```
bronze_rows == silver_valid_rows + quarantine_rows
```

This holds **by construction**, not by a downstream count adjustment: both
outputs partition the identical tagged dataframe by a single boolean
condition and its exact negation. `notebooks/01_reconcile_counts.py`
queries all four tables and `assert`s this invariant explicitly before
returning its JSON payload.

## 9. Monitoring behavior

No SDK `.result()` waiter is ever called for a cluster, pipeline update, or
job run. `monitoring.py` implements three explicit polling loops, each
with a configurable timeout and poll interval, and each logging **only on
state change** (not on every poll):

- `poll_job_run` -- life-cycle states `QUEUED`, `PENDING`, `RUNNING`,
  `TERMINATING`, `BLOCKED`, `WAITING_FOR_RETRY` are treated as non-terminal;
  `TERMINATED`, `INTERNAL_ERROR`, `SKIPPED` are terminal. Once terminated,
  result state (`SUCCESS`, `FAILED`, `TIMEDOUT`, `CANCELED`, or any other
  value the API returns) is recorded separately from life-cycle state.
- `poll_pipeline_update` -- `QUEUED`, `WAITING_FOR_RESOURCES`,
  `INITIALIZING`, `SETTING_UP_TABLES`, `RUNNING` are non-terminal;
  `COMPLETED`, `FAILED`, `CANCELED` are terminal.
- `poll_cluster_state` -- polls until `RUNNING` (usable) or a bad terminal
  state (`TERMINATED`, `ERROR`, `UNKNOWN`).

Every one of these state names was verified against the actually-installed
SDK's real enums (`RunLifeCycleState`, `RunResultState`, `UpdateInfoState`,
`State`) rather than assumed from memory; the real enums include a few
additional values in some cases (e.g. pipeline updates also report
`CREATED`, `RESETTING`, `STOPPING`), which the polling loops safely treat
as "keep polling" rather than crashing on an unrecognized state.

A timeout returns a result with `timed_out=True` instead of raising --
`cli.py` treats a timed-out outcome as a failed step and still runs the
outer cleanup/report path.

## 10. Cleanup guarantees

- The temporary notebook-job cluster is always targeted for termination in
  `cli.py`'s `_finish()`, which every `run-all` code path -- success,
  explicit failure, and unhandled exception -- routes through before
  returning. `compute.cluster_exists_and_active()` is checked first so a
  cluster that already terminated itself (or was never created) is not
  redundantly (and harmlessly, but noisily) re-deleted.
- `cleanup --reset-landing` only ever deletes paths under this config's own
  `trips/` (and, with `--reset-reference`, `reference/`) Volume prefix.
- Nothing in this code path ever calls `terraform apply`, deploys to Azure
  PROD, or deletes a Unity Catalog schema/table outside the Lab 9-owned
  path.

## 11. CLI examples

```bash
# From labs/lab_09_rest_api_automation, with a real profile:
python -m lab09.cli --profile personal-yahoo preflight
python -m lab09.cli --profile personal-yahoo preflight --probe-cluster-create
python -m lab09.cli --profile personal-yahoo run-all
python -m lab09.cli --profile personal-yahoo status
python -m lab09.cli --profile personal-yahoo cleanup --reset-landing
python -m lab09.cli --profile personal-yahoo cleanup --reset-landing --reset-reference

# Or via explicit host/token env vars (the pattern GitHub Actions uses):
DATABRICKS_HOST=... DATABRICKS_TOKEN=... python -m lab09.cli run-all
```

`--profile` (or `DATABRICKS_CONFIG_PROFILE`/`DATABRICKS_HOST`+
`DATABRICKS_TOKEN`) is **required** -- see "Security model" for why this
package refuses to pick a default profile itself.

## 12. GitHub Actions integration

`.github/workflows/lab09_api_automation.yml` is a **separate** workflow
from Lab 8's `lab08_cicd.yml` (not modified by this PR), scoped to
`labs/lab_09_rest_api_automation/**` and its own workflow file:

- `pull_request` / `push`: `static-checks` only -- Ruff, Black, pytest
  against a mocked `WorkspaceClient`. No live Databricks call is possible
  from this path.
- `workflow_dispatch`: `static-checks` -> `run-live-automation`.
  `run-live-automation` itself references the `lab09-live-approval`
  GitHub Environment (its required-reviewers rule must be configured once
  in GitHub's UI, which this workflow file cannot do itself) -- that alone
  is the approval gate; there is no separate dummy approval job, since a
  second job referencing the same protected environment would just make a
  human approve the identical prompt twice for one dispatch. Before
  running, an explicit shell step verifies `DATABRICKS_PERSONAL_HOST` /
  `DATABRICKS_PERSONAL_TOKEN` are actually set and fails with a clear
  `::error::` message if either is missing -- there is no hardcoded host
  fallback, so this workflow can never silently resolve against an
  unintended workspace. Then `python -m lab09.cli run-all` runs, and
  `actions/upload-artifact` uploads the generated `lab09_report.json`.
- `concurrency: { group: lab09-api-automation, cancel-in-progress: false
  }` prevents two live demonstrations from running at once.
- No step prints or logs a token; secrets only ever appear as step `env:`
  values.

## 13. Security model

- **This package never guesses a Databricks auth profile.** This
  repository's local `~/.databrickscfg` has profiles literally named
  `dev` and `AZURE_DEV` that resolve to
  `adb-7405604503619901.1.azuredatabricks.net` -- the exact host Lab 8's
  own documentation identifies as **Azure PROD**, not a separate dev
  workspace. `client.get_workspace_client()` raises
  `ProfileNotSpecifiedError` unless a profile (or explicit
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN`) is passed, specifically so nobody
  -- human or automation -- accidentally resolves against Azure PROD by
  relying on an unspecified default.
- Live mutations are restricted by convention (not by a technical guard in
  this code, which cannot itself verify which workspace a given token
  belongs to) to the Personal/academy workspace. **Never Azure PROD.**
- Every Unity Catalog / Volume / Files API path this code touches is
  prefixed by the configured `catalog.schema.volume` (`dbr_dev.
  parvinbadalov.lab09_landing` in `config/dev.yml`); `reset_landing()`
  additionally asserts every path it deletes starts with that exact
  prefix.
- No token, secret, or `.databrickscfg` content is ever printed, logged,
  or committed. `config/dev.yml` contains no credentials.
- GitHub Actions reuses Lab 8's existing `DATABRICKS_PERSONAL_HOST` /
  `DATABRICKS_PERSONAL_TOKEN` variable/secret pair rather than introducing
  new ones; this PR does not create, modify, or rotate any GitHub secret,
  variable, or environment protection rule.

## 14. Known limitations

- **A real bug in `job_cluster` mode was found and fixed while adding
  `serverless_job` mode's tests**: `jobs.py` built `Task.new_cluster` using
  `databricks.sdk.service.jobs.ClusterSpec` -- a different, unrelated
  struct (used for `JobSettings.job_clusters` list entries: it has no
  `node_type_id`/`spark_version`/etc. of its own) than the
  `databricks.sdk.service.compute.ClusterSpec` the `Task.new_cluster`
  field is actually typed as. This went undetected because no earlier
  test inspected the constructed task's `new_cluster` object's own
  fields, only that it was non-`None`. Fixed to use `compute.ClusterSpec`;
  `job_cluster` mode has still never been exercised live either way (see
  below), so this fix itself remains live-unverified too.
- **Phase 0 (the lightweight checks plus the full `--probe-cluster-create`
  live probe) has now been run against the confirmed-safe `personal-yahoo`
  profile** (`https://dbc-1750318a-76a9.cloud.databricks.com`, identity
  `parvinbadalov@yahoo.com`, confirmed via `databricks auth describe` /
  `current-user me` before any mutation) -- see
  `evidence/phase0_2026-09-24.json`. All lightweight checks passed for
  real: `authenticated_identity`, `spark_runtimes_listed`,
  `node_types_listed`, `cluster_policies_listed`, `catalog_access`,
  `schema_access`, `volume_access` (the Lab 9 volume was actually
  created), `files_api_roundtrip` (a real upload/list/delete round trip),
  and `pipeline_list_permission` (22 visible pipelines).
- **`cluster_create_probe` genuinely fails in this workspace** --
  confirmed root cause, not assumed: `TimeoutError: Timed out after
  0:05:00 | caused by BadRequest: Current organization
  7474653929863069 does not have any associated worker environments`.
  This organization has no backend "worker environment" for classic
  compute; `allow-cluster-create` being present in this identity's
  entitlements does not change that, since it is an infrastructure-level
  condition, not a permission. See "Cluster fallback behavior" above for
  the full investigation, including a separate real bug
  (`resolve_lts_spark_version`/`resolve_node_type` ignoring CPU
  architecture) found and fixed along the way, and confirmed live
  afterward to not be the cause of this specific failure. No cluster was
  ever created on the backend by either attempt (confirmed via
  `databricks clusters list` returning zero results both times), so no
  cleanup was required.
- Because of the above, **explicit cluster creation is confirmed
  unsupported specifically for this organization/workspace** -- `run-all`
  should be run with `compute_mode = "serverless_job"` deliberately here
  (recorded in `config/dev.yml`'s `compute.preferred_mode`; see "Cluster
  fallback behavior" for why `job_cluster` is not the right next thing to
  try -- it is still classic compute -- and why the automatic
  exception-driven fallback does not and should not self-select either
  mode for a `TimeoutError`-shaped failure). **`serverless_job` itself has
  not yet been confirmed by an actual successful live job run** -- only
  the classic-compute failure has been confirmed; see "Lab requirement vs.
  Personal workspace reality" for the two open compliance interpretations
  this leaves for the literal "create clusters" task requirement.
- Phase 0 was run under `parvinbadalov@yahoo.com` via the `personal-yahoo`
  profile. This only proves that identity's permissions -- it does **not**
  prove GitHub Actions CI's own identity does, since CI authenticates with
  its own secret-backed token (`DATABRICKS_PERSONAL_TOKEN`).
- **Live-confirmed (2026-09-23): the pipeline's serverless creation path
  works, and `workspace.py`'s FILE-vs-NOTEBOOK upload/glob distinction is
  correct.** A real `run-all` reached `lab09_taxi_pipeline`
  (`pipeline_id=9fcf88d2-8dac-4e2a-91e6-e89407c4fe92`, `serverless: true`),
  and every one of 6 pipeline update attempts reached `runtime_details`
  (Databricks' own event log confirms the `/Workspace/Users/.../lab09/pipeline/**`
  glob resolved `bronze.py`/`silver.py`/`gold.py` as valid pipeline source
  every single time) before failing later, at Unity Catalog table
  creation -- see "Landing schema vs. pipeline output schema" above. The
  classic-fallback branch of `ensure_pipeline`/`upload_pipeline_sources`
  remains unexercised live, since serverless has not been rejected here.
- **Live-confirmed (2026-09-23): `dbr_dev.parvinbadalov` (the landing
  schema, shared with every other lab/demo in this Personal workspace) has
  hit Unity Catalog's per-schema table-count quota**
  (`QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED`, ~100+ existing tables
  against a limit of 100) -- this, not the workspace path, was the actual
  cause of all 6 failed pipeline updates. Fixed non-destructively (no
  existing tables/schemas were touched) by giving Lab 9's pipeline outputs
  their own dedicated schema, `dbr_dev.lab09` (`pipeline.target_schema`),
  via the new `volumes.ensure_output_schema()`; the reconciliation
  notebook now receives that schema (and all four table names) via
  `NotebookTask.base_parameters` instead of relying on its own hardcoded
  defaults. **Retargeting the existing pipeline to `dbr_dev.lab09` was then
  attempted live (2026-09-24) and failed** -- not because of the schema
  quota fix, but because of a separate, genuine Databricks platform
  restriction (a Default-Storage-catalog DLT pipeline's target schema
  cannot be changed via update, only chosen at creation); see "Landing
  schema vs. pipeline output schema" above for the exact error and the
  fix (a new pipeline, `lab09_taxi_pipeline_v2`, targeting `dbr_dev.lab09`
  from creation). **A successful live pipeline update/run against
  `dbr_dev.lab09` has still not yet been confirmed** -- that remains the
  next proposed live step.
- `taxi_zone_lookup.csv`'s schema (`LocationID`, `Borough`, `Zone`,
  `service_zone`) and the specific values for LocationID 264/265 were
  confirmed by downloading the real, current file directly from
  `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` during
  this review, not assumed. What remains unverified is only whether
  `pipeline/silver.py`'s live Spark column expressions (`_join_zone`)
  produce the same result as their pure-Python mirror
  (`src/lab09/zone_lookup.py`) when actually run inside a Lakeflow
  pipeline -- the two are kept in sync by hand, not by a shared runtime
  dependency, and only the pure-Python side has automated test coverage.

## 15. Evidence section

See `evidence/README.md` and `evidence/phase0_2026-09-24.json` -- the
first real live evidence for this lab: a full `preflight
--probe-cluster-create` run against the confirmed-safe `personal-yahoo`
workspace. All lightweight checks (identity, catalog/schema/volume access,
a real Files API round trip, pipeline list permission) passed for real;
the cluster-create probe found a genuine, confirmed backend limitation in
this specific organization (see "Cluster fallback behavior" and "Known
limitations"). No `run-all` evidence exists yet -- that still requires a
separate, explicitly authorized live execution.

## 16. How a supervisor can reproduce the demo

1. Confirm which `~/.databrickscfg` profile (or `DATABRICKS_HOST`/
   `DATABRICKS_TOKEN` pair) points at the intended DEV/academy workspace --
   do not trust a profile's name alone; this repository has profiles named
   `dev` that are not actually a separate dev workspace. `personal-yahoo`
   (`https://dbc-1750318a-76a9.cloud.databricks.com`) has been explicitly
   confirmed safe and is used throughout this section's evidence.
2. `cd labs/lab_09_rest_api_automation && pip install -e . -r requirements-dev.txt`
3. `python -m lab09.cli --profile <confirmed-profile> preflight --probe-cluster-create`
   and review the JSON output, especially `cluster_create_supported` and
   `ci_identity_caveat`. Against `personal-yahoo` specifically, expect
   `cluster_create_supported: false` -- see "Known limitations" for why,
   and use `--compute-mode serverless_job` (or rely on `auto`, which
   already reads `config/dev.yml`'s `compute.preferred_mode:
   serverless_job` for this workspace) rather than `job_cluster`, which is
   still classic compute and expected to fail the same way here.
4. `python -m lab09.cli --profile <confirmed-profile> run-all` and inspect
   `evidence/lab09_report.json`.
5. Re-run `run-all` a second time and confirm the report's `status` is
   `NO_NEW_DATA` only once all configured 2024 months have landed --
   otherwise it lands the next month and reports `SUCCESS`.
6. To watch the manual GitHub Actions path instead: configure the
   `lab09-live-approval` environment's required reviewers once, then
   dispatch `.github/workflows/lab09_api_automation.yml` from the Actions
   tab and approve the pending deployment when prompted.

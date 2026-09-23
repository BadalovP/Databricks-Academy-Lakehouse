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

## 5. API/SDK operations used

| Module | Databricks SDK / REST surface |
|:--|:--|
| `client.py` | `WorkspaceClient` construction (never guesses a profile -- see Security model) |
| `preflight.py` | `current_user.me`, `clusters.spark_versions`, `clusters.list_node_types`, `cluster_policies.list`, `catalogs.get`, `schemas.get`, `volumes.read`, `files.upload`/`list_directory_contents`/`delete`, `pipelines.list_pipelines`, plus the real cluster-create probe below |
| `volumes.py` | `volumes.read`, `volumes.create` |
| `landing.py` | `files.list_directory_contents`, `files.upload`, `files.get_metadata`, `files.delete` |
| `workspace.py` | `workspace.mkdirs`, `workspace.upload` (`ImportFormat.SOURCE`) |
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

1. `authenticated_identity` -- `current_user.me()`
2. `spark_runtimes_listed` -- `clusters.spark_versions()`
3. `node_types_listed` -- `clusters.list_node_types()`
4. `cluster_policies_listed` -- `cluster_policies.list()`
5. `catalog_access` -- `catalogs.get(catalog)`
6. `schema_access` -- `schemas.get(catalog.schema)`
7. `volume_access` -- `volumes.read(...)`; a `NOT_FOUND` here is reported as
   **passed** ("does not exist yet, will be created"), since the volume is
   created later in the execution order, not before preflight
8. `files_api_roundtrip` -- upload/list/delete a tiny probe file under
   `_preflight/`; skipped (not failed) if the volume doesn't exist yet
9. `pipeline_list_permission` -- `pipelines.list_pipelines()`
10. `cluster_create_probe` -- **opt-in only** (`--probe-cluster-create`):
    creates a real tiny single-node cluster
    (`autotermination_minutes=20`), polls it explicitly to `RUNNING`, and
    immediately terminates it in a `finally` block regardless of outcome.
    This is the only check in this list that provisions real compute, and
    it is the only reliable way to confirm create permission --
    `clusters.list_node_types()` succeeding proves nothing about create
    permission on its own.

### Cluster fallback behavior

If the workspace forbids explicit `clusters.create()` (a policy denial, for
example), the architecture's documented fallback is `jobs.submit(...,
new_cluster=...)`: `compute.ClusterSpec.as_new_cluster_dict()` produces the
exact same shape needed for a job task's inline `new_cluster`, and
`jobs.py`'s `_task_settings()` already accepts a `new_cluster` dict as an
alternative to an `existing_cluster_id`. This fallback exists in the code
today; it has not been exercised live because explicit cluster-create has
not yet been tested against a confirmed-safe workspace (see "Known
limitations").

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
`reference/taxi_zone_lookup.csv`. An unmatched ID (including TLC's own
264/265 "Unknown"/"N/A" codes) is **kept**, with `pickup_zone_known` /
`dropoff_zone_known` set to `false` and the borough/zone coalesced to the
literal string `"UNKNOWN"` -- never dropped, and never based on a
hardcoded numeric ID range.

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
- `workflow_dispatch`: `static-checks` -> `approve-live-run` (a
  `lab09-live-approval` GitHub Environment gate; its required-reviewers
  rule must be configured once in GitHub's UI, which this workflow file
  cannot do itself) -> `run-live-automation` (`python -m lab09.cli
  run-all`, using the same `DATABRICKS_PERSONAL_HOST` /
  `DATABRICKS_PERSONAL_TOKEN` variable/secret pair Lab 8 already uses) ->
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

- **No live execution has been performed as part of this PR.** Every
  behavior described above is verified by (a) unit tests against a mocked
  `WorkspaceClient`, and (b) manually cross-checking every SDK call's
  method signature, dataclass fields, and enum values against the actual
  `databricks-sdk` package installed in this repository's local Python
  environment. Neither is a substitute for a real run, and this README
  does not claim one occurred.
- **Phase 0 was not run against a live workspace in this PR**, because the
  locally configured `~/.databrickscfg` profiles named `dev`/`AZURE_DEV`
  resolve to the Azure PROD host, and no profile in that file is
  unambiguously confirmed (by explicit instruction, not just by matching
  naming convention with Lab 8) to be the intended Lab 9 target. The
  `personal-yahoo` profile is the best available candidate (it matches
  Lab 8's established "Personal" workspace host and this config's
  `dbr_dev.parvinbadalov` catalog/schema naming), but per this task's own
  safety instructions ("if host/identity is ambiguous, do not create
  compute"), no cluster was created and no live call of any kind was made.
  **A human needs to explicitly confirm the correct profile and run
  `python -m lab09.cli --profile <confirmed-profile> preflight
  --probe-cluster-create` before this is considered verified.**
- Because of the above, whether **explicit cluster creation is supported**
  in the target workspace is **unknown** -- this is exactly the question
  the (not-yet-run) `--probe-cluster-create` check answers. The
  `jobs.submit(new_cluster=...)` fallback exists in the code but has
  correspondingly not been exercised either.
- Even once Phase 0 is run, it only proves the identity used for that run
  has these permissions -- it does **not** prove GitHub Actions CI's own
  identity does, since CI authenticates with its own secret-backed token.
- The pipeline's serverless-vs-classic fallback logic
  (`pipelines.ensure_pipeline`) has not been exercised against a real
  workspace, so it is unverified whether this specific workspace accepts
  serverless Lakeflow pipelines or requires the classic fallback.
- `taxi_zone_lookup.csv`'s schema (`LocationID`, `Borough`, `Zone`) is
  assumed from the well-documented TLC reference format; it has not been
  independently re-verified column-by-column against a freshly downloaded
  copy in this PR.

## 15. Evidence section

See `evidence/README.md`. As of this PR, no run report exists yet -- there
is nothing to show because nothing has been run live. Live run
evidence should be added in a follow-up once a human has confirmed the
correct DEV/academy profile and dispatched the workflow (or run the CLI
locally).

## 16. How a supervisor can reproduce the demo

1. Confirm which `~/.databrickscfg` profile (or `DATABRICKS_HOST`/
   `DATABRICKS_TOKEN` pair) points at the intended DEV/academy workspace --
   do not trust a profile's name alone; this repository has profiles named
   `dev` that are not actually a separate dev workspace.
2. `cd labs/lab_09_rest_api_automation && pip install -e . -r requirements-dev.txt`
3. `python -m lab09.cli --profile <confirmed-profile> preflight --probe-cluster-create`
   and review the JSON output, especially `cluster_create_supported` and
   `ci_identity_caveat`.
4. `python -m lab09.cli --profile <confirmed-profile> run-all` and inspect
   `evidence/lab09_report.json`.
5. Re-run `run-all` a second time and confirm the report's `status` is
   `NO_NEW_DATA` only once all configured 2024 months have landed --
   otherwise it lands the next month and reports `SUCCESS`.
6. To watch the manual GitHub Actions path instead: configure the
   `lab09-live-approval` environment's required reviewers once, then
   dispatch `.github/workflows/lab09_api_automation.yml` from the Actions
   tab and approve the pending deployment when prompted.

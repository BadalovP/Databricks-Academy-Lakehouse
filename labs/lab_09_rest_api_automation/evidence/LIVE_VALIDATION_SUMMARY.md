# LAB 09 — Live validation summary

This is a sanitized summary of the live validations performed for Lab 9.
**Sections 1–8 cover a confirmed-safe, non-Azure-PROD Personal Databricks
workspace** — the workspace Lab 9 was otherwise built and run against.
**Section 9 is a distinct, later addition and is explicitly the
exception to that scope: it documents a supplementary demonstration run
against Azure PROD itself**, under separate, specific owner
authorization, precisely because the Personal workspace in sections 1–8
cannot support classic compute at all (section 1). Section 9 states that
deviation plainly rather than letting this paragraph's original
non-Azure-PROD framing silently cover it. Throughout, this document
intentionally omits the workspace hostname(s), the authenticated
identity's email address, the Databricks organization ID(s), run/job/
pipeline/cluster UUIDs, and any private URLs — none of that is needed to
understand what was proven. The full, unsanitized evidence (with those
identifiers, for this project's own traceability) exists only in local
JSON files under this `evidence/` directory and is **not** committed to
this repository.

**Scope note:** sections 1–5 below were validated with **separate,
targeted, single API calls** (one pipeline update, one Job run), made
directly through this project's own helper functions rather than a single
`run-all` invocation — that was deliberate, staged validation while the
fixes in sections 2–3 were being found and confirmed. **Section 8
documents a later, separate milestone: a single `python -m lab09.cli
run-all` command succeeding completely end-to-end**, landing a new month
and driving the pipeline and the reconciliation Job itself, with no
per-stage manual intervention. **Section 9 documents a further, separate
milestone in a different workspace (Azure PROD)**, involving one manual
step (a cluster restart performed by the project's operator, not by this
project's code) between two otherwise-automated stages — stated
explicitly in that section rather than folded into "no manual
intervention" language that would only be true of sections 1–8. **Section
10 documents the first successful run of the actual GitHub Actions
`workflow_dispatch` live-automation path** (back on the Personal
workspace), rather than a locally-run CLI invocation. All four kinds of
evidence are kept, clearly labeled, rather than one overwriting
another — see "What this does and does not prove" (section 11) for
exactly what remains open.

## 1. Phase 0: classic compute is confirmed unsupported on this workspace

A full preflight probe (including a real cluster-create/terminate attempt)
was run against the target workspace. Every lightweight check passed
(identity, catalog/schema/volume access, a real Files API round trip,
pipeline-list permission). The explicit cluster-create probe failed with a
backend error indicating this specific Databricks organization has no
"worker environment" provisioned for classic compute — a genuine
workspace/organization-level infrastructure limitation, not a permissions
problem (the identity does hold cluster-create entitlement) and not a
code defect (a separate, unrelated CPU-architecture mismatch bug was found
and fixed during this same investigation, and the corrected spec still
failed identically, ruling it out as the cause).

**Conclusion:** explicit/classic cluster creation is not available on this
workspace. The reconciliation Job instead runs on Databricks-managed
**serverless** job compute (no `existing_cluster_id`, `new_cluster`, or
`job_cluster_key` set on its task).

## 2. Why the original pipeline was replaced

The project's pipeline initially wrote its output tables into the same
Unity Catalog schema used for landing input data. That schema is shared
with many other unrelated projects in this workspace and had already hit
Unity Catalog's per-schema table-count quota, so the pipeline's own output
tables could never be created there. The fix was to give the pipeline
outputs a dedicated schema instead — created idempotently, non-
destructively, with no existing tables, schemas, or data touched.

Retargeting the *existing* pipeline to that new schema was then attempted
and rejected outright by Databricks: a Lakeflow Declarative Pipeline using
the default storage catalog cannot have its target schema changed via an
update, only chosen at creation time. This is a genuine platform
restriction, not a bug in this project's retry/fallback logic (the
fallback logic was independently verified and, separately, hardened so it
never wastes a second API call retrying an error that has nothing to do
with compute type).

**Resolution:** a **new** pipeline (referred to here as "v2") was created,
targeting the dedicated output schema from creation. The original pipeline
("v1") was left **permanently untouched** — confirmed unchanged
before and after every subsequent validation in this document.

## 3. The `timestampNtz` table-creation failure and its fix

The first live update of the new (v2) pipeline progressed further than
any previous attempt — its raw ingestion table was created successfully —
but failed creating the derived quarantine table with a Delta error
stating that the `timestampNtz` table feature must be manually enabled
before a table containing that column type can be created. Exactly one
update was explicitly started for this attempt; Databricks' own pipeline
service then automatically retried the identical failure five more times
on its own before giving up, entirely independent of and uninitiated by
this project's code or operator — this project never started a second
update, and its own tooling never implements or triggers any retry loop
of its own for this kind of failure.

Root cause: the source dataset's raw pickup/dropoff timestamp columns are
inferred by Databricks' Auto Loader as a naive ("no timezone") timestamp
type, and are carried through unmodified into the derived tables. Creating
a plain table via a materialized view does not automatically enable this
Delta table feature the way the raw streaming ingestion table apparently
does.

**Fix:** the affected table definitions now declare
`table_properties={"delta.feature.timestampNtz": "supported"}` using the
officially documented Lakeflow Python API parameter for setting Delta
table properties at creation time — no manual `ALTER TABLE` was ever run.
This is purely declarative: no validity rule, zone-join, timestamp value,
timezone, table name, or reconciliation logic was changed. The one output
table whose actual schema never retains this column type does not declare
the property, since it doesn't need it.

## 4. Successful pipeline update (targeted, single API call)

With the fix applied and the updated source files uploaded, exactly one
pipeline update was started (not a full refresh) and monitored to
completion. It reached the terminal **COMPLETED** state with zero error
events in the pipeline's own event log, and Databricks did not need to
auto-retry it (unlike the earlier, pre-fix failures, which were
auto-retried multiple times by Databricks itself before giving up).

## 5. Successful serverless reconciliation Job (targeted, single run)

A persistent reconciliation Job was created fresh (it did not exist
before), configured with exactly one task pointing at the reconciliation
notebook, running on serverless compute (no cluster reference of any
kind), with notebook parameters overriding the pipeline's actual output
catalog/schema and all four table names. The Job was triggered exactly
once and reached **SUCCESS**.

## 6. All four output tables — names and types

| Table | Type |
|:--|:--|
| `lab09_taxi_bronze` | Streaming table (Auto Loader ingestion) |
| `lab09_taxi_silver` | Materialized view |
| `lab09_taxi_quarantine` | Materialized view |
| `lab09_taxi_daily_summary` | Materialized view |

## 7. Actual row counts and the reconciliation invariant (targeted-validation snapshot, 4 months)

From the reconciliation notebook's own output at the time of the targeted
validation above (four months of real NYC TLC Yellow Taxi trip data
landed so far). **This is a point-in-time snapshot, superseded by the
larger, 5-month dataset in section 8** — both are kept for an accurate
record of what was true at each stage, not as conflicting numbers.

| Metric | Value |
|:--|--:|
| Bronze rows | 13,069,067 |
| Silver (valid) rows | 12,624,925 |
| Quarantine (rejected) rows | 444,142 |
| Gold (daily summary) rows | 27,085 |

**Reconciliation invariant confirmed:**
`bronze_rows == silver_valid_rows + rejected_rows`
→ `13,069,067 == 12,624,925 + 444,142` ✅

Both the bronze and gold row counts are large and non-trivial, confirming
this is real ingested/aggregated data, not empty tables that would satisfy
the invariant vacuously.

Per-rule rejection counts (from the quarantine table's `failed_rules`):

| Rule | Rows |
|:--|--:|
| `INVALID_FARE` | 198,472 |
| `INVALID_DISTANCE` | 261,577 |
| `INVALID_DATETIME_ORDER` | 3,890 |
| `INVALID_MONTH` | 65 |

**Note:** these four counts sum to more than the total rejected-row count
above. This is expected, not an error: a single rejected row's
`failed_rules` array can name more than one violated rule at once (for
example, a row can simultaneously have an invalid fare *and* an invalid
distance), so the same row is counted once per rule it violates.

## 8. Successful single-command `run-all` execution (end-to-end)

**This is the milestone the staged validations in sections 1–5 were
building toward: one single `python -m lab09.cli run-all
--compute-mode serverless_job` command, run exactly once, completed the
entire pipeline start to finish with no manual per-stage intervention.**
Every earlier `run-all` attempt (see `evidence/README.md`'s "Current
status") had failed partway through; this is the first one that didn't.

What the one command did, in order:
- Reused the existing landing Volume and the existing `dbr_dev.lab09`
  output schema (both already existed; created idempotently, nothing
  duplicated).
- Uploaded the current pipeline source files and the reconciliation
  notebook.
- **Reused the existing v2 pipeline** (found by name, not recreated) and
  updated its configuration in place.
- Landed the next missing month, **2024-05** (**62,553,128 bytes**),
  extending the dataset to five real months (January–May 2024) without
  removing or overwriting any previously landed month.
- Started exactly one pipeline update. It reached **COMPLETED**, with
  **zero error events** in the pipeline's own event log and **zero
  automatic retries** by Databricks (unlike the earlier `timestampNtz`
  failure, which needed several) — a clean run on the first attempt.
- **Reused the existing persistent reconciliation Job** (found by name,
  not recreated) and reset its task to point at the current serverless
  configuration — an ordinary, expected part of reusing one persistent
  Job across runs, not a deletion of anything.
- Triggered that Job exactly once. It reached **SUCCESS**.

All four output tables were present and populated afterward:

| Table | Type |
|:--|:--|
| `lab09_taxi_bronze` | Streaming table |
| `lab09_taxi_silver` | Materialized view |
| `lab09_taxi_quarantine` | Materialized view |
| `lab09_taxi_daily_summary` | Materialized view |

Row counts and the reconciliation invariant, now across five months:

| Metric | Value |
|:--|--:|
| Bronze rows | 16,792,900 |
| Silver (valid) rows | 16,241,181 |
| Quarantine (rejected) rows | 551,719 |
| Gold (daily summary) rows | 34,256 |

**Reconciliation invariant confirmed:**
`bronze_rows == silver_valid_rows + rejected_rows`
→ `16,792,900 == 16,241,181 + 551,719` ✅ -- and `reconciliation_passed`
was `true` in the notebook's own output.

Per-rule rejection counts (from the quarantine table's `failed_rules`):

| Rule | Rows |
|:--|--:|
| `INVALID_FARE` | 261,037 |
| `INVALID_DISTANCE` | 311,368 |
| `INVALID_DATETIME_ORDER` | 5,037 |
| `INVALID_MONTH` | 98 |

As before, these four counts sum to more than the total rejected-row
count, since a single rejected row can violate more than one rule at
once — expected, not an error.

The original v1 pipeline was confirmed unchanged both before and after
this execution.

## 9. Classic-compute demonstration in a separate Azure PROD workspace (2026-09-27)

**This section documents a supplementary demonstration run in a different
workspace than sections 1–8 above** — an Azure-hosted workspace this
project's operator already had separate, pre-existing access to (the same
workspace referenced as "Azure PROD" in this project's own README
"Security model" section), not the Personal workspace whose classic-compute
limitation is documented in section 1. This is exactly the "supplementary
run in a separate workspace that does support classic clusters" option
described as still-open in section 11's note below — with one explicit,
important deviation from how that option was originally scoped, called out
below rather than glossed over.

**Authorization basis:** the workspace owner explicitly granted permission
for this specific, scoped test before anything was created. Authentication
used a dedicated OAuth user-to-machine profile created solely for this
test; the operator's existing long-lived Personal Access Token for this
workspace was never accessed, displayed, logged, or used at any point.

**Explicit deviation from `evidence/CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md`
§8:** that document's own test plan was written with an explicit "No Azure
PROD" safeguard, on the premise that any supplementary demonstration would
run only in a confirmed non-production workspace. **This demonstration
did not follow that constraint — it ran in Azure PROD itself**, under
separate, specific, owner-granted authorization for this one test, using
only newly created, uniquely-named/tagged resources, with every
pre-existing resource in that workspace (including two other
long-running personal clusters) explicitly never started, stopped,
modified, or deleted by this project's automation at any point. This is
recorded here plainly so a reviewer can weigh it accurately, not as
something to treat as equivalent to the originally-planned
non-production-only test.

**What was demonstrated, live, using this project's own `compute.py` /
`monitoring.py` functions and no reimplemented logic — including the
manual step in the middle, stated plainly rather than smoothed over:**

1. A real classic (non-serverless) cluster was created via
   `compute.start_cluster_create()` under that workspace's `Personal
   Compute` cluster policy: single-node (`num_workers=0`),
   `Standard_D4ds_v5` node type, `SINGLE_USER` data security mode, a
   dynamically resolved standard (non-Photon, non-aarch64) LTS Spark
   runtime, and a 10-minute `autotermination_minutes` safety net.
   - **A real bug was found and fixed first**: an earlier attempt at this
     same step was rejected outright by the backend
     (`INVALID_PARAMETER_VALUE: Invalid spark version
     18.x-photon-scala2.13`) because `resolve_lts_spark_version()` did not
     exclude Photon runtime variants, which this workspace's cluster
     policy forbids. This was the same class of tie-breaking bug as the
     earlier aarch64 issue (section 1's cross-reference); it is now fixed
     and covered by three new regression tests, all passing (see
     `compute.py` and `tests/test_compute.py`). No cluster object was
     created by that rejected attempt.
2. After the fix, cluster creation succeeded and returned a real cluster
   id. **This project's own automated readiness poll (8-minute bound)
   timed out while the cluster was still `PENDING`** — provisioning a
   `Standard_D4ds_v5` node in this workspace runs on the order of
   5–8.5 minutes based on historical events for the same node type,
   longer than the window used in that attempt. Per this script's own
   design (it always requests termination regardless of whether the
   readiness poll succeeded), it then requested termination of that same
   cluster and **independently confirmed `TERMINATED`** — a real,
   completed create-and-tear-down cycle, but one that never itself
   observed `RUNNING`.
3. **The project's operator then manually restarted that exact same
   cluster (same cluster id) via the Databricks UI**, independently of
   and outside any of this project's own code, and it reached `RUNNING`.
   **This manual step is the reason the create-to-`RUNNING` leg above was
   not achieved by this project's automation alone** — the automated
   attempt in step 2 terminated the cluster before observing `RUNNING`,
   and it was a human, not this project's code, that subsequently started
   it again.
4. This project's automation then independently, read-only confirmed the
   cluster's state (`RUNNING`) and full configuration (matching runtime,
   node type, policy, and the original unique tags — confirming it was
   the identical cluster resource, not a new one).
5. **Only after that manual restart**, in a separate automated run
   against that already-`RUNNING` cluster, this project's own automation:
   uploaded a trivial notebook to a new, uniquely named path; created one
   temporary Job with `existing_cluster_id` pointing at that cluster (no
   `new_cluster`, no additional cluster created); triggered it with
   `run_now()`; polled to completion via `monitoring.poll_job_run()`; and
   retrieved its output, which matched the expected result exactly. It
   then requested termination a second time and independently confirmed
   `TERMINATED` via the same dedicated verification loop described in
   `evidence/CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md` §8 step 11, and
   deleted the temporary Job and notebook it had created.

**Conclusion, stated precisely:** this project's own automation
successfully exercised, live, every Clusters-API and Jobs-API primitive
relevant to the "create clusters" requirement — create, terminate,
attach a Job to a running cluster via `existing_cluster_id`, run and
verify a real notebook execution, terminate again, and independently
verify termination each time. **The one leg not achieved by automation
alone was the create-to-`RUNNING` transition**: the automated attempt
terminated the cluster after its readiness poll timed out in `PENDING`,
and a human manually restarted the same cluster afterward. The Jobs-API
portion (step 5) that followed was fully automated against that
human-started cluster.

## 10. First successful GitHub Actions live workflow execution (2026-09-27)

**Every prior live validation in sections 1–9 was driven by a locally-run
CLI invocation.** This section documents the first time
`.github/workflows/lab09_api_automation.yml`'s `workflow_dispatch` ->
`run-live-automation` path was actually dispatched and completed, rather
than just described.

Before this run, the workflow's live job referenced a `lab09-live-approval`
GitHub Environment that had never actually been created in this
repository (confirmed by a direct, read-only GitHub API query returning
404) — meaning a dispatch would have run the live job with no
required-reviewer gate at all. This was found and fixed first: the job now
references `personal-prod-approval`, an existing environment already
carrying a `required_reviewers` protection rule, already used by
`lab08_cicd.yml` for its own personal-workspace live steps. Pointing a
second workflow at the same environment name has no effect on Lab 8's own
use of it, since GitHub checks environment protection independently per
workflow run.

**Run:** GitHub Actions run
[`36294786218`](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/36294786218),
dispatched from `feature/lab09-rest-api-automation`, triggered by the
repository owner (the same identity configured as `personal-prod-approval`'s
required reviewer) after explicit, separate authorization for this
specific dispatch. Both jobs succeeded: `Static Checks and Tests`
(Ruff/Black/pytest) and `Run Lab 9 Live Automation` (`python -m
lab09.cli run-all`), the latter in 3m49s.

**Actual result, read directly from the run's own uploaded
`lab09_report.json` artifact (not re-typed from memory or reused from an
earlier run):**

| Field | Value |
|:--|--:|
| `status` | `SUCCESS` |
| `month` landed | `2024-06` (`59,859,922` bytes) |
| `compute_mode` | `serverless_job` (`cluster_id: null` — no classic compute of any kind) |
| `bronze_rows` | 20,332,093 |
| `silver_valid_rows` | 19,669,204 |
| `rejected_rows` | 662,889 |
| `gold_rows` | 41,165 |
| `reconciliation_passed` | `true` |

**Reconciliation invariant confirmed:** `bronze_rows == silver_valid_rows + rejected_rows`
-> `20,332,093 == 19,669,204 + 662,889` ✅

Per-rule rejection counts: `INVALID_FARE` 324,080; `INVALID_DISTANCE`
364,322; `INVALID_DATETIME_ORDER` 6,177; `INVALID_MONTH` 149 (these sum to
more than the total rejected-row count, expected, since one row can
violate more than one rule).

The pipeline and persistent Job were both reused, not recreated
(`ensure_pipeline()`/`ensure_job()`'s tested find-or-create logic, exactly
as in every prior run). GP1, GP2, and the Azure PROD test cluster from
section 9 are unrelated to this Personal-workspace run and were confirmed
unaffected by a separate, independent read-only check immediately
afterward.

**What this proves that sections 1–9 did not:** the actual GitHub Actions
CI/CD path a grader or CI system would use — not just this project's own
local CLI — has now been exercised live, successfully, end to end.

## 11. What this does and does not prove

**Proven, live:**
- Phase 0 capability checks and the classic-compute limitation on this
  project's Personal workspace.
- Every individual stage (sections 1–5), targeted and separate.
- **A single `run-all` command succeeding completely end-to-end** on
  serverless compute (section 8) — landing a new month, updating the
  pipeline, and running the reconciliation Job, all from one invocation,
  all four tables populated, the reconciliation invariant holding against
  real, growing data.
- **The same, driven by GitHub Actions itself, not a local CLI invocation**
  (section 10) — landing a further new month via the actual
  `workflow_dispatch` live-automation path, first attempt.
- **Classic cluster create and terminate (automated), and — against a
  cluster a human subsequently, manually restarted — Job attachment via
  `existing_cluster_id`, notebook execution, and confirmed termination
  (fully automated)** (section 9) — demonstrated live in a separate Azure
  PROD workspace, under explicit owner authorization, with the explicit
  caveat that this deviates from this project's own previously-stated "no
  Azure PROD" safeguard for that supplementary test. That deviation, and
  the fact that reaching `RUNNING` required a manual restart rather than
  being achieved by the automated poll itself, are what remain for a
  reviewer/mentor to weigh — not the underlying code's correctness, which
  section 9 shows working against a real backend in both the automated
  and human-assisted portions.

**Not yet proven / open for reviewer judgment:**

- Whether a classic-cluster demonstration run in Azure PROD — rather than
  the confirmed non-production workspace originally envisioned in
  `evidence/CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md` §8 — satisfies the
  literal "create clusters" portion of the Lab 9 task requirement, or
  whether Option A (accepting the serverless `run-all` proof as
  sufficient) remains preferable regardless. This project does not
  resolve that question on its own — see the main README's "Lab
  requirement vs. Personal workspace reality" section and
  `evidence/CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md` §9 for the full,
  honest framing. **This document does not claim the deviation from the
  "no Azure PROD" safeguard was itself pre-approved by that document —
  only that a separate, specific, owner-granted authorization for this
  exact test existed before it ran.**
- Whether a create-to-`RUNNING` cycle completed by automation alone, with
  no manual restart in the middle, is required to fully satisfy the
  requirement, given that section 9's automated attempt did complete a
  real create-and-terminate cycle but did not itself observe `RUNNING`
  before a human intervened.

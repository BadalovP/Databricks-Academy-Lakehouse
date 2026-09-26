# LAB 09 — Live validation summary

This is a sanitized summary of the live validations performed against a
confirmed-safe, non-Azure-PROD Personal Databricks workspace. It
intentionally omits the workspace hostname, the authenticated identity's
email address, the Databricks organization ID, run/job/pipeline UUIDs, and
any private URLs — none of that is needed to understand what was proven.
The full, unsanitized evidence (with those identifiers, for this
project's own traceability) exists only in local JSON files under this
`evidence/` directory and is **not** committed to this repository.

**Scope note:** sections 1–5 below were validated with **separate,
targeted, single API calls** (one pipeline update, one Job run), made
directly through this project's own helper functions rather than a single
`run-all` invocation — that was deliberate, staged validation while the
fixes in sections 2–3 were being found and confirmed. **Section 8
documents a later, separate milestone: a single `python -m lab09.cli
run-all` command succeeding completely end-to-end**, landing a new month
and driving the pipeline and the reconciliation Job itself, with no
per-stage manual intervention. Both kinds of evidence are kept, clearly
labeled, rather than one overwriting the other — see "What this does and
does not prove" at the end of this document for exactly what remains
open.

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

## 9. What this does and does not prove

**Proven, live:**
- Phase 0 capability checks and the classic-compute limitation.
- Every individual stage (sections 1–5), targeted and separate.
- **A single `run-all` command succeeding completely end-to-end** on
  serverless compute (section 8) — landing a new month, updating the
  pipeline, and running the reconciliation Job, all from one invocation,
  all four tables populated, the reconciliation invariant holding against
  real, growing data.

**Not yet proven:**
- The literal "create clusters" portion of the Lab 9 task requirement.
  This workspace has no classic-compute worker environment, and no
  classic cluster has been created here at any point in this project, in
  any of the validations above. Whether that requirement is satisfied by
  proving everything else on serverless compute, or requires a
  supplementary run in a separate workspace that does support classic
  clusters, remains an open decision for a reviewer/mentor — see the main
  README's "Lab requirement vs. Personal workspace reality" section. **This
  document does not claim that requirement is fulfilled.**

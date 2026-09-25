# LAB 09 — Live validation summary

This is a sanitized summary of the live validations performed against a
confirmed-safe, non-Azure-PROD Personal Databricks workspace. It
intentionally omits the workspace hostname, the authenticated identity's
email address, the Databricks organization ID, run/job/pipeline UUIDs, and
any private URLs — none of that is needed to understand what was proven.
The full, unsanitized evidence (with those identifiers, for this
project's own traceability) exists only in local JSON files under this
`evidence/` directory and is **not** committed to this repository.

**Scope note:** every stage below was validated with a **separate,
targeted, single API call** (one pipeline update, one Job run), not by a
single `run-all` invocation. **A full `run-all` execution has not yet
succeeded end-to-end since the fixes below were applied** — see
"What this does and does not prove" at the end of this document.

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

## 7. Actual row counts and the reconciliation invariant

From the reconciliation notebook's own output, for the months landed so
far (four months of real NYC TLC Yellow Taxi trip data):

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

## 8. What this does and does not prove

**Proven, live, individually:**
- Phase 0 capability checks and the classic-compute limitation.
- The v2 pipeline can be created, updated, and reach `COMPLETED` with all
  four tables created and populated with real data.
- The serverless reconciliation Job can be created, run, and succeed,
  reading those tables and confirming the reconciliation invariant.

**Not yet proven:**
- A single `run-all` invocation succeeding end-to-end, incorporating the
  `timestampNtz` fix, in one continuous execution. The most recent
  `run-all` attempt (before this fix existed) failed at the pipeline
  step; the successful pipeline update and successful Job run documented
  above were each triggered as separate, targeted, single API calls
  using this project's own helper functions — not by `run-all` itself.
  Re-running `run-all` end-to-end against this now-fixed configuration is
  the natural next validation step, not yet performed.
- The literal "create clusters" portion of the Lab 9 task requirement.
  This workspace has no classic-compute worker environment, and no
  classic cluster has been created here at any point in this project.
  Whether that requirement is satisfied by proving everything else on
  serverless compute, or requires a supplementary run in a separate
  workspace that does support classic clusters, remains an open decision
  for a reviewer/mentor — see the main README's "Lab requirement vs.
  Personal workspace reality" section.

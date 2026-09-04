# Demo2 Olist Lakehouse: 15-Minute Presentation Guide

## Audience and objectives

**Audience:** data engineers, analytics engineers, and Databricks practitioners.

By the end, the audience should understand how this project builds a governed Olist lakehouse, why the large Job preserves parallel work while enforcing validation gates, and how bundle deployment connects pipeline outputs to a refreshed dashboard.

## Timing and speaker script

### 0:00-1:00 - Problem

"Olist gives us realistic e-commerce data, but raw files do not provide trustworthy business metrics. The problem is to build repeatable order and customer outputs while detecting bad input, broken keys, reconciliation drift, and stale dashboard data. This project makes those checks part of one development workflow."

### 1:00-2:30 - Architecture

"The source enters through setup and landing validation. Bronze separates reference data from Auto Loader ingestion. Silver applies quality, deduplication, business transformations, and customer history logic. Gold produces dimensions and the order-item fact. Reconciliation and governance then establish whether downstream consumers can trust the data."

Show the data-flow diagram in the README. Point out that the Olist Test pipeline is a downstream declarative consumer of the Gold fact table, not a second copy of ingestion.

### 2:30-4:30 - Main Job DAG

"The first part of the Job is intentionally sequential where each step consumes the previous table. Bronze reference and Auto Loader work can run in parallel, so the DAG joins them at Bronze validation. After Gold reconciliation, governance, alerts, DQX, tests, and the parallel-learning checks fan out."

Show the complete DAG. Explain that `final_validation` depends on dashboard validation, alert validation, DQX, automated tests, the parallel-learning summary, and the dashboard refresh. A successful final task therefore means every required branch succeeded.

### 4:30-6:00 - Declarative pipeline

"The existing Olist Test pipeline is submitted as one `pipeline_task` using its real pipeline ID. Its Python and SQL sources create the learning base, status aggregation, KPI summary, and quality status materialized views. The Job waits for that update, then runs a Spark Python validator against the published tables."

Mention that the validator uses `parse_known_args()` because Databricks can inject arguments such as `-f`.

### 6:00-7:30 - Expectations and quality

"An `expect` records a violation and keeps the row. `expect_or_drop` records it and removes the row. `expect_or_fail` stops the update. The Olist base view uses `expect_or_fail` for a null order ID because an unidentified order cannot safely participate in downstream metrics. DQX adds auditable quality outcomes and quarantine information."

### 7:30-9:00 - Testing

"Testing happens at several levels. Local unit tests protect utilities and rule definitions. Integration tests query published outputs. Reconciliation checks protect known business totals. Runtime notebook tasks cover governance, alerts, dashboard readiness, and final validation. The output validator checks counts, monetary totals within one cent, and PASS quality status."

### 9:00-10:30 - Dashboard and Genie

"The dashboard is refreshed only after the Olist Test pipeline and output validator succeed. Its KPIs show order-item volume, distinct orders, price, freight, total value, status distribution, and quality status. Genie can answer governed questions over the Gold and learning tables after the refresh."

### 10:30-12:00 - Bundle isolation and deployment

"This repository contains many demos and labs, so this project uses an isolated bundle named `demo2-olist-end-to-end`. Its summary must show exactly two Jobs: the preserved small validation Job and the new comprehensive Job. Deployment and execution happen from `Demos/Demo2_Olist`, targeting `azure_dev`; the root bundle is never used for this project."

Show the exact commands:

```powershell
databricks bundle validate -t azure_dev
databricks bundle summary -t azure_dev
databricks bundle deploy -t azure_dev
databricks bundle run -t azure_dev demo2_olist_pipeline
```

### 12:00-14:00 - Live demo

1. Open the isolated bundle and run `databricks bundle summary -t azure_dev`.
2. Open the large Job and show the two Bronze branches and the post-Gold fan-out.
3. Show the Olist Test pipeline task, output validation parameters, and dashboard refresh dependency.
4. Open completed Run `107916031830226` and show the final task statuses and validation metrics.
5. Open the dashboard and point out total value, order count, status distribution, and PASS quality status.

The expected values are 112650 order items, 98666 distinct orders, 13591643.70 total price, 2251909.54 freight, 15843553.24 total value, 7 status rows, and `PASS`.

### 14:00-15:00 - Close

"The important design choice is that correctness is part of orchestration. The Job does not refresh the dashboard merely because a pipeline finished; it refreshes only after published outputs match the contract and every required branch succeeds. The isolated bundle makes that workflow deployable without taking ownership of unrelated academy resources."

## Backup demo plan

If Databricks is slow, use the saved bundle summary, YAML DAG, validator source, and recorded evidence in `docs/DEPLOYMENT_EVIDENCE.md`. Walk through the dependency graph and explain the expected metrics without claiming a new run. Show the dashboard only if it loads; do not invent screenshots or runtime results.

## Likely technical questions

**Why is the Olist Test pipeline not split into Job tasks?** Its transformations and monitoring files execute inside Lakeflow, so the correct Job boundary is one pipeline task.

**Why not run all tasks sequentially?** Independent Bronze and post-Gold checks can run concurrently, reducing wait time while dependencies still protect data readiness.

**What prevents a dashboard refresh after bad output?** The refresh depends on `validate_pipeline_outputs`, whose assertions fail the task and block all downstream tasks.

**Why use `expect_or_fail` for order IDs?** The key is required to identify and aggregate orders; publishing rows without it would make downstream metrics ambiguous.

**How are existing resources protected?** The isolated bundle includes both Job resources, reuses the existing pipeline, dashboard, warehouse, and cluster, and is deployed only to `azure_dev`.

**What are the main limitations?** Some notebook sources still contain development-specific table references, and the expected metrics are fixed to the current development dataset. A future version can centralize those contracts and parameterize every source.

## Final summary

This is a complete, testable Olist lakehouse workflow: layered transformations, declarative downstream views, parallel validation, strict output checks, governed dashboard refresh, and a final success gate. The deployment evidence document records what was actually validated and run.

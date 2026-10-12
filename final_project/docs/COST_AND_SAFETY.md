# UrbanFlow Phase 2 cost and safety plan

> **Historical record.** The component Jobs named below (`urbanflow_*_test`) were retired on
> 2026-10-03 after the unified `[azure] UrbanFlow End-to-End` Job superseded them. Their
> definitions live, undeployed, in `resources/retired/component_jobs.yml`, and their workspace
> definitions and run histories in `evidence/2026-10-03_retired_component_jobs.json`. Commands
> below that deploy or run them describe what was done at the time; current runs go through the
> unified Job - see [UNIFIED_RELEASE.md](UNIFIED_RELEASE.md).

[← Main README](../README.md) · [Resource inventory](RESOURCE_INVENTORY.md)

## Current safety state

The first live milestone is complete: 2,520 events were published once and reconciled exactly into
`dbr_dev.parvinbadalov_urbanflow.bronze_station_status`. Phase 2 development since that run has been
local or read-only. No additional Event Hubs message, Databricks table write, Job deployment,
Lakeflow execution, serverless session, or cluster lifecycle action occurred.

## Implemented guardrails

- The new Job has no schedule, defaults `run_transform=false`, and contains two dependent notebook
  tasks under one 1,200-second outer timeout.
- Both tasks use `${var.compute_cluster_id}` through `existing_cluster_id`; no new cluster is
  declared.
- Each notebook exits before any Spark table action unless `run_transform=true` is supplied.
- Both notebooks verify the actual cluster ID against the GP1/GP2 run allowlist.
- The source is fixed to completed execution `urbanflow-20260929T195132Z-r3`; the Job neither calls
  Event Hubs nor runs Structured Streaming.
- Silver writes are gated by `Bronze = Silver + Quarantine + Duplicates` and unique event IDs.
- Gold writes are gated by fact-to-Silver and aggregate-to-fact reconciliation.
- Delta MERGE validates identifiers, rejects duplicate source keys, and uses stable grains.
- JSON evidence contains counts and limitations, not raw messages, tokens, or connection strings.
- GP1 and GP2 remain hard-coded against termination; Phase 2 invokes no cluster mutation API.
- Static GitHub Actions has no Azure login, bundle deploy, Job run, or pipeline step.

## Prepared consolidated approval request

This request is ready to present only after a fresh read-only preflight finds GP1 or GP2 already
`RUNNING`. Approval covers the following actions as one bounded Phase 2 execution:

1. Recheck the current Databricks identity, workspace host, Bronze table metadata, and GP1/GP2
   state without changing them.
2. Select GP1 (`0702-132442-toro5spu`) when it is already `RUNNING`; otherwise select GP2
   (`0702-171207-xo9bbc0y`) only when it is already `RUNNING`. Stop if neither qualifies.
3. Validate and deploy only DAB resource `urbanflow_silver_gold_test` to the Azure target, pinned to
   the selected existing cluster. Do not deploy or run Lakeflow.
4. Run that one unscheduled Job once with `run_transform=true` and
   `source_execution_id=urbanflow-20260929T195132Z-r3`.
5. Task `04_bronze_to_silver.py` reads the existing 2,520-row Bronze slice, validates and
   reconciles it, then MERGEs `silver_station_status`, `quarantine_station_status`, and
   `duplicate_station_status`.
6. After Silver succeeds, task `05_silver_to_gold.py` builds and reconciles the stable availability
   fact, a clearly named 40-row development station dimension, the daily summary, shortage table,
   and rebalancing-priority table, then MERGEs them.
7. Both tasks write small execution-specific JSON reports under the existing managed Volume.
8. Read back counts and evidence, rerun no task, and verify the selected shared cluster's state and
   configuration are unchanged. Never stop or terminate it.

## Explicit exclusions

The approval does not include:

- another GBFS producer call or any Event Hubs publish;
- Kafka consumption or a Structured Streaming query;
- historical archive download or Auto Loader execution;
- Lakeflow deployment or execution;
- serverless compute;
- a new cluster, cluster restart, resize, library change, or termination;
- changes outside `dbr_dev.parvinbadalov_urbanflow` and its existing managed Volume;
- dashboard, alert, governance, or unrelated Lab/Demo changes.

## Expected duration and cost boundary

The two bounded batch tasks target completion within ten minutes and the Job has a 20-minute outer
timeout. The existing USD 2 operator stop threshold remains a planning guard, not a guaranteed
price. Azure Databricks charges depend on VM and DBU rates, agreement, workload, instance, region,
and currency. See [Azure Databricks pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/).

## Success evidence

- The Job and both tasks finish `SUCCESS` within the bound.
- Source Bronze count remains 2,520 for the verified execution.
- Silver reconciliation is `PASS`; accepted, quarantined, and duplicate counts add to 2,520.
- Gold reconciliation is `PASS`; fact count equals Silver and daily observation totals equal fact.
- A second run is not required. MERGE before/after counts and stable-key tests provide the planned
  rerun proof; a later approved rerun should insert zero duplicate rows.
- The Gold report states that availability contains one real snapshot and station reference is the
  committed 40-row development sample.
- The shared cluster has the same ID, runtime, access mode, and state before and after.

## Stop conditions

Stop before deployment or execution when the host, identity, source execution, Bronze count,
cluster ID, runtime, access mode, permissions, or state differs from the verified contract. During
the run, stop the Job task if reconciliation fails or the 20-minute bound is reached. Do not repair
Phase 2 by republishing Event Hubs data, deleting checkpoints, clearing tables, starting another
compute target, or modifying shared cluster configuration.

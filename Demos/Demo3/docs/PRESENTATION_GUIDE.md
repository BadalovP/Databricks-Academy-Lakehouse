# UrbanFlow 20–25 minute presentation guide

[← Main README](../README.md)

This first-milestone presentation is honest about the boundary between working local code,
read-only discovery, and future Azure execution.

## 0–2 minutes — Business problem

- Start with the customer impact of empty and full stations.
- State the two demonstration thresholds: bikes ≤ 2 and docks ≤ 2.
- Say that these are transparent starting rules, not industry standards.

**Suggested wording:** “UrbanFlow helps an operations team see where rentals or returns may fail.
The first milestone builds a trustworthy event and quality foundation before attempting prediction.”

## 2–5 minutes — Real data and identifier discovery

- Open the README source table.
- Show that GBFS endpoints are discovered from the official feed.
- Open the sample metadata and historical CSV header.
- Explain the UUID versus `short_name` finding.

**Suggested wording:** “The historical ID is not the current UUID. I verified that it maps to
GBFS `short_name` in the selected sample and preserved both identifiers.”

## 5–9 minutes — First educational notebook

- Open `notebooks/01_fundamentals.py`.
- Point out that every code cell has a structured explanation.
- Walk through explicit schemas, operational filtering, the station join, thresholds, and
  `groupBy` KPIs.
- Show the dry-run Delta MERGE and SQL dashboard query.

**Do not claim:** that the Delta table or dashboard already exists in Azure.

## 9–13 minutes — Streaming design

- Show the Event Hubs Mermaid sequence.
- Explain topic/hub, partition, offset, consumer group, checkpoint, and micro-batch.
- Open `producer.py` and show bounded polls, TTL, stable event IDs, and duplicate suppression.
- Open `notebooks/03_streaming.py` and show the `run_stream=false` gate and `availableNow` trigger.

**Suggested wording:** “The default path is a dry run. The live path is finite, checkpointed, and
requires both a managed secret and explicit approval.”

## 13–16 minutes — Data quality and SCD

- Show named quality rules and the quarantine routing test.
- Explain Bronze retention, Silver trust, Quarantine evidence, and reconciliation.
- Show the synthetic capacity-change SCD2 test and label it as synthetic.

## 16–19 minutes — Azure discovery and safety

- Open `RESOURCE_INVENTORY.md`.
- Show the verified workspace identity, existing identity-owned Event Hub, Kafka capability,
  Key Vault secret metadata, storage, and compute policies.
- Show the GP1/GP2 table: both are technically compatible, GP1 is preferred, and the latest
  read-only check found GP1 `RUNNING`; UrbanFlow did not start it.
- Emphasize that discovery was read-only and other students' resources are excluded.

## 19–22 minutes — Testing and CI/CD

- Run or show `pytest`, Ruff, and Black results.
- Open `.github/workflows/demo3_urbanflow.yml` and show that it has no login/deploy/run step.
- Explain DAB target configuration and why resource deployment is deferred.

## 22–25 minutes — Phase 2 live milestone

- Open `COST_AND_SAFETY.md`.
- Propose one two-task existing-cluster Job that reuses the verified 2,520-row Bronze execution,
  writes reconciled Silver and Gold tables, and never contacts Event Hubs or Lakeflow.
- State the exact reconciliation and cluster-unchanged evidence needed for `validated live`.

## First-milestone files to show

1. `README.md` — architecture and current status.
2. `notebooks/01_fundamentals.py` — teaching quality and business result.
3. `producer.py` and `streaming.py` — real streaming mechanics.
4. `tests/` — proof that core behavior runs offline.
5. `RESOURCE_INVENTORY.md` — real Azure discovery.
6. `LABS_1_TO_9_COVERAGE.md` — accurate progress, including pending work.

## Questions to prepare for

- **Why not run continuously?** A bounded academy demonstration controls cost and prevents an
  unattended stream. Continuous operation would require production monitoring and budget approval.
- **Are these live trips?** No. GBFS provides station availability. Historical files provide real
  trips. UrbanFlow never invents live individual ride transactions.
- **Why Event Hubs if the API is already available?** Event Hubs decouples polling from Spark,
  buffers observations, and demonstrates Kafka-compatible streaming and replay.
- **How do retries avoid duplicates?** Stable event IDs, Kafka offsets/checkpoints, deterministic
  deduplication, and Delta MERGE.
- **What is proven today?** Public contracts, real samples, offline business logic, safety code,
  tests, and resource visibility. Cloud writes and executions remain pending approval.

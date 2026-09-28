# UrbanFlow cost and safety plan

[← Main README](../README.md) · [Resource inventory](RESOURCE_INVENTORY.md)

## Current safety state

All completed activity is local or read-only. No Event Hubs message was sent, no Databricks table
or Volume was written, no Job/pipeline was created, and no compute was started.

## Guardrails already implemented

- Public samples are small and attributed; the 369 MB historical ZIP is never downloaded in full.
- The producer is finite, respects the 60-second GBFS TTL, and requires `--confirm-publish`.
- The notebook stream defaults to `run_stream=false` and uses `availableNow`, not an endless trigger.
- Target host equality is checked before SDK actions.
- Secrets are loaded only at runtime and never logged.
- PR/push CI has no Azure login or live job.
- Checkpoint and schema paths are UrbanFlow-specific.
- GP1 and GP2 are protected from termination in code; cleanup stops only UrbanFlow queries.
- Shared-compute preflight requires the selected cluster to already be `RUNNING` and never invokes
  a start, restart, resize, edit, library, or termination API.

## Combined approval request for the first live test

This is the single request to present before any billable action.

### Existing resources to reuse

- Azure Databricks workspace `dbr_dev` at the verified host.
- Unity Catalog `dbr_dev` and the identity-owned ADLS external location.
- Event Hubs Standard namespace `evhpl24databricks`.
- Existing Event Hub `parvinbadalov_evh`, policy `parvinbadalov_policy`, and consumer group
  `parvinbadalov`.
- Key Vault-backed Databricks scope `azure-secrets`, secret name
  `parvinbadalov-eventhub-cs`.
- GP1 (`0702-132442-toro5spu`) if it is already running; otherwise GP2
  (`0702-171207-xo9bbc0y`) only if it is already running.

### Proposed new resources

- Schema `dbr_dev.parvinbadalov_urbanflow`.
- Volume `dbr_dev.parvinbadalov_urbanflow.urbanflow_landing` or an approved equivalent isolated
  subpath.
- Bronze table `bronze_station_status` and its dedicated checkpoint directory.

No new workspace, storage account, Key Vault, Event Hubs namespace, SQL warehouse, or cluster is
required for this first test. The DAB Job and Lakeflow definitions remain local and undeployed.

### Code to execute

1. Publish one GBFS poll to the existing Event Hub, at most roughly the current station count.
2. Run the read-only shared-compute preflight. Stop if neither GP1 nor GP2 is already `RUNNING`.
3. Run the guarded streaming notebook once with `availableNow` on the selected existing cluster.
4. Validate explicit schema, Kafka partition/offset metadata, event-ID uniqueness, source and
   collection timestamps, and input/output reconciliation.
5. Stop the query, verify it is inactive, and confirm the shared cluster was not modified.

### Expected duration

- Producer: usually under one minute for one poll.
- Databricks startup: zero by policy; a terminated cluster blocks the test.
- Bounded stream and validation: target under five minutes after startup.
- Total requested window: up to 20 minutes, followed by query and cluster-state verification.

### Approximate cost

The Event Hubs namespace already exists, so the incremental message volume is negligible relative
to its standing namespace cost. Databricks compute is the main incremental cost. Exact DBU and VM
prices depend on the academy contract and current Azure pricing. For approval planning, retain the
conservative **USD 2 maximum incremental budget** for at most 20 minutes on an already-running
shared cluster; this is a ceiling, not a price quote. Recalculate it in the
[Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/) after the exact mode is
selected. Limit cost with one bounded run and immediate query cleanup. UrbanFlow does not change
the shared clusters' existing auto-termination settings. Stop rather than extend the run if the
20-minute window is reached.

### Required permissions

- `USE CATALOG`, `CREATE SCHEMA` or use of an approved existing isolated schema.
- `CREATE VOLUME`, `CREATE TABLE`, and write permission only in the UrbanFlow boundary.
- Read permission on the named Databricks secret scope/key.
- Send/Listen rights on `parvinbadalov_evh` and use of the named consumer group.
- `CAN ATTACH TO` or higher on the selected already-running cluster; read-only inspection confirmed
  effective `CAN MANAGE`, but UrbanFlow deliberately does not use lifecycle permissions.

### Cleanup

- Stop the streaming query and confirm it is inactive.
- Do not stop, terminate, restart, resize, or edit GP1/GP2; record its state before and after.
- Leave the small Bronze table/checkpoint only if approved for the next milestone.
- If cleanup is requested, delete only explicitly identified `urbanflow` objects after a separate
  destructive-action review. Never touch other Event Hubs, checkpoints, schemas, Labs, GP1, or GP2.

### Success evidence

- Producer report with count only, no credential or message payload dump.
- Bronze count, distinct event-ID count, partition/offset range, timestamp completeness, and
  reconciliation result.
- Screenshot of the bounded query or Job task after success.
- Before/after evidence showing the shared cluster configuration was unchanged and no lifecycle API
  was called.
- Dated entry under `docs/evidence/` and coverage-matrix status updated to `validated live`.

## Stop conditions

Stop immediately if the resolved host or identity differs, neither GP1 nor GP2 is already running,
the cluster metadata differs from the pinned contract, the named resource is not isolated, secret
access is broader than expected, a continuous query remains active, or the checkpoint points
outside the UrbanFlow path.

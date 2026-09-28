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
- Temporary cluster cleanup succeeds only after exact `TERMINATED` is observed.

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

### Proposed new resources

- Schema `dbr_dev.parvinbadalov_urbanflow`.
- Volume `dbr_dev.parvinbadalov_urbanflow.urbanflow_landing` or an approved equivalent isolated
  subpath.
- Bronze table `bronze_station_status` and its dedicated checkpoint directory.

No new workspace, storage account, Key Vault, Event Hubs namespace, SQL warehouse, Job, pipeline,
or permanent cluster is required for this first test.

### Code to execute

1. Publish one GBFS poll to the existing Event Hub, at most roughly the current station count.
2. Run the guarded streaming notebook once with `availableNow`.
3. Validate explicit schema, Kafka partition/offset metadata, event-ID uniqueness, source and
   collection timestamps, and input/output reconciliation.
4. Stop and independently verify compute termination.

### Expected duration

- Producer: usually under one minute for one poll.
- Databricks startup: environment-dependent, commonly several minutes.
- Bounded stream and validation: target under five minutes after startup.
- Total requested window: up to 20 minutes, followed by termination verification.

### Approximate cost

The Event Hubs namespace already exists, so the incremental message volume is negligible relative
to its standing namespace cost. Databricks compute is the main incremental cost. Exact DBU and VM
prices depend on the approved compute mode, contract, and current Azure pricing. For approval
planning, use a conservative **USD 2 maximum incremental budget** for one 20-minute run on the
smallest permitted compute; this is a budget ceiling, not a price quote. Recalculate it in the
[Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/) after the exact mode is
selected. Limit cost with one bounded run, a 10-minute auto-termination safety net where supported,
and immediate manual/API cleanup verification. Stop rather than extend the run if the 20-minute
window is reached.

### Required permissions

- `USE CATALOG`, `CREATE SCHEMA` or use of an approved existing isolated schema.
- `CREATE VOLUME`, `CREATE TABLE`, and write permission only in the UrbanFlow boundary.
- Read permission on the named Databricks secret scope/key.
- Send/Listen rights on `parvinbadalov_evh` and use of the named consumer group.
- Permission to use the selected compute policy.

### Cleanup

- Stop the streaming query and confirm it is inactive.
- Terminate temporary compute and poll until exact `TERMINATED`.
- Leave the small Bronze table/checkpoint only if approved for the next milestone.
- If cleanup is requested, delete only explicitly identified `urbanflow` objects after a separate
  destructive-action review. Never touch other Event Hubs, checkpoints, schemas, Labs, GP1, or GP2.

### Success evidence

- Producer report with count only, no credential or message payload dump.
- Bronze count, distinct event-ID count, partition/offset range, timestamp completeness, and
  reconciliation result.
- Screenshot of the bounded query or Job task after success.
- Independent compute state showing `TERMINATED`.
- Dated entry under `docs/evidence/` and coverage-matrix status updated to `validated live`.

## Stop conditions

Stop immediately if the resolved host or identity differs, the named resource is not isolated,
secret access is broader than expected, a continuous query remains active, the checkpoint points
outside the UrbanFlow path, or compute termination cannot be confirmed.

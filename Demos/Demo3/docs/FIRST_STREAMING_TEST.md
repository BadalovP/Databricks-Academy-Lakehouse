# First bounded streaming test - prepared, not executed

[Back to README](../README.md) · [Cost and safety](COST_AND_SAFETY.md)

## Current readiness

Refreshed by read-only checks on **2026-09-29**. The compute precondition is now met for the
first time, and a hard code blocker found the same day has been fixed.

- GP1 `0702-132442-toro5spu` is **`RUNNING`**, started by the project operator. Cluster ID, DBR
  `17.3.x-scala2.13`, `USER_ISOLATION` access mode and `Standard_F4` nodes were all re-verified.
  Its 60-minute auto-termination is the practical deadline for the run.
- GP2 `0702-171207-xo9bbc0y` is `TERMINATED` and is not needed while GP1 is running.
- **A blocker was found and fixed before proposing any run.** `parse_station_events` selected
  `"kafka_*"`, and Spark expands only `*` and `<struct>.*`, so a prefix pattern is read as a literal
  column name. Reproduced locally against real pyspark: `AnalysisException:
  [UNRESOLVED_COLUMN.WITH_SUGGESTION] ... kafka_* cannot be resolved`. Analysis fails before any row
  moves, so a live run started before this fix would have consumed shared compute and produced
  nothing. The Kafka lineage columns are now named explicitly and a regression test analyses the
  query plan, verified to fail if the glob returns.
- **No UrbanFlow Job exists in either workspace yet**, so the sequence below must deploy the bundle
  before it can run anything. This step was missing from the earlier version of this plan.
- **Event Hubs retention is one hour, not one day.** The hub's authoritative
  `retentionDescription.retentionTimeInHours` is `1`; the legacy `messageRetentionInDays` field
  reports `1` only because it cannot express sub-day values, and an earlier version of this plan
  repeated that wrong figure. Steps 4 to 6 below must therefore complete inside the same hour, or
  our own published events expire before the consumer reads them and reconciliation fails on a
  count mismatch. The upside is that foreign-message exposure is capped at one hour of data.
- The second workspace `dbr_dev_trial` is **deliberately not used for this test**. It shares the
  Unity Catalog metastore and can read this project's data, but Databricks secret scopes are
  workspace-local and `azure-secrets` does not exist there, so the Event Hubs connection string is
  unreachable. See [RESOURCE_INVENTORY.md](RESOURCE_INVENTORY.md#two-workspace-topology).
- Both use DBR `17.3.x-scala2.13`, standard `USER_ISOLATION`, and are compatible with Unity
  Catalog and Kafka Structured Streaming. The identity has effective attach permission.
- `dbr_dev.parvinbadalov_urbanflow` and its `urbanflow_landing` Volume do not exist yet.
- The intended Key Vault-backed scope and secret metadata exist. Secret-value access is unproven.
- Fresh Azure metadata confirms `evhpl24databricks` is Active, Standard, Kafka-enabled; the Active
  `parvinbadalov_evh` has one partition and one-day retention; consumer group `parvinbadalov`
  exists; and `parvinbadalov_policy` has Listen and Send.

Cluster state is ephemeral. A fresh read-only preflight immediately before approval must select
GP1 only if it is exactly `RUNNING`; otherwise it may select GP2 only if GP2 is exactly `RUNNING`.
If neither is ready, stop. UrbanFlow never starts either cluster and never creates a fallback.

## Prepared components

| Component | Purpose |
|---|---|
| `sql/00_prepare_urbanflow_storage.sql` | Idempotent creation of only the dedicated schema and managed Volume. |
| `urbanflow produce` | One validated GBFS snapshot, deterministic IDs, 5,000-event cap, Event Hubs publish, and producer JSON report. |
| `03_eventhubs_to_bronze.py` | Guarded Kafka read, explicit schema, `availableNow`, checkpointed Bronze write, reconciliation, and report. |
| `urbanflow reconcile-reports` | Offline exact comparison of producer and Bronze event-ID sets. |
| `urbanflow.medallion` | Local Silver deduplication, Quarantine, shortage flags, and row reconciliation. |
| `resources/jobs.yml` | Unscheduled Job using `existing_cluster_id`; default `run_stream=false`. |

The existing Lakeflow resource remains serverless and triggered. It is neither attached to GP1 or
GP2 nor included in this first execution.

## One combined approval request

This is a prepared request, not an active approval request. It must not be submitted until a fresh
read-only preflight names GP1 or GP2 as already `RUNNING`. At present neither cluster is running,
the isolated schema and Volume are absent, and the secret-scope ACL does not name the user.

Approve one execution window of at most 20 minutes with a USD 2 operator stop limit to:

1. repeat the read-only GP1/GP2 identity, state, runtime, access-mode, and attach-permission check;
2. create or verify only `dbr_dev.parvinbadalov_urbanflow` and
   `dbr_dev.parvinbadalov_urbanflow.urbanflow_landing`;
3. verify access to only `azure-secrets/parvinbadalov-eventhub-cs` without printing its value;
4. fetch and validate one current Citi Bike GBFS station-status snapshot;
5. publish at most 5,000 events to the existing `parvinbadalov_evh` and save its JSON manifest;
6. run one unscheduled `availableNow` Job on the already-running selected shared cluster, writing
   only the UrbanFlow Bronze table, checkpoint, and JSON report;
7. reconcile published and consumed counts and IDs, rejected and duplicate counts, source,
   collection and broker timestamps, Kafka partitions and offsets, and query termination; and
8. leave the selected cluster unchanged and verify its final state without stopping it.

The request does not include Lakeflow execution, a second workload, a new cluster, a cluster
lifecycle action, library changes, Event Hub deletion or clearing, or any change to other projects.

## Exact execution sequence after approval

1. Run `urbanflow compute-status --require-ready` against the pinned Azure workspace profile and
   stop unless GP1 is exactly `RUNNING`.
2. Apply the prepared storage SQL only if the two isolated objects are absent.
3. **Deploy only the Job**, using the Databricks CLI's `--select` flag:
   `databricks bundle deploy -t azure --select jobs.urbanflow_bounded_stream_test`.
   No UrbanFlow Job exists yet, so nothing is runnable before this. `--select` keeps the Lakeflow
   pipeline out of the first deployment entirely, which was verified read-only with
   `databricks bundle plan`: the unrestricted plan reports `2 to add` (the Job and the pipeline),
   while the selected plan reports `1 to add` (the Job alone). Requires Databricks CLI v1.12.1 or
   later, which supports both `--select` and `plan`.
4. Generate one execution ID and run the producer with `--poll-count 1`, `--execution-id`,
   `--report-path`, and `--confirm-publish`. The credential is resolved by
   `--secret-source key-vault`, which is the default (see "Secret handling" below).
5. Pass the report's execution ID, published count, and source timestamp to the unscheduled Job.
6. Run the Job once with `run_stream=true`; do not start the Lakeflow pipeline.
7. Download the Bronze JSON report and run `urbanflow reconcile-reports` locally.
8. Record the final query state and a read-only after-state for GP1, without stopping it.

The `azure` target is the correct one for this test because its `schema` variable resolves to
`parvinbadalov_urbanflow`, which is exactly what the prepared storage SQL creates. The `dev` target
resolves to `parvinbadalov_urbanflow_dev` instead, so mixing the two would create a Bronze table in
one schema and a Volume in the other.

Starting offsets are `earliest` only when the dedicated checkpoint has no saved position. Retained
messages are not deleted. The execution ID isolates this snapshot during reconciliation, and the
checkpoint advances normally for later approved reruns.

## Secret handling

The Event Hubs connection string is never typed into a chat, never pasted into a terminal, never
placed in a shell variable, and never committed. There are two credential consumers and each reads
the same Key Vault secret through its own platform's supported mechanism:

| Consumer | Where it runs | Mechanism |
|---|---|---|
| The producer | The operator's machine | `EventHubsPublisher.from_key_vault()` reads `parvinbadalov-eventhub-cs` from `kvpl24databricks2` at run time using the existing Azure CLI sign-in. `--secret-source key-vault` is the CLI default. |
| The notebook consumer | GP1, in Databricks | `dbutils.secrets.get(scope="azure-secrets", key="parvinbadalov-eventhub-cs")`, whose output Databricks redacts. The notebook prints only `{"secret_retrieved": True}`. |

Both therefore resolve to the same Key Vault secret rather than to two copies that could drift.
Read access was confirmed read-only on 2026-09-29 by requesting the secret and projecting only
non-value fields (`id`, `enabled`, `created`): the call succeeded, which proves the `get` permission
without revealing the value. The vault uses access policies, not RBAC
(`enableRbacAuthorization: false`).

`from_environment()` is retained for non-interactive CI use and is selectable with
`--secret-source environment`, but it is documented as the less safe path because an environment
variable has to be populated from somewhere first.

Fully passwordless Entra ID authentication to Event Hubs was investigated and is **not currently
available**: the operator holds `Contributor` on the resource group, which is a control-plane role,
and Event Hubs data operations additionally require a data-plane role such as
`Azure Event Hubs Data Sender`. Granting that is a privileged change and was deliberately not made.
Key Vault therefore remains the credential source.

## Cost recheck

The earlier 20-minute and USD 2 numbers remain useful as stop conditions because the plan reuses an
already-running cluster, caps publication, and uses a terminating `availableNow` query. They are not
a guaranteed cost. Azure Databricks billing combines VM and DBU usage and varies by contract,
workload and region; the academy agreement is unavailable to this repository. Stop the test rather
than extend either limit.

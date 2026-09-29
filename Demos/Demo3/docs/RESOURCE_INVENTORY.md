# UrbanFlow read-only resource inventory

[← Main README](../README.md) · [Cost and safety](COST_AND_SAFETY.md)

Discovery began read-only on **2026-09-28**, Event Hubs/compute metadata was refreshed on
**2026-09-29**, and a **second workspace (`dbr_dev_trial`) was inventoried on 2026-09-29** — see
"Two-workspace topology" below. No Azure secret value, token, connection
string, Event Hubs event, Databricks table, Job, pipeline, cluster, or workspace file was created,
retrieved, modified, or deleted.

## Verified identity and target

| Item | Confirmed value |
|---|---|
| Azure subscription | `subscr-ita-vkoldov` (`419d681c-4d7e-48a1-ba31-24afe1f4e486`) |
| Azure tenant | `1b8b4c43-4a62-45e3-bd37-55ded2a5d965` |
| Databricks workspace | `dbr_dev`, East US, Premium |
| Workspace host | `https://adb-7405604503619901.1.azuredatabricks.net` |
| Databricks identity | `parvinbadalov@softserve.academy` |
| Visible catalogs | `system`, `samples`, `dbr_dev` |

## Two-workspace topology

A second workspace, `dbr_dev_trial`, was created by the supervisor and inventoried read-only on
2026-09-29. Both workspaces live in the same subscription, resource group (`PL_24_Databricks`) and
region (East US), and **both are shared with other students** — `dbr_dev_trial` already contains
nine other people's Jobs, so the same "do not modify another student's resources" rule applies
there.

| Property | `dbr_dev` | `dbr_dev_trial` |
|---|---|---|
| Host | `adb-7405604503619901.1.azuredatabricks.net` | `adb-7405615123305702.2.azuredatabricks.net` |
| Azure SKU | `premium` | **`trial`** |
| Identity confirmed | `parvinbadalov@softserve.academy` | `parvinbadalov@softserve.academy` (id `144313299958472`) |
| Groups | `admins`, `users` | `admins`, `users` |
| Entitlements | workspace access, SQL access | workspace access, SQL access, workspace consume — **no cluster-create entitlement** |
| Unity Catalog metastore | `7af05576-c79e-4f56-b84f-ead80be5c8b6` | `7af05576-c79e-4f56-b84f-ead80be5c8b6` — **the same metastore** |
| Default catalog | `dbr_dev` | `dbr_dev_trial` |
| Visible catalogs | `system`, `samples`, `dbr_dev` | `system`, `samples`, `dbr_dev`, `dbr_dev_trial` |
| Interactive clusters | GP1, GP2 (shared, instructor-owned) | **none (0 clusters)** |
| Serverless compute | not used by UrbanFlow | serverless Jobs in active use; one serverless SQL warehouse (`Serverless Starter Warehouse`, PRO, Small, `STOPPED`) |
| `azure-secrets` scope | present (Key Vault backed) | **absent** |

### What the shared metastore does and does not give us

**Shared (verified):** the two workspaces are attached to the same metastore, the `dbr_dev` catalog
has `isolation_mode = OPEN` (so it is not workspace-bound), and Unity Catalog grants are
metastore-level — `ALL_PRIVILEGES` on `dbr_dev` resolves identically from both workspaces. A
read-only cross-workspace probe from `dbr_dev_trial` successfully listed the schema, all four
tables and the managed Volume of `dbr_dev.parvinbadalov_lab09_prod`. External locations are also
metastore-level and all five are visible from the trial workspace. So **the trial workspace can
reach UrbanFlow's data**.

**Not shared (verified):** Databricks secret scopes are workspace-local, not metastore-level. The
trial workspace has only four scopes (`default2`, `entsoe`, `neon`,
`pawelnowak2004pri219_scope`) and **`azure-secrets` is not among them**. The Event Hubs connection
string is therefore unreachable from `dbr_dev_trial` today. This is the single blocker that keeps
the first live Event Hubs test in `dbr_dev` on GP1.

Two further asymmetries worth recording: the `dbr_dev_trial` catalog is `ISOLATED` (bound to the
trial workspace, so `dbr_dev` cannot use it), and the trial workspace's default namespace is
`dbr_dev_trial`, so any UrbanFlow code running there must fully qualify `dbr_dev.<schema>` rather
than relying on the default catalog.

### Serverless assessment for the bounded Kafka consumer

Serverless Jobs compute is demonstrably working in `dbr_dev_trial`: 29 tasks across nine existing
Jobs run with no cluster attached. One of those Jobs (`entsoe-pipeline-prod`) contains a task named
`event_hub_task`, which is circumstantial — not conclusive — evidence that serverless egress to an
Azure Event Hubs endpoint works from this workspace. That student uses their own `entsoe` scope.

Whether serverless can run *our* bounded `availableNow` Kafka consumer is therefore **plausible but
unproven**, and it is blocked today for a reason unrelated to Spark: no readable Event Hubs secret.
Proving it would need either a secret scope created in the trial workspace (a new resource, and the
Key Vault access policy behind it) or a different credential path. The `trial` SKU is also a
time-limited Azure offer, so its compute must not be assumed free or permanent.

## Identity-owned Unity Catalog resources

The existing `dbr_dev.parvinbadalov` schema is owned by the user. Existing layer schemas include
`parvinbadalov_bronze`, `parvinbadalov_silver`, and `parvinbadalov_gold`. Existing Lab/Demo schemas
remain out of scope.

Relevant Volumes in `dbr_dev.parvinbadalov` include:

- `raw_files` and `checkpoints` (managed),
- `lab03_streaming`, `lab05_lakeflow`, and other completed-lab Volumes,
- `demo1_crypto`, `demo2_ecommerce`, and `demo2_olist` (external).

UrbanFlow proposes a separate `dbr_dev.parvinbadalov_urbanflow` schema and
`urbanflow_landing` Volume. Neither existed during discovery. Creation requires approval.

## Storage and external locations

| Resource | Finding |
|---|---|
| `dlspl21databricks` | East US, StorageV2, hierarchical namespace enabled, Standard LRS, Hot tier |
| `parvinbadalov_external_location` | Identity-owned external location rooted at the user's authorized ADLS container |
| `databricks_uc_connector` | Existing managed-identity storage credential and accessible external location |
| `lab08_travelops_*` | Existing Lab 8 resources; explicitly excluded from UrbanFlow |

The likely lowest-risk landing design is a new isolated subpath in the existing authorized
identity container, exposed through a dedicated UrbanFlow Volume. That write has not occurred.

## Event Hubs

| Property | Confirmed value |
|---|---|
| Namespace | `evhpl24databricks` |
| Tier / region | Standard / East US |
| Kafka enabled | `true` |
| Existing identity Event Hub | `parvinbadalov_evh` |
| Partitions | 1 |
| Retention (effective) | **1 hour** (`retentionDescription.retentionTimeInHours = 1`, cleanup policy `Delete`) |
| Retention (legacy field) | `messageRetentionInDays = 1` — misleading, see below |
| Existing consumer groups | `$Default`, `parvinbadalov` |
| Event-level policy | `parvinbadalov_policy` with Listen and Send |

The 2026-09-29 Azure CLI refresh confirmed the namespace is `Active`, Standard tier, in East US,
and Kafka-enabled. The Event Hub is `Active` with one partition. The named consumer group exists,
and the event-level rule still has `Listen` and `Send`. Only metadata and rights were read; no key
or connection string was requested.

### Retention is one hour, not one day

An earlier version of this document reported one-day retention. That was wrong, and the Azure
Portal was right. The hub exposes two retention properties and they disagree:

| Property | Value | Authority |
|---|---|---|
| `messageRetentionInDays` | `1` | Legacy field. It can only express whole days, so a sub-day setting is reported as `1`. |
| `retentionDescription` | `{cleanupPolicy: Delete, retentionTimeInHours: 1}` | **Authoritative.** The effective setting is **one hour**. |

Reading only the legacy field produced the wrong answer. Three operational consequences follow:

1. **The producer and the consumer must run within the same hour.** If the Job run is delayed more
   than an hour after publishing, our own events expire and the bounded read consumes nothing. The
   reconciliation would then correctly fail on a count mismatch rather than silently pass.
2. **It sharply limits foreign-message risk.** Only messages published in the previous hour can
   still exist, so `startingOffsets=earliest` on a fresh checkpoint reads at most one hour of data.
   Combined with the `execution_id` filter, batch isolation is strong.
3. **`failOnDataLoss` is hardcoded to `false`**, so expiry would be tolerated silently by Spark.
   The real guard against that is the reconciliation count-and-ID comparison, not the connector.

Other students' Event Hubs are visible and excluded. UrbanFlow can probably reuse
`parvinbadalov_evh` for a short demonstration, subject to approval and validation that existing
messages/checkpoints will not interfere.

## Key Vault and Databricks secret scope

- Key Vault `kvpl24databricks2` contains an enabled secret named
  `parvinbadalov-eventhub-cs`.
- Databricks scope `azure-secrets` is backed by that Key Vault.
- Discovery listed the secret name and scope metadata only. The secret value was never requested.
- The first live test must confirm that the notebook identity can read only the intended secret.

## GP1 and GP2 read-only inspection

| Item | GP1 | GP2 |
|---|---|---|
| Cluster ID | `0702-132442-toro5spu` | `0702-171207-xo9bbc0y` |
| Latest read-only state on 2026-09-29 | **`RUNNING`** (started by the operator) | `TERMINATED` (inactivity) |
| Runtime | `17.3.x-scala2.13` | `17.3.x-scala2.13` |
| Access mode | `USER_ISOLATION` | `USER_ISOLATION` |
| Worker shape | Standard_F4, autoscale 1–2 | Standard_F4, autoscale 1–2 |
| Auto-termination | 60 minutes | 20 minutes |
| Effective API permission | `CAN_MANAGE` | `CAN_MANAGE` |
| UrbanFlow role | Preferred existing cluster | Compatible fallback |

The user belongs directly to `admins`, which grants inherited `CAN_MANAGE`; the `users` group has
`CAN_RESTART`. UrbanFlow deliberately uses only the included attach capability. It does not use
the broader mutation permissions. Both clusters meet the documented Unity Catalog requirement of
DBR 11.3 LTS or later plus standard/dedicated access mode. Their DBR and standard access mode also
support Kafka Structured Streaming subject to the documented option restrictions. Live Event Hubs
network and secret access have not been tested.

GP1 was started by the project operator and a read-only check on 2026-09-29 confirmed it
`RUNNING`, with the expected cluster ID `0702-132442-toro5spu`, DBR `17.3.x-scala2.13`,
`USER_ISOLATION` access mode and `Standard_F4` nodes. It is owned by `lbiel@softserve.academy`,
not by this project. **Its 60-minute auto-termination is the practical deadline for the first live
test**: if GP1 idles out before the run is approved, UrbanFlow must wait for the operator to start
it again rather than starting it itself. UrbanFlow did not start or stop it. Earlier checks showed
pending compute-scoped
`azure-eventhub`, `databricks-labs-dqx`, and `pytest` libraries; UrbanFlow did not modify them. The
Spark consumer uses the runtime Kafka data source rather than the Python `azure-eventhub` package.

## Compute policies

| Policy | Relevant finding |
|---|---|
| Personal Compute | Single-node `num_workers=0`, on-demand, approved node allowlist, configurable auto-termination |
| Job Compute | `Standard_F4`, one or two workers, spot with fallback, intended for non-interactive jobs |
| Shared Compute | GP1 preferred, GP2 fallback; attach only when already running and after explicit execution approval |

The first live execution should prefer the smallest authorized short-lived mode. A policy being
visible does not itself authorize creation or prove available capacity.

## Schema and Volume creation permission

A read-only privilege check on 2026-09-29 returned `ALL_PRIVILEGES` for
`parvinbadalov@softserve.academy` on the `dbr_dev` catalog, resolved identically from both
workspaces. `ALL_PRIVILEGES` is a wildcard, so an explicit `CREATE_SCHEMA` entry is not listed
separately and its absence from the itemised list is expected rather than a gap.

The strongest evidence is precedent rather than a privilege listing: this identity already owns
`dbr_dev.parvinbadalov_lab09_prod` containing a **managed** Volume named `lab09_landing` — exactly
the schema-plus-managed-Volume shape UrbanFlow needs, in the same catalog. Neither
`dbr_dev.parvinbadalov_urbanflow` nor `dbr_dev.parvinbadalov_urbanflow_dev` exists yet, so the
first live test still has to create them, and that creation is a write awaiting approval.

## Access still unproven

- Creating `parvinbadalov_urbanflow` and `urbanflow_landing` (permission is strongly evidenced
  above, but the objects do not exist yet, so creation itself remains unexecuted).
- Reading the Event Hubs secret through `azure-secrets` from the execution identity.
- Sending to and consuming from `parvinbadalov_evh` without affecting prior exercises.
- Live outbound Event Hubs connectivity and secret-scope access from GP1/GP2.
- A dedicated Lakeflow pipeline and Job under UrbanFlow names. No UrbanFlow Job is deployed in
  either workspace yet, so the first live test must deploy the bundle before it can run anything.
- SQL warehouse, dashboard, alert, email destination, row filter, and mask permissions.
- Whether serverless compute in `dbr_dev_trial` can run the bounded `availableNow` Kafka consumer.
  Blocked today by the missing `azure-secrets` scope there, not by Spark.

**No longer unproven:** a second authorized environment for genuine DEV-to-PROD promotion now
exists (`dbr_dev_trial`), it shares the metastore, and it can read this project's Unity Catalog
objects. What it still lacks is the Event Hubs secret.

The latest read-only secret metadata check confirmed that `azure-secrets` is backed by
`kvpl24databricks2` and contains `parvinbadalov-eventhub-cs`. The scope ACL returned one unrelated
principal and did not name the current user. No secret value was requested, so the Job identity's
ability to read that exact key remains unproven and may require an administrator ACL change.

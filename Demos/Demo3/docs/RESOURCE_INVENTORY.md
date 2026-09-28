# UrbanFlow read-only resource inventory

[← Main README](../README.md) · [Cost and safety](COST_AND_SAFETY.md)

Discovery was performed read-only on **2026-09-28**. No Azure secret value, token, connection
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

A trial workspace is also visible in the subscription. UrbanFlow is not configured to use it.

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
| Retention | 1 hour |
| Existing consumer groups | `$Default`, `parvinbadalov` |
| Event-level policy | `parvinbadalov_policy` with Listen and Send |

Other students' Event Hubs are visible and excluded. UrbanFlow can probably reuse
`parvinbadalov_evh` for a short demonstration, subject to approval and validation that existing
messages/checkpoints will not interfere.

## Key Vault and Databricks secret scope

- Key Vault `kvpl24databricks2` contains an enabled secret named
  `parvinbadalov-eventhub-cs`.
- Databricks scope `azure-secrets` is backed by that Key Vault.
- Discovery listed the secret name and scope metadata only. The secret value was never requested.
- The first live test must confirm that the notebook identity can read only the intended secret.

## Compute policies

| Policy | Relevant finding |
|---|---|
| Personal Compute | Single-node `num_workers=0`, on-demand, approved node allowlist, configurable auto-termination |
| Job Compute | `Standard_F4`, one or two workers, spot with fallback, intended for non-interactive jobs |
| Shared Compute | Visible, but no shared cluster will be used or modified without explicit approval |

The first live execution should prefer the smallest authorized short-lived mode. A policy being
visible does not itself authorize creation or prove available capacity.

## Access still unproven

- Creating `parvinbadalov_urbanflow` and `urbanflow_landing`.
- Reading the Event Hubs secret through `azure-secrets` from the execution identity.
- Sending to and consuming from `parvinbadalov_evh` without affecting prior exercises.
- The Kafka connector/runtime combination on the selected compute.
- A dedicated Lakeflow pipeline and Job under UrbanFlow names.
- SQL warehouse, dashboard, alert, email destination, row filter, and mask permissions.
- A second authorized environment for genuine DEV-to-PROD promotion.

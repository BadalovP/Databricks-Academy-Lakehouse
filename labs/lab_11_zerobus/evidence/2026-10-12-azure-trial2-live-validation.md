# Lab 11 live validation - Azure dbr_dev_trial_2 - 2026-10-12

One bounded live run of the unchanged Lab 11 implementation through the official Zerobus Python SDK
(`databricks-zerobus-ingest-sdk` 1.10.0) into the Academy Azure workspace. All times UTC.
Machine-readable records: [`producer_azure_trial2_t2-run-001.json`](producer_azure_trial2_t2-run-001.json)
(every record sent) and [`validation_azure_trial2_t2-run-001.json`](validation_azure_trial2_t2-run-001.json).

## Environment

| Item | Value |
|---|---|
| Workspace | `dbr_dev_trial_2`, `https://adb-7405618361895520.0.azuredatabricks.net`, ID `7405618361895520`, Azure `eastus` |
| Zerobus endpoint | `https://7405618361895520.zerobus.eastus.azuredatabricks.net` |
| Tables | `dbr_dev.parvinbadalov_lab11_zerobus_trial2.bronze_iot_events`, `...silver_iot_events` (managed Delta, shared Academy metastore; created by `scripts/setup_tables.py`) |
| Ingesting identity | Existing Academy service principal `sp-databricks-adls`, Entra-managed, application ID `d1a2118a-1d70-44cf-ba58-ea3454da6727` (reused at the administrator's request; the Databricks-managed duplicate `c4b38c11-...` with the same name was not used). The Key Vault entry `sp-databricks-adls-appid` was compared in memory and matched |
| Workspace access | Assigned temporarily to `dbr_dev_trial_2` with `USER` permission and the `workspace-access` entitlement only (no admin, no groups); **removed after validation** |
| Unity Catalog | No grants changed: the service principal already held `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY` on `dbr_dev` |
| Credential | Databricks OAuth secret created by the account administrator and stored in Key Vault `kvpl24databricks2` as `sp-databricks-adls-dbx-oauth`; read from the Key Vault-backed scope into process memory only, never printed or written. The Entra client secret (`sp-databricks-adls-appkey`) was not read or used |
| Verification compute | Existing Serverless Starter Warehouse `d8b8ff9942968cb8`, queried with the owner's own identity |

## Producer run `t2-run-001`

| Batch | Delivery | Records | Attempts | Acknowledged offset | Seconds to acknowledgment |
|---|---|---|---|---|---|
| `t2-run-001-primary-01` | primary | 10 | 1 | 0 | 3.265 (includes token minting and opening the stream) |
| `t2-run-001-primary-02` | primary | 10 | 1 | 1 | 0.203 |
| `t2-run-001-primary-03` | primary | 4 | 1 | 2 | 0.204 |
| `t2-run-001-replay-01` | replay | 6 | 1 | 3 | 0.187 |

30 records sent, 30 acknowledged with `wait_for_offset`, 0 retries; stream opened 10:29:12 and closed
cleanly 10:29:12.9. The SDK minted a Zerobus token with the `client_credentials` grant scoped to the Bronze
table. The 6 replayed records reuse the `event_id` of already-acknowledged events (controlled duplicates).

## Verification

| Check | Result |
|---|---|
| Rows queryable / acknowledged | 30 / 30 on the first check |
| Time to queryable Delta rows | Bronze commit v1 (`WRITE`, no user name - the Zerobus service) at 10:29:16, about 3 s after the last acknowledgment (Delta history, 1 s resolution). The polling check ran 26.5 s after the acknowledgment and is only an upper bound |
| Payload comparison (every landed row vs the record sent) | 0 differences |
| Bronze rows / unique events / duplicate deliveries / events with duplicates | 30 / 24 / 6 / 6 |
| Conflicting payloads / null required fields | 0 / 0 |
| Silver MERGE run 1 | 24 inserted, 0 updated, 0 deleted (Delta v1) |
| Silver MERGE run 2 (no Bronze change) | 0 affected rows; Silver identical; empty MERGE commit recorded (v2, 0 output rows) |
| Silver vs the Python reference policy | identical |
| Silver rows / duplicate keys / missing / unexpected / wrong delivery counts | 24 / 0 / 0 / 0 / 0 |
| Objects in the schema | exactly `bronze_iot_events`, `silver_iot_events` |
| Overall | **PASS** |

## Delta history

| Table | Version | Operation | By | Rows |
|---|---|---|---|---|
| Bronze | 0 | CREATE TABLE | owner | - |
| Bronze | 1 | WRITE | Zerobus service (no user name) | 30 |
| Silver | 0 | CREATE TABLE | owner | - |
| Silver | 1 | MERGE | owner | 24 inserted |
| Silver | 2 | MERGE | owner | 0 inserted, 0 updated |

## Cleanup

- `sp-databricks-adls` removed from `dbr_dev_trial_2` (no longer in the workspace's permission assignments
  or SCIM); the account-level service principal, its Entra credentials and Key Vault entries are unchanged.
- **Remaining for the administrator:** revoke the Databricks OAuth secret behind `sp-databricks-adls-dbx-oauth`
  and delete that Key Vault entry. The owner cannot do this (no Service Principal Manager role).
- No cluster, warehouse, Job or schedule was created. The two small tables remain as evidence.

# Lab 11 live validation - Personal DEV - 2026-10-12

One bounded, approved live run through the official Zerobus Python SDK (`databricks-zerobus-ingest-sdk`
1.10.0, synchronous JSON stream). All times UTC. Machine-readable records:
[`producer_personal_dev_pdev-run-001.json`](producer_personal_dev_pdev-run-001.json) (manifest of
every record sent) and [`validation_personal_dev_pdev-run-001.json`](validation_personal_dev_pdev-run-001.json).

## Environment

| Item | Value |
|---|---|
| Workspace | Personal `https://dbc-1750318a-76a9.cloud.databricks.com`, ID `7474653929863069`, AWS `us-east-2` (Zerobus-supported region) |
| Zerobus endpoint | `https://7474653929863069.zerobus.us-east-2.cloud.databricks.com` |
| Tables | `workspace.lab_11_zerobus.bronze_iot_events`, `workspace.lab_11_zerobus.silver_iot_events` (managed Delta, created by `scripts/setup_tables.py`) |
| Ingesting identity | Service principal `lab11-zerobus-producer` (application ID `036b3157...`), entitlement `workspace-access` only |
| Grants | `USE CATALOG` on `workspace`; `USE SCHEMA` on `workspace.lab_11_zerobus`; `SELECT, MODIFY` on the Bronze table only |
| Credential | One OAuth secret, lifetime 1 hour, held in process memory only, never printed or written; **deleted immediately after the run** (secrets remaining on the service principal: none) |
| Verification compute | Existing Serverless Starter Warehouse `410e5f3b4784e981`, queried with the owner's own identity |

## Producer run `pdev-run-001`

| Batch | Delivery | Records | Attempts | Acknowledged offset | Seconds to acknowledgment |
|---|---|---|---|---|---|
| `pdev-run-001-primary-01` | primary | 10 | 1 | 0 | 3.25 (includes opening the stream and minting the token) |
| `pdev-run-001-primary-02` | primary | 10 | 1 | 1 | 0.203 |
| `pdev-run-001-primary-03` | primary | 4 | 1 | 2 | 0.187 |
| `pdev-run-001-replay-01` | replay | 6 | 1 | 3 | 0.204 |

30 records sent, 30 acknowledged (each batch waited on with `wait_for_offset`), 0 retries. Started
08:52:24, last acknowledgment 08:52:28. The SDK log shows a Zerobus token minted with the
`client_credentials` grant scoped to the Bronze table, then the stream opened and closed cleanly.

The 6 replayed records reuse the `event_id` of 6 already-acknowledged events - a controlled
duplicate-delivery experiment, as a producer would cause by retrying after losing an acknowledgment.

## Verification

| Check | Result |
|---|---|
| Rows queryable / acknowledged | 30 / 30 on the first check |
| Time to queryable Delta rows | Bronze Delta commit v1 (`WRITE`, no user name - written by the Zerobus service) at 08:52:33, about 5 s after the last acknowledgment (second resolution). The polling check itself ran 22.7 s after the acknowledgment, so it is only an upper bound |
| Payload comparison (each landed row vs the record sent, keyed by `event_id` + `batch_id`) | 0 differences |
| Bronze rows / unique events / duplicate deliveries / events with duplicates | 30 / 24 / 6 / 6 |
| Conflicting payloads / null required fields | 0 / 0 |
| Silver MERGE run 1 | 24 inserted, 0 updated, 0 deleted (Delta v1) |
| Silver MERGE run 2 (no Bronze change) | 0 affected rows; Silver snapshot identical; Delta still records an empty MERGE commit (v2, 0 output rows) |
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

## What this does and does not prove

It proves real events travelled producer -> Zerobus Ingest -> Unity Catalog Delta with durability
acknowledgments, landed unchanged, that duplicates are detected, and that Silver processing is
idempotent. It does not measure throughput or cost: 30 records are a functional test.

## Not done in Azure `dbr_dev`

Checked read-only on the same day: the owner is not a workspace administrator in `dbr_dev` (cannot
create a service principal there), has no permission on its only SQL warehouse, and the existing
`zerobus-producer-prod` service principal belongs to someone else and was not used. See the README
section "Azure dbr_dev" for the exact steps to complete it.

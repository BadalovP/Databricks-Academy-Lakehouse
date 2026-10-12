# Lab 10 — supervisor review guide

For Łukasz. Each item below links to the thing itself; checking all of it takes about 15 minutes.

> [!IMPORTANT]
> **Please review the Azure Job before 2026-10-20.** `dbr_dev_trial_2` is an Azure Databricks **trial** workspace created on 2026-10-06, so its 14-day trial ends around 2026-10-20. After that date the Job and run pages may no longer open. These stay available regardless:
> - the code and evidence in GitHub;
> - the Delta tables in the shared `dbr_dev` catalog, which was created with the stable `dbr_dev` workspace on 2026-07-02;
> - the Terraform state in HCP.
>
> You can open the Job as a member of the workspace `admins` group.

**What Lab 10 shows:** live read-only queries against Neon PostgreSQL through Lakehouse Federation, a PostgreSQL-to-Delta join, and an incremental CDC pipeline on a Delta table with Change Data Feed. That pipeline covers range-bounded reads, `MERGE` into a target and an audit history, two-way reconciliation and a checkpoint that only advances after the checks pass. Everything is deployed as a three-task serverless Job managed by Terraform with remote HCP state.

## 1. The code (GitHub)

| What | Where |
|---|---|
| Pull request | [PR #69](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/69) (branch `feature/lab10-azure-prod`) |
| Notebooks | [`01_federation_validation`](01_federation_validation.ipynb) → [`02_incremental_cdc_sync`](02_incremental_cdc_sync.ipynb) → [`03_final_validation`](03_final_validation.ipynb); one-time [`00_setup`](00_setup.ipynb) |
| Azure Terraform | [`terraform_azure_prod/main.tf`](terraform_azure_prod/main.tf) - one serverless Job, no cluster, no schedule, auto-retry disabled |
| Personal DEV Terraform | [`terraform/main.tf`](terraform/main.tf) |
| CI | [`lab10_terraform_ci.yml`](../../.github/workflows/lab10_terraform_ci.yml) + [`scripts/validate_lab10_static.py`](scripts/validate_lab10_static.py) + [23 tests](tests/test_validate_lab10_static.py) |
| Azure CD / run (OIDC, waiting for identity) | [`lab10_azure_prod_cd.yml`](../../.github/workflows/lab10_azure_prod_cd.yml), [`lab10_azure_prod_run.yml`](../../.github/workflows/lab10_azure_prod_run.yml) |

Key logic to look at in `02_incremental_cdc_sync`: the `table_changes` range read, the duplicate event-key check, the two `MERGE` statements, and the reconciliation asserts placed **before** the checkpoint `MERGE`. The CI validator fails the build if that order is ever reversed.

## 2. The deployed Job (Azure, `dbr_dev_trial_2`)

| What | Link / value |
|---|---|
| Job | [`[Azure PROD] Lab 10 Federation & CDC` - 582983448494007](https://adb-7405618361895520.0.azuredatabricks.net/#job/582983448494007) |
| End-to-end run | [run 499124610685736](https://adb-7405618361895520.0.azuredatabricks.net/#job/582983448494007/run/499124610685736) - 3/3 tasks SUCCESS, 2026-10-12 00:09-00:11 UTC |
| Compute | Serverless Jobs compute; no cluster, no schedule, `max_concurrent_runs = 1` |
| Delta schema | `dbr_dev.parvinbadalov_lab10_prod` (`customers_delta`, `customers_target`, `customer_change_history`, `cdc_checkpoint`) |
| Foreign catalog | `parvinbadalov_lab10_neon_catalog` on connection `parvinbadalov_lab10_neon_conn` (read-only), bound read-only to `dbr_dev_trial_2` only |
| Terraform state | HCP Terraform `badalov-lakehouse-labs/lab10-azure-prod`: one resource, `databricks_job.lab10_azure_prod` |

In the run, open task `03_final_validation`. Its last cell returns:

```json
{"status": "PASS", "schema": "dbr_dev.parvinbadalov_lab10_prod", "source_rows": 12, "target_rows": 12,
 "history_events": 12, "processed_version": 1, "latest_source_version": 1,
 "source_target_differences": 0, "history_duplicates": 0}
```

Task `01_federation_validation` shows the federated counts (12 customers, 20 orders), the hybrid join and the `EXPLAIN EXTENDED` plans.

## 3. Check the data yourself (any SQL warehouse on the shared metastore)

```sql
-- Current state: expect 12 / 12 / 12 and checkpoint 1.
SELECT (SELECT count(*) FROM dbr_dev.parvinbadalov_lab10_prod.customers_delta)        AS source_rows,
       (SELECT count(*) FROM dbr_dev.parvinbadalov_lab10_prod.customers_target)       AS target_rows,
       (SELECT count(*) FROM dbr_dev.parvinbadalov_lab10_prod.customer_change_history) AS history_events,
       (SELECT last_processed_version FROM dbr_dev.parvinbadalov_lab10_prod.cdc_checkpoint) AS checkpoint;

-- Reconciliation: expect 0 rows.
SELECT * FROM dbr_dev.parvinbadalov_lab10_prod.customers_delta
EXCEPT ALL SELECT * FROM dbr_dev.parvinbadalov_lab10_prod.customers_target;

-- Operations on each table: MERGE on target and history, checkpoint MERGE last.
DESCRIBE HISTORY dbr_dev.parvinbadalov_lab10_prod.cdc_checkpoint;
```

The foreign catalog is isolated to `dbr_dev_trial_2`, and only its owner has data privileges on it. The federated results are therefore in the Job output above, not in a query you can run.

## 4. Personal DEV (the full CDC scenario)

The Personal workspace is not reachable from the Academy account, so its evidence is recorded in [the evidence file](evidence/2026-10-12-manual-azure-deployment.md#personal-dev-re-verification-read-only-same-day). It shows Job `453391098761164`, with history covering insert, update and delete events across Delta versions 3-10. Its run `1058102967727375` was a no-change rerun: 0 events and nothing written, which proves the pipeline is idempotent. Its Terraform plan shows `No changes`.

## 5. What is not done, and why

| Item | Reason |
|---|---|
| GitHub OIDC CD for Azure has never run | Needs the deployer service principal and federation policy, which only an account administrator can create: [ADMIN_REQUEST.md](ADMIN_REQUEST.md) |
| Updates, deletes and the no-change rerun were not repeated in Azure | They would need artificial source changes and more runs; they are proven in Personal DEV with the same notebooks |
| Azure Job reads branch `feature/lab10-azure-prod` | Re-pointed to `main` with a reviewed in-place Terraform update after PR #69 is merged |

Full write-up: [README.md](README.md).

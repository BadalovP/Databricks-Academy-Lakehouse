# Lab 10 manual Azure deployment and end-to-end run - 2026-10-12

Steps B-E of the manual deployment plan, approved by the user, in `dbr_dev_trial_2`
(`adb-7405618361895520`) with the user's own Azure CLI identity, followed by the one controlled
end-to-end Job run. Times are UTC: steps C-E ran late on 2026-10-11 UTC, the Job run early on
2026-10-12 UTC. Not done: any Neon write, any manufactured change to the source table, a second Job
run, merging PR #69.

## Pre-checks (read-only)

| Check | Result |
|---|---|
| Git credential (`/api/2.0/git-credentials`) | `gitHubOAuth`, user `BadalovP`, default for provider (linked by the user in the UI) |
| Foreign catalog before changes | `OPEN`, no bindings, grants `account users: BROWSE`, created and last updated 2026-10-11 23:23 |
| Connection | `parvinbadalov_lab10_neon_conn`, PostgreSQL, owner the user, `read_only = true` |
| Azure schema / Job | `dbr_dev.parvinbadalov_lab10_prod` absent; no Lab 10 Job |
| HCP `lab10-azure-prod` | local execution, unlocked, no state version |

## B. PR #69

Commit `6f8233b` (5 Lab 10 files: Azure `main.tf` retry fix, validator rule, test, two evidence files;
no Co-Authored-By trailer; `.github/terraform/lab10-azurerm-backend.tf` not staged), pushed
fast-forward `18b4efb..6f8233b`. CI run `38185218270`: both legs passed. PR still **draft**, not merged.
The GitHub OIDC workflows were not changed.

## C. Catalog binding and isolation

Dependency check before the change: no Job or pipeline in `dbr_dev`, `dbr_dev_trial` or
`dbr_dev_trial_2` referenced the catalog (all Jobs and pipelines visible to the user scanned), and no
other principal held a data privilege (only `BROWSE`). The system audit, lineage and query-history
tables were not readable (`USE SCHEMA` missing), so they could not be consulted.

1. Binding added first: `BINDING_TYPE_READ_ONLY` for workspace `7405618361895520`.
2. Then `isolation_mode = ISOLATED`.

Effect: `dbr_dev_trial_2` lists `demo_customers`, `lab10_customers`, `lab10_orders`; `dbr_dev` and
`dbr_dev_trial` return "Catalog ... is not accessible in current workspace".

## D. One-time `00_setup`

One serverless run (`jobs submit`), notebook from commit `6f8233b`, `disable_auto_optimization = true`,
`max_retries = 0`, idempotency token `lab10-azure-00-setup-6f8233b95f82`.

| Item | Result |
|---|---|
| Run | `326755590926905`, task run `750929689284052`, SUCCESS, attempt 0, 2026-10-11 23:45:11-23:46:17 (66 s) |
| Notebook output | schema and tables present, CDF on; seeded 12 customers via Federation; checkpoint initialised at 0 |
| Schema | `dbr_dev.parvinbadalov_lab10_prod`, owner the user; exactly 4 tables |
| `customers_delta` | 12 rows; v0 CREATE TABLE, v1 WRITE; `delta.enableChangeDataFeed = true` |
| `customers_target` | 0 rows; v0 only |
| `customer_change_history` | 0 rows; v0 only |
| `cdc_checkpoint` | 1 row `customers_delta_to_target`, version 0 |
| Source vs Neon `lab10_customers` | 12 = 12 rows, ids 1001-1012, identical fingerprint, 0 differences in both directions |
| Neon after setup | 12 customers, 20 orders, 3 demo customers (read only, unchanged) |

## E. Terraform

From `terraform_azure_prod/` at `6f8233b`, `DATABRICKS_AUTH_TYPE=azure-cli`, HCP backend:

1. `terraform plan -var git_branch=feature/lab10-azure-prod -out=...` - the saved plan was checked by
   script: exactly one change, `databricks_job.lab10_azure_prod` `create`; branch
   `feature/lab10-azure-prod`; no schedule, trigger, continuous or job cluster; all 3 tasks
   `disable_auto_optimization = true` with no cluster.
2. `terraform apply` of that saved plan only. The plan file was deleted afterwards.

| Item | Result |
|---|---|
| Job | `582983448494007` `[Azure PROD] Lab 10 Federation & CDC`, created 2026-10-11 23:49:20 |
| Creator / run as | the user |
| Settings | Git branch `feature/lab10-azure-prod`; `max_concurrent_runs = 1`; timeout 1800 s; no schedule or trigger; 3 serverless tasks, auto-retry disabled |
| Runs | none (the Job has not been triggered) |
| HCP state | `sv-b9LiYpLvfytrUXSs`, serial 1, Terraform 1.15.8, one resource `databricks_job.lab10_azure_prod`; workspace unlocked |
| Re-plan | "No changes" (exit code 0) |

## F. One controlled end-to-end Job run

Before the run: source 12 rows at version 1, target 0, history 0, checkpoint 0, no earlier runs of
the Job. Notebook 03 was first changed so its printed report is kept in the Job output (commit
`4695995`, CI run `38186832636` passed); the run used branch `feature/lab10-azure-prod` at that commit.

`jobs run-now` once, idempotency token `lab10-azure-e2e-1-4695995`, serverless, no retry.

| Task | Task run | Result | Output |
|---|---|---|---|
| `01_federation_validation` | `1125166976149880` | SUCCESS, attempt 0, 60 s | Neon customers 12, orders 20; PASS Federation; PASS Federation + Delta join |
| `02_incremental_cdc_sync` | `88505024944991` | SUCCESS, attempt 0, 34 s | last processed 0, latest source 1, range 1-1, **12 CDF events**, source = target, checkpoint -> 1 |
| `03_final_validation` | `225414791378493` | SUCCESS, attempt 0, 17 s | report and JSON below |

Run `499124610685736`: TERMINATED SUCCESS, 2026-10-12 00:09:33-00:11:45 (132 s).
URL: `https://adb-7405618361895520.0.azuredatabricks.net/#job/582983448494007/run/499124610685736`

Final validation JSON (from the Jobs output API):

```json
{"status": "PASS", "schema": "dbr_dev.parvinbadalov_lab10_prod", "source_rows": 12, "target_rows": 12,
 "history_events": 12, "processed_version": 1, "latest_source_version": 1,
 "source_target_differences": 0, "history_duplicates": 0}
```

### Independent SQL verification after the run

| Check | Result |
|---|---|
| Source / target / history / checkpoint | 12 / 12 / 12 / version 1 (`processed_at` 2026-10-12 00:11:15) |
| History by version and type | version 1: 12 `insert` |
| History vs `table_changes(source, 1, 1)`, both directions | 0 / 0 |
| Target vs source, both directions | 0 / 0 |
| Target vs Neon `lab10_customers`, both directions | 0 / 0 |
| Duplicate keys: target `customer_id`; history `(customer_id, _commit_version, _change_type)` | 0 / 0 |
| Hybrid join Neon orders LEFT JOIN Delta customers | 20 orders, 0 unmatched, 20 distinct orders, 11 customers |
| Delta history `customers_target` | v1 MERGE: 12 inserted, 0 updated, 0 deleted |
| Delta history `customer_change_history` | v1 MERGE: 12 inserted |
| Delta history `cdc_checkpoint` | v1 WRITE (setup, version 0), v2 MERGE: 1 updated (0 -> 1), written after the reconciliation |
| Federated plan (`EXPLAIN FORMATTED ... lab10_orders WHERE amount > 100`) | `Scan JDBC ... parvinbadalov_lab10_neon_catalog.public.lab10_orders` with `PushedDownOperators` holding the `amount` filter - the filter is pushed down to PostgreSQL |
| Neon after the run | 12 customers, 20 orders, 3 demo customers (unchanged) |

### What this run does and does not prove in Azure

It proves Federation, the hybrid join, CDF reading, the target and history `MERGE`s, two-way
reconciliation and late checkpoint advancement on Azure serverless compute, using real CDF events (the
seed batch). Updates, deletes and the no-change rerun were **not** repeated in Azure: that would need
artificial source changes and further runs. They are proven by the Personal DEV runs below, which use
the same notebooks and logic.

## Personal DEV re-verification (read-only, same day)

| Check | Result |
|---|---|
| Job `453391098761164` | 3 ordered Git tasks (branch `main`), no cluster, no schedule; the only Lab 10 Job there |
| Runs | `1058102967727375` SUCCESS: checkpoint 9 = latest 9, **0 events, nothing written** (idempotent no-change run); `674533552504139` SUCCESS: range 10-10, **2 events** (update pre/post-image), checkpoint -> 10 |
| Federation / hybrid join | 12 customers, 20 orders; 20 orders kept by the LEFT JOIN (3 without a Delta customer after the simulated deletes) |
| Source / target / history | 12 / 12 / 12; differences 0 / 0; duplicate keys 0 / 0 |
| Checkpoint | 10 = latest source version 10 |
| History by type | versions 3-10: insert x2, update pre/post-image x4 each, delete x2 |
| History vs CDF versions 3-10, both directions | 0 / 0 |
| Terraform `lab10-personal-dev` | state holds only `databricks_job.lab10`; plan "No changes" |
| Running clusters | none |

## Still open

- Merge PR #69, then re-point the Azure Job's `git_branch` to `main` with a reviewed plan (an in-place
  update).
- Automated GitHub OIDC deployment still needs the deployer service principal and federation policy
  (account administrator); see `ADMIN_REQUEST.md`.

# Lab 10 · Lakehouse Federation & CDC

**Neon PostgreSQL federation · Delta Change Data Feed · checkpointed incremental MERGE · Terraform on HCP · GitHub Actions CI/CD**

> [!IMPORTANT]
> **Personal DEV is complete and verified.** **Azure PROD is prepared but NOT deployed**: its Terraform, workflows and setup notebook are validated statically, and it waits on one administrator action (a foreign catalog) and two GitHub settings. Nothing below marked *planned* has run.

> [!NOTE]
> **Two integration patterns, deliberately.** Neon PostgreSQL is reached **through Lakehouse Federation** - live, read-only queries. **CDC is simulated on a local Delta table** with **Delta Change Data Feed**. This lab does not capture changes from PostgreSQL itself (no logical replication, WAL decoding or Debezium), so it is not PostgreSQL-native CDC.

## Status at a glance

| Area | Personal DEV | Azure PROD |
|---|---|---|
| Workspace | `dbc-1750318a-76a9.cloud.databricks.com` | `adb-7405604503619901.1.azuredatabricks.net` |
| Delta schema | `workspace.lab_10_federation_cdc` | `dbr_dev.parvinbadalov_lab10_prod` (*planned*) |
| Foreign catalog | `neon_postgres_conn_catalog` | `parvinbadalov_lab10_neon_catalog` (*requested*, see [ADMIN_REQUEST.md](ADMIN_REQUEST.md)) |
| Neon connection | `neon_postgres_conn` | `parvinbadalov_lab10_neon_conn` (**exists**) |
| Compute | Serverless | Serverless Jobs compute (*planned*) |
| Databricks Job | `453391098761164` ✅ | `[Azure PROD] Lab 10 Federation & CDC` (*planned*) |
| Terraform root | [`terraform/`](terraform/) | [`terraform_azure_prod/`](terraform_azure_prod/) |
| HCP Terraform workspace | `lab10-personal-dev` ✅ | `lab10-azure-prod` (*to create*) |
| Deployment auth | PAT secret | GitHub OIDC → managed identity → `azure-cli` (no PAT) |
| CD workflow | [`lab10_terraform_cd.yml`](../../.github/workflows/lab10_terraform_cd.yml) ✅ | [`lab10_azure_prod_cd.yml`](../../.github/workflows/lab10_azure_prod_cd.yml) (*untested*) |
| Run workflow | Databricks UI | [`lab10_azure_prod_run.yml`](../../.github/workflows/lab10_azure_prod_run.yml) (*untested*) |

## Architecture

```mermaid
flowchart LR
    NEON[("Neon PostgreSQL<br/>lab10_customers · lab10_orders")]

    subgraph DEV["Personal DEV - verified"]
        DFC["neon_postgres_conn_catalog"]
        DDELTA["workspace.lab_10_federation_cdc"]
        DJOB["Job 453391098761164<br/>serverless"]
    end

    subgraph PROD["Azure PROD - planned"]
        PFC["parvinbadalov_lab10_neon_catalog"]
        PDELTA["dbr_dev.parvinbadalov_lab10_prod"]
        PJOB["[Azure PROD] Lab 10 Job<br/>serverless"]
    end

    NEON -- "Federation, read-only" --> DFC
    NEON -- "Federation, read-only" --> PFC
    DJOB --> DFC & DDELTA
    PJOB --> PFC & PDELTA

    GH["GitHub Actions"] -- "PAT · HCP lab10-personal-dev" --> DJOB
    GH -- "OIDC · HCP lab10-azure-prod · approval" --> PJOB
```

Each Job runs the same notebooks in order:

| Task | Notebook | Writes? |
|---|---|---|
| 1 | [`01_federation_validation`](01_federation_validation.ipynb) - federated counts, hybrid Neon ⨝ Delta join, `EXPLAIN EXTENDED` | No |
| 2 | [`02_incremental_cdc_sync`](02_incremental_cdc_sync.ipynb) - CDF range, target and history `MERGE`, checkpoint | Yes, own schema only |
| 3 | [`03_final_validation`](03_final_validation.ipynb) - reconciliation, uniqueness, audit completeness; returns a JSON summary | No |

[`00_setup`](00_setup.ipynb) is **not** a Job task. It is run once per new environment.

## Lakehouse Federation

A Unity Catalog **connection** stores the Neon endpoint and credentials; a **foreign catalog** exposes the PostgreSQL tables as `<catalog>.public.<table>`. Rows stay in PostgreSQL and Databricks pushes supported work down to it. Notebook 01 proves direct federated reads (12 customers, 20 orders) and a hybrid join with a local Delta table.

## Incremental CDC

1. Read `last_processed_version` from `cdc_checkpoint`; read the source's latest version.
2. If they are equal, exit without writing.
3. Otherwise read `table_changes` for exactly the new version range.
4. Keep the newest final-state event per customer and `MERGE` it into `customers_target` (SCD Type 1; deletes remove the row).
5. `MERGE` every event, including update pre-images, into `customer_change_history`, keyed by `(customer_id, _commit_version, _change_type)`.
6. Reconcile target and source **at the processed version** in both directions.
7. Advance the checkpoint **only after** every check passes.

A failure leaves the checkpoint unchanged, and both writes are `MERGE`s, so the same range can be replayed safely.

## Environment portability

All notebooks take `catalog` and `schema` parameters (01 and 00 also take `foreign_catalog`). The defaults are the Personal DEV names, so the DEV Job - which passes no parameters - behaves exactly as before; this was checked line by line against the previous notebooks. The Azure Job passes its own values.

[`00_setup`](00_setup.ipynb) creates the schema and four tables with the same column types as DEV, enables CDF on the source, seeds the source **from Neon through the foreign catalog**, and sets the checkpoint to version 0. The first CDC run then processes the seed as a real batch (12 inserts). It is idempotent: it never drops, truncates, overwrites or reseeds, and it stops on an inconsistent state rather than guessing.

## Terraform and state separation

| | Personal DEV | Azure PROD |
|---|---|---|
| Root | `terraform/` | `terraform_azure_prod/` |
| State | HCP `badalov-lakehouse-labs/lab10-personal-dev` | HCP `badalov-lakehouse-labs/lab10-azure-prod` |
| Resource | `databricks_job.lab10` | `databricks_job.lab10_azure_prod` |
| Provider host | pinned to the Personal workspace | pinned to the Azure workspace |
| Provider | `databricks/databricks` 1.137.0 (same lock file) | same |

Separate roots and separate HCP workspaces mean a PROD change can never read, refresh or destroy DEV state, and vice versa. No Azure Storage account is used for Lab 10 state.

The Azure Job declares **no** `existing_cluster_id`, `new_cluster` or `job_cluster`, so it runs on serverless Jobs compute. A local read-only plan against the Azure workspace returned `1 to add, 0 to change, 0 to destroy` with no cluster fields.

## CI/CD

**CI** ([`lab10_terraform_ci.yml`](../../.github/workflows/lab10_terraform_ci.yml)) runs `fmt -check`, `init -backend=false` and `validate` on **both** roots for every pull request and push. It contacts no workspace.

**Personal DEV CD** - manual; the confirmation field alone decides the operation (empty = plan, `APPLY-453391098761164` = apply).

**Azure PROD CD** ([`lab10_azure_prod_cd.yml`](../../.github/workflows/lab10_azure_prod_cd.yml)) - manual, `main` only:

| Safeguard | How |
|---|---|
| Plan or apply from one field | empty = plan; `APPLY-LAB10-AZURE-PROD` = apply; anything else fails |
| Approval | apply requires `azure-release-approval`; plan needs none (read-only) |
| Secretless Azure auth | GitHub OIDC in `azure-prod` → managed identity `github-lab08-travelops` → `azure-cli` |
| Correct workspace | the workflow, `DATABRICKS_AZURE_HOST` and the provider block must all name the same host |
| No duplicate Job | a first deployment is refused if a Job with the same name already exists |
| State integrity | after the first deployment, state must hold exactly the one Job and match `LAB10_AZURE_PROD_JOB_ID` |
| Safe changes only | one `create` on the first deployment; afterwards only no-op or in-place update |
| Serverless only | any task or Job referencing cluster compute fails the plan |
| Exact plan | apply uses the saved plan; the Job is then re-read and verified |
| No silent skip | a requested apply that does not complete fails the run |

**Azure PROD run** ([`lab10_azure_prod_run.yml`](../../.github/workflows/lab10_azure_prod_run.yml)) - separate confirmation `RUN-LAB10-AZURE-PROD` and approval. It refuses a Job with any cluster compute, waits for completion, and writes the run ID, run URL, per-task results and the final validation JSON to the run summary.

## Azure PROD: remaining steps

| # | Step | Who | Status |
|---|---|---|---|
| 1 | Foreign catalog on `parvinbadalov_lab10_neon_conn` ([request](ADMIN_REQUEST.md)) | Academy admin | Waiting |
| 2 | Create HCP workspace `lab10-azure-prod`, execution mode **Local** | You | To do |
| 3 | Add environment secret `HCP_TERRAFORM_TOKEN` to GitHub environment `azure-prod` | You | To do |
| 4 | Run `00_setup` once in the Azure workspace on serverless compute, with `catalog=dbr_dev`, `schema=parvinbadalov_lab10_prod`, `foreign_catalog=parvinbadalov_lab10_neon_catalog` | You (approved) | To do |
| 5 | Grant the deployment identity read on the foreign catalog and read/write on the schema (SQL in [ADMIN_REQUEST.md](ADMIN_REQUEST.md)) | You | To do |
| 6 | Azure PROD CD with an empty confirmation (plan); review `1 to add` | You | To do |
| 7 | Azure PROD CD with `APPLY-LAB10-AZURE-PROD`; record the Job ID as repository variable `LAB10_AZURE_PROD_JOB_ID` | You | To do |
| 8 | Run workflow with `RUN-LAB10-AZURE-PROD`; expect 3/3 tasks and a PASS summary | You | To do |

## Verified results (Personal DEV)

| Check | Result |
|---|---|
| Federated Neon customers / orders | 12 / 20 |
| Delta source / target customers | 12 / 12 |
| Source-target differences | 0 |
| CDC checkpoint = latest source version | 10 |
| Persisted CDC history events | 12 |
| Job runs | `674533552504139` SUCCESS, `1058102967727375` SUCCESS |
| Terraform | HCP remote state; plan `No changes` |
| CD plan-only run | `38102000753` - apply skipped by design |
| CD apply run | `38102681544` - Apply, Verify Job ID and apply guard all succeeded (`0 added, 0 changed, 0 destroyed`) |
| CI | `38101418182` success |

## Evidence checklist for review

- [x] DEV Job `453391098761164` and its successful runs
- [x] DEV CD plan run and no-change apply run
- [x] CI passing
- [ ] Azure foreign catalog created (administrator)
- [ ] `00_setup` run in Azure; four tables present, checkpoint 0
- [ ] Azure PROD CD plan run (`1 to add`)
- [ ] Azure PROD CD apply run; Job ID recorded
- [ ] Azure PROD Job run: 3/3 tasks SUCCESS, final validation JSON with 0 differences

## Limitations

- **CDC is simulated** on Delta with CDF; PostgreSQL changes are visible through Federation but not captured as CDC events.
- **Small scale:** 12 customers prove correctness and replay safety, not throughput.
- **Azure serverless is strongly indicated but not yet proven for Jobs.** The workspace has run a serverless Lakeflow pipeline and a serverless SQL warehouse; the first Azure Job run is the proof.
- **Neon network reachability from Azure serverless is unverified.** If the Neon project restricts client IP addresses, Azure serverless egress must be allowed.
- **Git-backed tasks** read notebooks from GitHub `main`; whether the deployment identity can read the repository without a Git credential is confirmed on the first run.
- **Notebooks are committed without outputs**; results come from Job runs.

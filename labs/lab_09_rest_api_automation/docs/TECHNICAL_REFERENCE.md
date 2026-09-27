# Lab 09 · Technical reference

[← Visual overview](../README.md) · [Live validation evidence](../evidence/LIVE_VALIDATION_SUMMARY.md) · [Workflow](../../../.github/workflows/lab09.yml)

This is the **implementation companion** to the shorter visual README. It condenses the design decisions documented by the original long-form README. The [previous README is preserved verbatim](legacy/README_before_refresh.md) for its exhaustive explanations and evidence trail.

## 1 · What is automated

The reusable package in [`src/lab09/`](../src/lab09/) is deliberately **imperative**: it talks to Databricks through the Python SDK / REST APIs instead of requiring a Databricks Asset Bundle. Main capabilities:

| Module | Role | Main SDK surface |
|---|---|---|
| `client.py` | Explicit authentication, profile/host validation, configuration | `WorkspaceClient`, `current_user.me()` |
| `preflight.py` | Check workspace features, approved policies, Volume/API capability | `clusters`, `cluster_policies`, Unity Catalog, Files API |
| `volumes.py` | Find or create the configured landing Volume | `volumes.read()`, `volumes.create()` |
| `landing.py` | Download, validate and incrementally land TLC monthly Parquet | `files.list_directory_contents()`, `files.upload()` |
| `workspace.py` | Upload source files and notebook objects correctly | `workspace.mkdirs()`, `workspace.import_()` |
| `compute.py` | Resolve policy-compatible runtimes and manage classic clusters | `clusters.create()`, `clusters.get()`, `clusters.delete()` |
| `pipelines.py` | Idempotent Lakeflow definitions and pipeline execution | `pipelines.list_pipelines()`, `create()`, `update()`, `start_update()` |
| `jobs.py` | Create/reset Job definitions, trigger runs, collect notebook output | `jobs.list()`, `create()`, `reset()`, `run_now()`, `get_run_output()` |
| `monitoring.py` | Explicit finite polling and post-run compute verification | `jobs.get_run()`, `pipelines.get_update()`, `clusters.get()` |
| `reporting.py` | Machine-readable run evidence and truthful cleanup reporting | Python JSON serialization |

The [optional CLI](../src/lab09/cli.py) exposes `preflight`, `run-all`, `status` and scoped `cleanup` commands. It requires an explicit, independently checked authentication target.

## 2 · Two deployment modes

| Concern | Personal workspace | Azure workspace |
|---|---|---|
| Config | [`dev.yml`](../config/dev.yml) | [`azure.yml`](../config/azure.yml) |
| Job display name | `lab09_taxi_reconciliation_job` | `lab09_taxi_reconciliation_job` |
| Lakeflow display name | `lab09_taxi_pipeline_v2` | `lab09_taxi_pipeline_v2` |
| Notebook task compute | Serverless | Shared on-demand `lab09_shared_compute` |
| Lakeflow compute | Pipeline managed | Pipeline managed; serverless preferred |
| Primary execution path | `python -m lab09.cli run-all` | [`deploy_azure_job.py`](../scripts/deploy_azure_job.py) + [`run_azure_job.py`](../scripts/run_azure_job.py) |
| GitHub approval | `personal-prod-approval` | `azure-release-approval` |

The Azure Job has `ingestion → lakeflow_pipeline → reconciliation` dependencies. Ingestion and reconciliation share one Job-cluster definition *per run*. Lakeflow is managed independently. The Job has no schedule and does not consume Job compute when idle.

In Azure, catalog-wide names are intentionally student-prefixed to avoid collisions in the shared academy workspace: `dbr_dev.parvinbadalov_lab09_prod` is the output schema and `lab09_landing` its managed Volume. The workspace project folder is under the executing user's `/Workspace/Users/.../lab09/` home path.

### Historical classic-compute proof

The separate [`validate_classic_e2e.py`](../scripts/validate_classic_e2e.py) demonstration created a policy-compliant classic cluster, observed `RUNNING` without a manual restart, executed a trivial Job notebook, verified `OK:42`, terminated the cluster and checked the final `TERMINATED` state. The sanitized result is recorded in [the live-validation report](../evidence/LIVE_VALIDATION_SUMMARY.md#10-second-azure-prod-attempt-2026-09-27--the-create-torunning-gap-closed-by-automation-alone). This is distinct from the permanent Azure taxi Job and is not something to rerun during routine CI.

## 3 · Dataset and ingestion contract

Data sources are the **official NYC TLC Yellow Taxi monthly Parquet files** and taxi-zone lookup CSV. A pre-existing sample table would bypass the important Files API and incremental-landing work. For configured months, `land_next_month()`:

1. Lists already-landed `yellow_tripdata_YYYY-MM.parquet` filenames in the dedicated Volume's `trips/` directory.
2. Selects the first configured month not already present, or returns `NO_NEW_DATA` if all configured months are landed.
3. Downloads with bounded retries and validates a nonempty file and the `PAR1` magic bytes at both ends before upload.
4. Uploads with `overwrite=False`; the function never overwrites an existing configured month.
5. Continues through pipeline execution and reconciliation even on `NO_NEW_DATA`, because already-landed data remains valid input.

Personal `run-all` downloads on the runner/local machine and transfers bytes via the Files API; the Azure Job includes an inspectable notebook ingestion task that runs as part of its three-task graph.

The pipeline's **landing trips path and taxi-zone reference path are both supplied through the active environment's Spark configuration**. This prevents earlier Azure failures caused by source code falling back to a Personal-workspace-only Volume path.

## 4 · Medallion logic and data quality

| Output | Input / rule |
|---|---|
| Bronze | Auto Loader reads new Parquet files and records source-file metadata |
| Silver | Valid trips, enriched with the taxi-zone lookup |
| Quarantine | Invalid trips *retained* with `failed_rules` for diagnosis |
| Gold | Daily-summary materialized view from valid Silver rows |

**Hard validity rules** (each rejected row can violate more than one):

- `INVALID_FARE`: null or nonpositive fare.
- `INVALID_DISTANCE`: null or nonpositive trip distance.
- `INVALID_DATETIME_ORDER`: missing timestamp or drop-off no later than pickup.
- `INVALID_MONTH`: pickup month disagrees with the source filename's month.

Passenger count is a **warning only**. Unknown or TLC-placeholder pickup/drop-off zones are retained with appropriate known/unknown flags instead of silently dropped. Silver and Quarantine partition the same tagged Bronze rows, giving the reconciliation invariant:

```text
bronze_rows = silver_valid_rows + rejected_rows
2,964,624  = 2,869,585         + 95,039
```

These exact figures come from the **successful September 27 Azure validation**. Gold contained 6,803 daily-summary records in that snapshot; future inputs can change all counts.

## 5 · Deployment, monitoring and cleanup

`deploy_azure_job.py` is **idempotent and does not trigger a run**. It uses the current environment config to ensure the dedicated schema, Volume, uploaded sources, Lakeflow pipeline and one three-task Job. The existing Job and pipeline have fixed IDs; ownership verification prevents unintentionally updating other academy users' resources merely because names match.

`run_azure_job.py` triggers **one** Job run, explicitly polls it and reads the individual task outcomes. Overall success requires *every* task to succeed, ingestion and reconciliation output to be retrievable, `reconciliation_passed is True`, a shared Job-cluster ID to be observed, and that cluster independently verified `TERMINATED` afterward. It does not treat a monitoring timeout as proof that Databricks stopped running; timeouts warrant an immediate state check and human escalation if compute remains active.

The permanent Azure Job's cluster configuration must satisfy the workspace's **Job Compute** policy. The previously tested policy required `Standard_F4`, a driver plus worker and forbade `cluster_name` and `autotermination_minutes` fields in automated cluster specifications. This policy differs from the Personal Compute policy used in the historical classic-cluster proof.

Deletion controls are intentionally narrow: GP1, GP2 and other users' resources are out of scope. Resetting landing files, deleting temporary classic-compute resources or changing unrelated Unity Catalog objects never occurs during static CI.

## 6 · Identity, ownership and security

Azure uses the existing, organization-configured **GitHub OIDC service principal** for deployment and Job triggering, with Databricks' separate **Run as** feature pointing actual task execution to the authorized user. Attempting to create a dedicated Lab 9 Entra application was blocked by tenant policy, so the existing shared identity was not granted new cluster-creation privileges.

The permanent Azure Job is owned by the user account and still has its original service-principal creator recorded; GitHub retains the scoped management permission needed for future idempotent deployments. The Lakeflow pipeline remains owned by the service principal while the user has management access and is its Run as identity. Pipeline ownership transfer would require a metastore administrator and is not necessary for successful operation.

GitHub Actions uses an **independently pinned Azure workspace host**, an explicit manual `deployment_target`, and an environment-specific human gate. Ordinary PRs/pushes never reach cloud-changing stages. The Personal workflow retains its existing approved secret configuration; the Azure workflow uses OIDC rather than a committed or stored PAT. Do not put private credentials into screenshots or Markdown.

## 7 · Safe commands and workflow usage

```bash
# From labs/lab_09_rest_api_automation — local and mock-only
python -m pip install -e . -r requirements-dev.txt
python -m pytest -q
ruff check .
black --check .

# Read-only status only, against a profile you explicitly verified
python -m lab09.cli --profile YOUR_VERIFIED_PROFILE status
```

To launch an intentionally billable run, use [the single GitHub workflow](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/workflows/lab09.yml) → **Run workflow** → choose `personal` or `azure` → complete that environment's existing GitHub approval. A push or PR executes only static tests.

## 8 · Evidence and troubleshooting

- [`evidence/LIVE_VALIDATION_SUMMARY.md`](../evidence/LIVE_VALIDATION_SUMMARY.md): dated, environment-separated observations, original failure investigation, final Azure success and cleanup.
- [Live-validation report, section 10](../evidence/LIVE_VALIDATION_SUMMARY.md#10-second-azure-prod-attempt-2026-09-27--the-create-torunning-gap-closed-by-automation-alone): complete classic-compute create → run → terminate demonstration.
- [Successful Azure GitHub Actions run](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/36351092702): deployed the actual Job and observed all three tasks and strict cleanup passing.
- [Successful Personal GitHub Actions run](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/36355434578): consolidated workflow with serverless execution.
- [Visual screenshots](images/): cropped, lossless versions of the real screenshots with originals of the two detailed Databricks graphs for closer inspection.

Historical failures included cluster-create permissions under the deploying identity; source import path resolution across Run as users; `/Workspace` prefix normalization in notebook paths; and separate Bronze and Silver Volume-path configuration bugs. The merged fixes and regression tests address each of these. Avoid retrying a live workflow simply to check Markdown formatting.

---

**Original exhaustive documentation:** the [previous README](legacy/README_before_refresh.md) is preserved verbatim alongside this companion. The repository's Git history is also authoritative for the original document.

# Lab 10 validation and OIDC review - 2026-10-12

Continuation of the [read-only audit](2026-10-12-read-only-audit.md). Offline checks, a design review
of the native Databricks GitHub OIDC deployment, and a read-only re-check of the Azure prerequisites.
No cloud resource, permission, Job run, Terraform apply, SQL query or Neon write was started.

## Offline validation (local)

| Check | Result |
|---|---|
| `scripts/validate_lab10_static.py` | PASS - notebook syntax with stripped outputs, CDC contract (checkpoint after two-way reconciliation), serverless-only Azure Job, workflow safety, secret scan |
| `tests/test_validate_lab10_static.py` (standard library `unittest`) | **22 passed** - the real repository passes; each of 20 unsafe changes is rejected (checkpoint before reconciliation, missing delete handling, missing reverse reconciliation, destructive setup, CDF off, no time travel, notebook outputs, syntax error, forbidden workspace, cluster, schedule, push trigger, `azure/login`, PAT auth, wrong environment, missing host guard, principal check without `userName`, mutable action tag, unsaved-plan apply, secret patterns) |
| Ruff (validator and tests) | pass |
| YAML (4 Lab 10 workflows) | all parse |
| Terraform 1.15.8 `fmt -check`, `init -backend=false`, `validate` | pass for `terraform/` and `terraform_azure_prod/` |
| Pinned actions | `actions/checkout@fbc6f39` (= `v5`), `hashicorp/setup-terraform@dfe3c3f` (= `v4`), `databricks/setup-cli@9b64c94` (= `main` on 2026-10-12) - each SHA exists and equals its ref |

CI (`lab10_terraform_ci.yml`) now runs the validator and the 22 tests in both matrix legs before the
Terraform checks; neither contacts a workspace.

## Defects found and fixed in this review

| Defect | Effect | Fix |
|---|---|---|
| The CD and run workflows checked the principal with `.applicationId // .application_id` | For a service principal, `current-user me` (SCIM `/Me`) carries the application ID in `userName`; the check would have rejected the correct deployer and blocked every run | Accept `.applicationId // .application_id // .userName`, still requiring an exact match on `DATABRICKS_CLIENT_ID` and the display name; enforced by the validator |
| The validator's mutable-action check missed the `- uses: owner/action@v5` step form | An unpinned action could pass review | Pattern covers both forms; covered by a test |
| CI invoked `python` | `python3` is the reliable name on Ubuntu runners | `python3` |
| README called serverless Jobs unverified in `dbr_dev_trial_2` | Out of date | All 11 existing Jobs there are serverless and the Lab 12 Azure Job ran serverless on 2026-10-11; this Job remains unverified |

## Native Databricks GitHub OIDC - compatibility review

Checked against the Azure Databricks documentation for GitHub Actions workload identity
federation (page dated 2026-10-02):

| Requirement | Workflows | Status |
|---|---|---|
| `DATABRICKS_AUTH_TYPE=github-oidc`, `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` | set in both Azure workflows; host pinned to `dbr_dev_trial_2` | matches |
| `permissions: id-token: write` | set on the deploying jobs | matches |
| Environment-based subject | jobs use `environment: lab10-azure` | matches the policy subject |
| Subject format | repo created 2026-07-06 (before the 2026-07-15 cutoff); GitHub reports `use_immutable_subject: false`, prefix `repo:BadalovP/Databricks-Academy-Lakehouse` | expected subject `repo:BadalovP/Databricks-Academy-Lakehouse:environment:lab10-azure` is correct |
| Audience | default (Databricks account ID `4b6644a4-...`) | matches the requested policy |
| Terraform provider | lock file pins `databricks` 1.137.0 (unified auth with `github-oidc`) | compatible |
| Service principal and policy | `github-databricks-academy-deployer` not assigned to `dbr_dev_trial_2`; policy not visible without account admin | **blocked - account administrator** |
| GitHub environment `lab10-azure` | does not exist (existing: `azure-prod`, `azure-release-approval`, `lab10-personal-dev`, ...) | **missing - needs approval to create** |

The design is consistent with the documentation, but it is **not verified live**: the first
plan-only CD run after the prerequisites exist is the acceptance test of the token exchange.

## Read-only re-check of Azure prerequisites (22:50 UTC)

| Item | Result |
|---|---|
| Identity | `parvinbadalov@softserve.academy` on `dbr_dev_trial_2` (Azure CLI auth) |
| Connection `parvinbadalov_lab10_neon_conn` | PostgreSQL, `read_only = true`, owned by me |
| Foreign catalog `parvinbadalov_lab10_neon_catalog` | does not exist |
| Schema `dbr_dev.parvinbadalov_lab10_prod` | does not exist |
| My metastore privileges | `USE_MARKETPLACE_ASSETS` only - no `CREATE CATALOG` |
| `dbr_dev` grants | `account users`: `ALL_PRIVILEGES`, `MANAGE` (so the deployer, once assigned, inherits write access to the schema; only the foreign-catalog read grant remains) |
| Deployer service principal | not assigned (workspace has only `hospital_zerobus`, `csv-api-app`) |
| Lab 10 Jobs | none |
| PR #69 | open draft, mergeable, head `8ffb21a` |

## Verified vs proposed

- **Verified:** Personal DEV Federation + CDC (historical runs and today's audit), static safety of all
  Lab 10 code and workflows, the OIDC configuration's consistency with the documentation.
- **Proposed, not verified:** the Azure OIDC token exchange, the Azure Terraform plan/apply, the Azure
  Job and its first serverless run, and a fresh per-operation CDC test.

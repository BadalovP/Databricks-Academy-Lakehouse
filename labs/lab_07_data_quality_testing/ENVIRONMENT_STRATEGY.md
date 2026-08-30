# Environment / Resource Reference

**GitHub**

| Item | Value |
|---|---|
| Repository | https://github.com/BadalovP/Databricks-Academy-Lakehouse |
| Lab 07 folder | [`labs/lab_07_data_quality_testing`](https://github.com/BadalovP/Databricks-Academy-Lakehouse/tree/main/labs/lab_07_data_quality_testing) |
| Lab 07 README | [README.md](https://github.com/BadalovP/Databricks-Academy-Lakehouse/blob/main/labs/lab_07_data_quality_testing/README.md) |
| Branch | `main` |

**Personal Databricks**

| Item | Value |
|---|---|
| Host | `<personal-workspace-host>.cloud.databricks.com` |
| User | *(redacted — personal account)* |
| CLI profile | `personal-yahoo` |
| Bundle targets | `personal`, `personal_dev`, `personal_prod` |
| Purpose | Independent experimentation, development, and bundle validation outside the Academy workspace |

**SoftServe Academy Azure Databricks**

| Item | Value |
|---|---|
| Workspace | `<academy-workspace-host>.azuredatabricks.net` |
| User | *(redacted — academy account)* |
| CLI profile | `dev` |
| SQL Warehouse ID | `3ed106620db591d9` |
| Bundle targets | `azure_dev`, `azure_prod` (same physical workspace, logically separated) |

**Lab 07 — Azure Dev**

| Item | Value |
|---|---|
| Pipeline ID | `6dce9f29-5797-46c5-a868-29c630372e85` |
| Job ID | `298080250797662` |
| Workflow | `00_source_preparation → 01_quality_pipeline → 02_scd2_validation → 03_quarantine_review → 04_delta_constraints → 05_gx_validation → 06_reconciliation → 07_freshness_volume → 08_schema_contract → 09_monitoring_validation → 10_quality_scorecard → 11_final_gate` |
| Result | Tasks 00–11 succeeded |
| Validated table | `dbr_dev.parvinbadalov.business_license_validated` (`4bdf96bb-b2ae-491d-9564-770c44e53349`) |
| Monitor source / output | `business_license_validated` → `business_license_validated_profile_metrics` |

**Lab 07 — Azure Prod**

| Item | Value |
|---|---|
| Job | `1120028109458030` |
| Pipeline | `01bc9500-242e-4a9c-afef-2e8ba26611d8` |
| Dashboard | `01f1a4180c7515cbb67c460f5cfe1bbe` |
| Status | **Deployed only — job intentionally not executed** |

**Unity Catalog / Storage**

| Item | Value |
|---|---|
| Catalog | `dbr_dev` |
| Schema | `parvinbadalov` |
| Volume | `dbr_dev.parvinbadalov.lab07_data_quality` |
| External ADLS location | `abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab07_data_quality` |

> Azure Dev and Azure Prod intentionally reuse the same Unity Catalog schema and external volume for this academy project. This is a logical, not physical, Dev/Prod separation — see below.

---

# Environment Strategy and Dev → Prod Promotion

## 1. Why two environments

This project runs across two Databricks environments: a **Personal Databricks workspace** and the **SoftServe Academy Azure Databricks workspace**. They serve different purposes and are not redundant with each other.

**Personal (`personal`, `personal_dev`, `personal_prod`)** — used for independent experimentation: validating bundle syntax, testing new pipeline logic, and practicing controlled promotion patterns before anything touches shared Academy infrastructure. `personal_dev` is where actual development and execution happens; `personal_prod` exists to practice production-style promotion discipline, not to represent enterprise-grade production.

**Azure (`azure_dev`, `azure_prod`)** — the actual Academy submission environment. `azure_dev` is where every Lab 07 artifact was built, validated, and executed: bundle deployment, Lakeflow pipeline runs, Databricks Jobs, SCD2 validation, data-quality checks, monitoring, and dashboard evidence. `azure_prod` is the promotion target used to demonstrate a controlled release process on top of that validated work.

## 2. Target comparison

| Target | Workspace | Purpose | Executed? |
|---|---|---|---|
| `personal` | Personal | Default/neutral personal target | — |
| `personal_dev` | Personal | Development, testing, bundle validation | Yes |
| `personal_prod` | Personal | Practice production-style promotion | As needed |
| `azure_dev` | Academy Azure | Full Lab 07 build, run, test, validate, capture evidence | **Yes — 00–11 succeeded** |
| `azure_prod` | Academy Azure | Controlled promotion target | **Deployed only, not run** |

## 3. Dev → Prod promotion flow

```
Local development repository
        │
        ▼
       Git
        │
        ▼
   GitHub main
        │
        ▼
Databricks workspace repository
        │
        ▼
     azure_dev
        │
   validate → deploy → run → tests → monitor → dashboard → evidence
        │
        ▼
     azure_prod
        │
   validate → plan → deploy
        │
   (NO automatic Prod run)
```

`azure_dev` is where runtime validation happens first — every task in the Lab 07 job (source preparation through the final quality gate) ran and succeeded there, and that run produced the evidence (dashboard, monitor, test results) this project is graded on. `azure_prod` is used strictly as a controlled promotion stage: the bundle was validated, planned, and deployed against it, but the job was **deliberately not executed**, since the point at this stage is to demonstrate that promotion is possible and repeatable, not to duplicate the Dev run.

## 4. Physical vs. logical separation — an important clarification

`azure_dev` and `azure_prod` are **not** two separate Azure Databricks workspaces. They point at the **same physical workspace**. The separation between them is entirely logical, coming from:

- separate Databricks Asset Bundle targets
- separate bundle deployment state
- separate bundle root paths
- Dev vs. Prod configuration overrides
- a controlled promotion workflow with explicit resource selection

For this academy project, Dev and Prod also intentionally **share the same Unity Catalog schema and external ADLS volume** rather than each getting its own — the existing volume was bound to the Prod bundle deployment state rather than recreated separately.

> The academy implementation uses logically separated Databricks Asset Bundle targets within a shared Azure Databricks workspace. It demonstrates controlled Dev → Prod promotion without claiming full physical infrastructure isolation.

## 5. How a real enterprise environment would differ

A production enterprise setup would typically go further, with:

- Separate DEV / TEST-QA / PROD **workspaces**, not just bundle targets
- Separate Unity Catalog catalogs and schemas per environment
- Separate ADLS containers or storage accounts per environment
- Separate service principals and managed identities per environment (not individual-user credentials)
- Environment-scoped secrets
- Production-only permission boundaries
- CI/CD deployment approval gates and protected Git branches
- Automated quality gates blocking promotion on failure
- Deployment executed by service principals, not personal accounts

This academy setup demonstrates the *pattern* of that workflow — bundle-based promotion, Dev-first validation, controlled Prod deployment — using shared infrastructure appropriate to a training environment, not enterprise-scale isolation.

## 6. Lab 07 resource architecture

```
resources/
├── lab07_infrastructure.yml
├── lab07_quality_pipeline.yml
├── lab07_data_quality_job.yml
├── lab07_monitoring.yml
└── lab07_dashboard.yml

labs/lab_07_data_quality_testing/
├── pipeline/
│   ├── bronze.py
│   ├── quality.py
│   ├── scd2.py          # dp.create_auto_cdc_from_snapshot_flow(), keys=["license_number"], stored_as_scd_type=2
│   └── gold.py
└── src/
    ├── lab07/            # importable package (root_path = src/)
    └── tests/             # native Lakeflow TestPipeline tests (Editor-managed)
        ├── test_expectations.py
        ├── test_quarantine.py
        ├── test_gold_quality_flow.py
        └── test_scd2_snapshot_flow.py
```

`dim_license_scd2` is built from the `business_license_snapshot_feed`, with configured snapshot cutoffs at `2024-12-31`, `2025-12-31`, and `2026-08-15`. The pipeline task (`01_quality_pipeline`) creates the SCD2 history; `02_scd2_validation.ipynb` runs afterward, purely as a read-and-validate step confirming current-row uniqueness and history — it does not itself run or recreate the pipeline.

## 7. Validated results (Azure Dev)

| Check | Result |
|---|---|
| Native Lakeflow TestPipeline | 4/4 passed |
| Local pytest | 16 passed, 1 deselected |
| Chispa | 3 passed |
| Ruff / Black | passed |
| Full job workflow (00–11) | Succeeded |
| Overall Quality Score | 99.56% |
| Total rows | 84,281 |
| Trusted | 83,910 |
| Quarantined | 371 |
| Warnings | 1 |

Quarantine breakdown by dimension: **consistency** 323, **validity** 40, **uniqueness** 8 (quarantine); **completeness** 1 (warning).

## 8. What this demonstrates

Running Lab 07 across two environments — one disposable and personal, one shared and Academy-graded — mirrors a real Dev/Prod discipline: validate cheaply and iterate freely in a low-stakes environment, then promote deliberately through a shared environment with explicit, auditable steps (validate → plan → deploy), stopping short of an automatic production run until that step is genuinely warranted. The logical-target pattern (`azure_dev`/`azure_prod` on one workspace) is a reasonable stand-in for full physical isolation at academy scale, while still exercising the actual bundle-based promotion mechanics an enterprise setup would use.

---

*Figures in Section 7 reflect the validated Azure Dev execution evidence captured for Lab 07 (`evidence/screenshots/`). Live values may change if the pipeline is executed again with newer source data.*

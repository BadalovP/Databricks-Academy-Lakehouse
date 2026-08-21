# Lab 06 External V2 — Shared External Delta Gold

## Why this version exists

The original Lab 06 uses the same ADLS-backed source Volume in both workspaces,
but its Gold tables are registered independently in Unity Catalog. Sharing the
source storage alone does **not** make `dim_*`, `fact_*`, and `agg_*` tables
appear in another workspace.

External V2 tests a different architecture:

```text
Personal / source workspace
        |
        | build Gold once
        v
Shared ADLS Delta paths
        |
        +-----------------------------+
        |                             |
        v                             v
Personal UC metadata           Azure UC metadata
(register paths)               (register same paths)
        |                             |
        +------------+----------------+
                     |
               same physical data
```

The goal is to prove that the second workspace can use the **same physical Gold
Delta data without rerunning the Gold transformations**.

## Safety

This project is isolated from the original Lab 06:

- source data is read from the existing `lab06_gold_analytics` Volume;
- External V2 uses a new UC schema: `dbr_dev.parvinbadalov_lab06_ext`;
- External V2 writes to a new sibling ADLS path:
  `.../lab06_gold_external_v2`;
- it does not overwrite the V1 Gold tables;
- resource keys are different from V1.

Do **not** put external table paths inside the existing Volume path. Tables and
Volumes should not overlap physical storage locations.

## Physical layout

```text
ADLS container root
├── lab06_gold_analytics/          # existing external Volume / source files
└── lab06_gold_external_v2/        # NEW shared Delta root
    ├── dim_date/
    ├── dim_patient/
    ├── dim_provider/
    ├── dim_organization/
    ├── dim_payer/
    ├── dim_condition/
    ├── fact_encounters/
    ├── fact_conditions/
    ├── agg_daily_encounters/
    ├── agg_organization_performance/
    ├── agg_payer_performance/
    ├── agg_condition_summary/
    └── lab06_data_volume_metrics/
```

## Important prerequisite

Both Databricks workspaces must have permission to create/read external tables
under:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2
```

The fact that the existing external Volume can be deployed is a good sign, but
table creation/registration still requires the relevant Unity Catalog/external
location permissions.

## Project structure

```text
labs/lab_06_gold_analytics_external/
├── README.md
├── notebooks/
│   ├── lab06e_00_dev_runner.ipynb
│   ├── lab06e_01_dimensions.ipynb
│   ├── lab06e_02_fact_encounters.ipynb
│   ├── lab06e_03_fact_conditions.ipynb
│   ├── lab06e_04_aggregations.ipynb
│   ├── lab06e_05_register_shared_tables.ipynb
│   ├── lab06e_06_alert_metrics.ipynb
│   └── lab06e_07_validation.ipynb
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── runtime_config.py
│   └── external_tables.py
├── sql/
│   └── cross_workspace_validation.sql
├── tests/
│   └── test_external_config.py
├── tools/
│   └── rewrite_exported_assets.py
├── dashboards/
└── genie/

resources/
├── lab06_external_schema.yml
├── lab06_external_gold_job.yml
├── lab06_external_register_job.yml
├── lab06_external_metrics_job.yml
├── lab06_external_alert.yml
├── lab06_external_dashboard.yml.template
└── lab06_external_genie.yml.template
```

## Parameter design

The recurring External V2 notebooks do not create widgets.

Parameters live in Job YAML and are read centrally by `src/runtime_config.py`.

The only notebook with visible widgets is the optional manual development
runner.

## Test sequence

### 1. Copy the package into the repository

Copy:

```text
labs/lab_06_gold_analytics_external/
resources/lab06_external_*
```

into the existing repository.

### 2. Merge the root variable patch

Open:

```text
databricks_external_v2_patch.yml.txt
```

and merge those variable definitions into the existing top-level `variables:`
section of `databricks.yml`.

Do not create a second `variables:` block.

### 3. Validate personal target

```bash
databricks bundle validate -t personal_dev
```

### 4. Deploy External V2 schema + source build Job in personal

```bash
databricks bundle deploy \
  -t personal_dev \
  --select schemas.lab06_external_schema

databricks bundle deploy \
  -t personal_dev \
  --select jobs.lab06_external_gold_job
```

### 5. Run the External V2 build **only in personal**

```bash
databricks bundle run \
  -t personal_dev \
  lab06_external_gold_job
```

Expected physical tables:

```text
abfss://.../lab06_gold_external_v2/dim_date
...
abfss://.../lab06_gold_external_v2/fact_encounters
...
```

and UC metadata:

```text
dbr_dev.parvinbadalov_lab06_ext.dim_date
dbr_dev.parvinbadalov_lab06_ext.fact_encounters
...
```

### 6. Create shared alert metrics in personal

```bash
databricks bundle deploy \
  -t personal_dev \
  --select jobs.lab06_external_metrics_job

databricks bundle run \
  -t personal_dev \
  lab06_external_metrics_job
```

This writes:

```text
.../lab06_gold_external_v2/lab06_data_volume_metrics
```

### 7. Deploy schema + registration Job to Azure

```bash
databricks bundle validate \
  -t azure_dev \
  --profile AZURE_DEV

databricks bundle deploy \
  -t azure_dev \
  --profile AZURE_DEV \
  --select schemas.lab06_external_schema

databricks bundle deploy \
  -t azure_dev \
  --profile AZURE_DEV \
  --select jobs.lab06_external_register_job
```

### 8. Run the **registration-only** Azure Job

```bash
databricks bundle run \
  -t azure_dev \
  --profile AZURE_DEV \
  lab06_external_register_job
```

This is intentionally different from running the Gold build. It performs only:

```sql
CREATE TABLE ... USING DELTA LOCATION 'shared-path'
```

plus validation/counts.

It does **not** recalculate dimensions, facts, or aggregates.

### 9. Validate Azure sees the same data

Use `sql/cross_workspace_validation.sql`.

The important proof is:

```sql
DESCRIBE DETAIL dbr_dev.parvinbadalov_lab06_ext.fact_encounters;
```

The `location` should be the same shared ADLS path in both workspaces.

The expected encounter row count remains:

```text
61,459
```

### 10. Deploy the V2 Alert in Azure

After `lab06_data_volume_metrics` is registered:

```bash
databricks bundle deploy \
  -t azure_dev \
  --profile AZURE_DEV \
  --select alerts.lab06_external_healthcare_volume_drop
```

The schedule is PAUSED by default.

## Dashboard and Genie

The existing V1 serialized Dashboard and Genie files contain V1 Unity Catalog
table references.

After External V2 tables are registered, run locally:

```bash
python \
  labs/lab_06_gold_analytics_external/tools/rewrite_exported_assets.py \
  --repo-root .
```

It copies the existing V1 assets and replaces:

```text
dbr_dev.parvinbadalov.*
```

with:

```text
dbr_dev.parvinbadalov_lab06_ext.*
```

Then inspect the generated JSON files.

If correct, rename:

```text
resources/lab06_external_dashboard.yml.template
→ resources/lab06_external_dashboard.yml

resources/lab06_external_genie.yml.template
→ resources/lab06_external_genie.yml
```

Validate again before deployment.

Then:

```bash
databricks bundle deploy \
  -t azure_dev \
  --profile AZURE_DEV \
  --select dashboards.lab06_external_healthcare_dashboard

databricks bundle deploy \
  -t azure_dev \
  --profile AZURE_DEV \
  --select genie_spaces.lab06_external_healthcare_genie
```

Because the External V2 Gold tables already exist in Azure metadata after the
registration Job, Genie should now be able to validate its table references
without rerunning the Gold transformation Job in Azure.

## What this experiment proves

If successful:

```text
Personal workspace
  External Gold build                         ✅
  Shared Delta paths                          ✅

Azure workspace
  Gold transformation Job not required        ✅
  Register same Delta paths                   ✅
  Same row counts                             ✅
  Same physical table locations               ✅
  Genie can resolve Gold tables               ✅
  Alert can resolve metrics table             ✅
```

This is the main difference from Lab 06 V1.

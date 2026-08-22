# Lab 06 External V2 — Azure Runbook

This runbook describes how to deploy and test Lab 06 External V2 in the Azure / SoftServe workspace.

## 1. Prerequisites

Required:
- access to the Azure Databricks workspace;
- Databricks CLI authenticated to the workspace;
- permission to read/write the configured ADLS external location;
- access to the Unity Catalog catalog/schema;
- access to the configured SQL warehouse;
- source external Volume `dbr_dev.parvinbadalov.lab06_gold_analytics`.

Current Azure workspace:

```text
https://adb-7405604503619901.1.azuredatabricks.net
```

Current V2 output:

```text
Catalog:       dbr_dev
Source schema: parvinbadalov
Source Volume: lab06_gold_analytics
Target schema: parvinbadalov_lab06_ext
Gold ADLS root:
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2
```

## 2. One-time source preparation

If the source Volume already contains the Synthea CSVs, skip this section.

Otherwise open and run:

```text
labs/lab_06_gold_analytics_external/notebooks/lab06_00_source_preparation.ipynb
```

It prepares:
- `patients.csv`
- `encounters.csv`
- `providers.csv`
- `organizations.csv`
- `payers.csv`
- `conditions.csv`

under:

```text
/Volumes/dbr_dev/parvinbadalov/lab06_gold_analytics/source/csv
```

Do not run source preparation as part of the recurring Gold workflow.

## 3. Authenticate the Databricks CLI

Use a local profile that authenticates to the Azure workspace.

Example:

```bash
databricks auth login \
  --host https://adb-7405604503619901.1.azuredatabricks.net \
  --profile <AZURE_PROFILE>
```

Verify:

```bash
databricks current-user me --profile <AZURE_PROFILE>
```

## 4. Validate the bundle

From the repository root:

```bash
databricks bundle validate \
  -t azure_dev \
  --profile <AZURE_PROFILE>
```

Expected:

```text
Validation OK!
```

## 5. Deploy only Lab 06 External V2 resources

```bash
databricks bundle deploy -t azure_dev \
  --profile <AZURE_PROFILE> \
  --select jobs.lab06_external_gold_job,jobs.lab06_external_metrics_job,jobs.lab06_external_register_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

This avoids deploying unrelated lab resources from the root bundle.

## 6. Run the Gold Build

```bash
databricks bundle run -t azure_dev \
  lab06_external_gold_job \
  --profile <AZURE_PROFILE>
```

Expected task order:

```text
01_dimensions
      ↓
02_fact_encounters
      ↓
03_fact_conditions
      ↓
04_aggregations
      ↓
07_validation
```

Expected final state:

```text
TERMINATED SUCCESS
```

## 7. Refresh alert metrics

```bash
databricks bundle run -t azure_dev \
  lab06_external_metrics_job \
  --profile <AZURE_PROFILE>
```

Expected:

```text
TERMINATED SUCCESS
```

## 8. Register/repair shared Gold metadata

Run after Gold data exists:

```bash
databricks bundle run -t azure_dev \
  lab06_external_register_job \
  --profile <AZURE_PROFILE>
```

Expected:

```text
TERMINATED SUCCESS
```

This job does not rebuild the Gold data. It registers the existing external Delta directories in Unity Catalog.

## 9. Verify the physical Delta location

Run in SQL Editor:

```sql
DESCRIBE DETAIL dbr_dev.parvinbadalov_lab06_ext.fact_encounters;
```

Verify that `location` points to:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2/fact_encounters
```

## 10. Verify counts

```sql
SELECT 'fact_encounters' AS object_name, COUNT(*) AS row_count
FROM dbr_dev.parvinbadalov_lab06_ext.fact_encounters

UNION ALL

SELECT 'fact_conditions', COUNT(*)
FROM dbr_dev.parvinbadalov_lab06_ext.fact_conditions

UNION ALL

SELECT 'agg_daily_encounters', COUNT(*)
FROM dbr_dev.parvinbadalov_lab06_ext.agg_daily_encounters;
```

Observed expected values for the fixed Synthea sample:

```text
fact_encounters       61459
fact_conditions       38094
agg_daily_encounters  17019
```

## 11. Test the Dashboard

Open:

```text
[dev parvinbadalov] Healthcare Operations & Cost Analytics - External V2
```

Expected top KPIs:
- 61.46K encounters
- 1.16K unique patients
- $255.03M total claim cost
- $63.53M payer coverage
- 400.2 min average duration
- $191.5M patient responsibility
- 3.5% emergency

If the dashboard stays on Loading, verify/start the Azure SQL warehouse.

## 12. Test Genie

Open:

```text
[dev parvinbadalov] Lab 06 External V2 - Healthcare Analytics Genie
```

Ask:

```text
How many total encounters and unique patients are there?
```

Expected:

```text
61,459 total encounters
1,163 unique patients
```

## 13. Test the SQL Alert

Open:

```text
[dev parvinbadalov] Lab 06 External V2 - Healthcare Volume Drop Alert
```

Click **Run now**.

Expected condition:

```text
FIRST_ROW(should_alert) = 1
```

Expected state:

```text
TRIGGERED
```

The configured Azure subscriber is:

```text
parvinbadalov@softserve.academy
```

## 14. Final bundle check

```bash
databricks bundle summary \
  -t azure_dev \
  --profile <AZURE_PROFILE>
```

Confirm URLs exist for:
- `lab06_external_gold_job`
- `lab06_external_metrics_job`
- `lab06_external_register_job`
- `lab06_external_healthcare_dashboard`
- `lab06_external_healthcare_genie`
- `lab06_external_healthcare_volume_drop`

## Recommended manager execution sequence

For an already-prepared Azure environment:

```text
1. bundle validate
2. bundle deploy --select Lab 06 V2 resources
3. run Shared Gold Build
4. run Shared Alert Metrics
5. run Register Shared Gold
6. test Dashboard
7. test Genie
8. Run now on SQL Alert
```

For a fresh environment, perform source/external-location prerequisites first.

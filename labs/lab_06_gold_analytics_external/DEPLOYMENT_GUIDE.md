# Lab 06 External V2 — Deployment Guide

## Final Deployment Model

Lab 06 supports four Databricks Asset Bundle targets:

```text
azure_dev
azure_prod
personal_dev
personal_prod
```

Compute model:

```text
Azure targets    -> existing all-purpose cluster (GP1 / GP2 / auto)
Personal targets -> Serverless
```

The final Lab 06 deployment uses these resources:

```text
jobs.lab06_external_gold_job
dashboards.lab06_external_healthcare_dashboard
genie_spaces.lab06_external_healthcare_genie
alerts.lab06_external_healthcare_volume_drop
```

The target schema is intentionally not part of the normal selective deployment set.

---

## Prerequisites

Before running the deployment commands:

1. Configure a Databricks CLI profile for the target workspace.
2. Confirm that the bundle target exists in `databricks.yml`.
3. For Azure targets, confirm that an existing all-purpose cluster is available.
4. Confirm access to the shared Lab 06 source and external ADLS Gold location.

General placeholders used in this guide:

```text
<databricks-cli-profile>
<azure-target>
<personal-target>
```

---

## Validate a Target

Use:

```bash
databricks bundle validate \
  -t <bundle-target> \
  --profile <databricks-cli-profile>
```

Example target values:

```text
azure_dev
azure_prod
personal_dev
personal_prod
```

---

## Plan the Final Lab 06 Resources

Use the same final resource selection for any target:

```bash
databricks bundle plan \
  -t <bundle-target> \
  --profile <databricks-cli-profile> \
  --select jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

For Azure targets, a cluster can also be supplied explicitly:

```bash
databricks bundle plan \
  -t <azure-target> \
  --profile <databricks-cli-profile> \
  --var lab06_cluster_id=<existing-cluster-id> \
  --select jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

Before deployment, confirm that the plan does not contain unexpected resource
deletions or unrelated Lab changes.

---

## Deploy the Final Lab 06 Resources

General deployment:

```bash
databricks bundle deploy \
  -t <bundle-target> \
  --profile <databricks-cli-profile> \
  --select jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

Azure deployment with an explicit existing cluster:

```bash
databricks bundle deploy \
  -t <azure-target> \
  --profile <databricks-cli-profile> \
  --var lab06_cluster_id=<existing-cluster-id> \
  --select jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

Production deployment does not require immediately running the Job. A production
target may remain deployment-ready until an execution is explicitly required.

---

## Running Lab 06

The repository runner provides a consistent way to validate, optionally deploy,
and execute the final Lab 06 Job.

### General form

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --target <bundle-target> \
  --profile <databricks-cli-profile>
```

### Azure targets

For Azure targets, select an existing cluster:

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --cluster <gp1|gp2|auto> \
  --target <azure-target> \
  --profile <databricks-cli-profile>
```

Cluster modes:

```text
gp1   -> use configured GP1
gp2   -> use configured GP2
auto  -> use an available configured Azure cluster
```

### Personal targets

Personal targets use Serverless compute, so a cluster option is not required:

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --target <personal-target> \
  --profile <databricks-cli-profile>
```

If the resources were already deployed and only Job execution is required:

```bash
bash tools/run_academy_lab.sh \
  --lab 06 \
  --target <bundle-target> \
  --profile <databricks-cli-profile> \
  --skip-deploy
```

Production execution should always be intentional.

---

## Final Job

Bundle resource:

```text
jobs.lab06_external_gold_job
```

Job name:

```text
Lab 06 External V2 - End-to-End Gold Analytics
```

Task graph:

```text
01_dimensions
      |
      v
02_fact_encounters
      |
      v
03_fact_conditions
      |
      v
04_aggregations
   /          \
  v            v
05_register   06_alert_metrics
   \          /
      v      v
    07_validation
```

The final design uses one end-to-end Job.

---

## Recorded Validation Results

The implementation has been independently validated on the Personal targets:

| Target | Compute | Result |
|---|---|---|
| `personal_dev` | Serverless | 7 tasks succeeded |
| `personal_prod` | Serverless | 7 tasks succeeded |

The Azure targets are configured for shared-workspace deployment and execution
with an existing all-purpose cluster.

---

## Post-Deployment Check

Check the resolved resources for a target:

```bash
databricks bundle summary \
  -t <bundle-target> \
  --profile <databricks-cli-profile>
```

The final Lab 06 resources should appear as deployed:

```text
lab06_external_gold_job
lab06_external_healthcare_dashboard
lab06_external_healthcare_genie
lab06_external_healthcare_volume_drop
```

---

## Dashboard Test

Open:

```text
Healthcare Operations & Cost Analytics - External V2
```

Confirm that the dashboard loads data from:

```text
dbr_dev.parvinbadalov_lab06_ext
```

Validated headline metrics are approximately:

```text
Total Encounters         61,459
Unique Patients           1,163
Total Claim Cost        $255.03M
Payer Coverage           $63.53M
Patient Responsibility  $191.50M
Emergency Encounter %      ~3.5%
```

---

## Genie Test

Open:

```text
Lab 06 External V2 - Healthcare Analytics Genie
```

Suggested validation questions:

```text
Which organizations had the most encounters?

What were the top 10 medical conditions by number of events?

Which payer covered the most healthcare cost?
```

The expected sources are the final Gold fact and aggregate tables.

---

## Alert Test

Open:

```text
Lab 06 External V2 - Healthcare Volume Drop Alert
```

The deployed alert reads:

```sql
SELECT
  should_alert,
  alert_status,
  data_source,
  test_month,
  baseline_encounter_count,
  observed_encounter_count,
  volume_drop_pct,
  drop_threshold_pct,
  generated_at
FROM dbr_dev.parvinbadalov_lab06_ext.lab06_data_volume_metrics
LIMIT 1;
```

Expected demonstration state:

```text
should_alert          = 1
alert_status          = TRIGGERED
baseline              = 326
observed              = 65
volume_drop_pct       = 80.06
drop_threshold_pct    = 30
```

Keep the automated alert schedule paused after testing so the static simulated
condition does not repeatedly send notifications.

### SQL Editor note

Bundle variables such as:

```text
${var.lab06_external_catalog}
${var.lab06_external_target_schema}
```

are resolved during bundle deployment.

When running SQL manually in the Databricks SQL Editor, use the concrete object
name instead:

```text
dbr_dev.parvinbadalov_lab06_ext.lab06_data_volume_metrics
```

---

## Shared External Storage

All Lab 06 targets reference the same External V2 ADLS Gold root.

Therefore:

```text
Do not run Lab 06 targets concurrently.
```

Sequential execution avoids overlapping writes against the same external Delta
locations.

---

## Deployment Summary

```text
Validate target
      |
      v
Plan selected Lab 06 resources
      |
      v
Deploy selected resources
      |
      +----> Dashboard
      +----> Genie
      +----> SQL Alert
      |
      v
Run End-to-End Gold Job when required
      |
      v
Validate Dashboard / Genie / Alert
```

This guide intentionally uses generic CLI profile, target, and cluster
placeholders so it can be reused from different workstations and Databricks
workspaces.

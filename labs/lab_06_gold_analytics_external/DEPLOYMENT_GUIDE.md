# Lab 06 External V2 — Deployment Guide

This guide is for developers/maintainers who need to deploy Lab 06 External V2 from the repository or change Bundle-managed configuration.

Reviewers who only want to test the existing deployment should use `HOW_TO_RUN.md`.

## 1. Prerequisites

Required:
- Azure Databricks workspace access;
- Databricks CLI authentication;
- Unity Catalog access;
- access to the configured ADLS external location;
- source Volume `dbr_dev.parvinbadalov.lab06_gold_analytics`;
- Synthea source CSVs;
- access to the configured SQL warehouse.

## 2. Validate the Bundle

```bash
databricks bundle validate -t azure_dev \
  --profile <AZURE_PROFILE>
```

## 3. Deploy Lab 06 V2

One deployment command deploys:
- the one End-to-End Job;
- Dashboard;
- Genie;
- SQL Alert.

```bash
databricks bundle deploy -t azure_dev \
  --profile <AZURE_PROFILE> \
  --select jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop
```

## 4. Alert subscriber

The Bundle-managed subscriber is controlled by:

```yaml
lab06_alert_subscriber_email: "<workspace-user-email>"
```

Set this to the workspace user's email before deployment if that user should be the persistent alert recipient.

For a temporary manual test, the user can instead add themselves in the Alert UI under **Notifications**.

## 5. Run

After deployment:

```bash
databricks bundle run -t azure_dev \
  lab06_external_gold_job \
  --profile <AZURE_PROFILE>
```

Or use **Run now** in the Databricks UI.

## 6. Source bootstrap

If source files are missing, run the one-time notebook:

```text
labs/lab_06_gold_analytics_external/notebooks/lab06_00_source_preparation.ipynb
```

This is not a recurring Job task.

# Lab 06 External V2 — How to Test the Existing Azure Deployment

This guide is for a reviewer, manager, or teammate who wants to test the already-deployed Lab 06 External V2 solution.

You do **not** need to validate or deploy the Bundle just to test it.

## 1. Run the End-to-End Job

In the Azure Databricks workspace open:

```text
Jobs & Pipelines
→ [dev parvinbadalov] Lab 06 External V2 - End-to-End Gold Analytics
→ Run now
```

Expected DAG:

```text
01_dimensions
      ↓
02_fact_encounters
      ↓
03_fact_conditions
      ↓
04_aggregations
   ↙          ↘
05_register   06_alert_metrics
   ↘          ↙
      07_validation
```

Expected result:

```text
Succeeded
```

All seven tasks should be green.

## 2. Test the Dashboard

Open:

```text
Dashboards
→ [dev parvinbadalov] Healthcare Operations & Cost Analytics - External V2
```

Expected top-level values include:

```text
61.46K  Total Encounters
1.16K   Unique Patients
$255.03M Total Claim Cost
$63.53M  Total Payer Coverage
400.2    Avg Encounter Duration
$191.5M  Patient Responsibility
3.5%     Emergency %
```

## 3. Test Genie

Open:

```text
Genie Agents
→ [dev parvinbadalov] Lab 06 External V2 - Healthcare Analytics Genie
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

## 4. Test the SQL Alert

Open:

```text
Alerts
→ [dev parvinbadalov] Lab 06 External V2 - Healthcare Volume Drop Alert
→ Run now
```

Expected:

```text
TRIGGERED
```

The alert uses the monitoring row prepared by task `06_alert_metrics`.

### Receive the alert yourself

If you are a Databricks workspace user and want the notification sent to your own email for this test:

```text
Alert → Edit → Notifications → add your workspace user/email
```

Then save and click **Run now**.

This is suitable for a temporary manual test. A later Bundle deployment may restore the Bundle-managed subscriber configuration.

## 5. Optional data checks

Open Catalog Explorer:

```text
dbr_dev.parvinbadalov_lab06_ext
```

Expected key counts:

```text
fact_encounters       61,459
fact_conditions       38,094
agg_daily_encounters  17,019
```

## That's all

For an already-deployed environment, the reviewer workflow is simply:

```text
Run Job
→ Open Dashboard
→ Ask Genie
→ Run Alert
```

No CLI deployment step is required.

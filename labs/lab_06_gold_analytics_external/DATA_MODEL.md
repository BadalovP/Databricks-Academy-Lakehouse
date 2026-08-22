# Lab 06 External V2 — Data Model

## Purpose

This document describes the final Gold analytical model used by Lab 06.

Target schema:

```text
dbr_dev.parvinbadalov_lab06_ext
```

Physical Delta root:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2
```

---

## Star Schema

```text
                         dim_date
                            |
                            |
dim_patient ----- fact_encounters ----- dim_provider
      |                 |   |
      |                 |   +---------- dim_payer
      |                 |
      |                 +-------------- dim_organization
      |
      +------- fact_conditions -------- dim_condition
```

---

## Dimensions

### `dim_date`

Grain: one row per calendar date.

The dimension is generated from the configured date range rather than only from
dates appearing in fact data.

### `dim_patient`

Grain: one row per synthetic patient.

### `dim_provider`

Grain: one row per healthcare provider.

### `dim_organization`

Grain: one row per healthcare organization.

### `dim_payer`

Grain: one row per payer.

### `dim_condition`

Grain: one row per medical condition code/description.

---

## Facts

### `fact_encounters`

Grain: one row per healthcare encounter.

Main relationships:

```text
date_key
patient_key
provider_key
organization_key
payer_key
```

Main measures:

```text
duration_minutes
base_encounter_cost
total_claim_cost
payer_coverage
patient_responsibility
```

Patient responsibility:

```text
total_claim_cost - payer_coverage
```

Validated row count:

```text
61,459
```

### `fact_conditions`

Grain: one patient-condition occurrence/event.

Main relationships:

```text
patient_key
condition_key
encounter_key
condition_start_date_key
```

Validated row count:

```text
38,094
```

---

## Business Aggregates

### `agg_daily_encounters`

Typical measures:

```text
encounter_count
unique_patients
organizations_active
providers_active
avg_duration_minutes
total_claim_cost
payer_coverage
patient_responsibility
emergency_encounters
emergency_encounter_pct
```

### `agg_organization_performance`

One row per healthcare organization.

### `agg_payer_performance`

One row per payer.

### `agg_condition_summary`

One row per medical condition.

---

## Monitoring Table

### `lab06_data_volume_metrics`

Important columns:

```text
should_alert
alert_status
data_source
test_month
baseline_encounter_count
observed_encounter_count
volume_drop_pct
drop_threshold_pct
generated_at
```

Designed demonstration state:

```text
baseline_encounter_count = 326
observed_encounter_count = 65
volume_drop_pct          = 80.06
drop_threshold_pct       = 30
should_alert             = 1
alert_status             = TRIGGERED
```

---

## Physical Storage

Each Gold table is written as Delta under the shared external root, for example:

```text
.../lab06_gold_external_v2/dim_patient
.../lab06_gold_external_v2/fact_encounters
.../lab06_gold_external_v2/agg_daily_encounters
.../lab06_gold_external_v2/lab06_data_volume_metrics
```

Example:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2/fact_encounters
```

Because multiple targets use the same physical external root, target runs should
be performed sequentially.

# Lab 06 External V2 — Data Model

## Purpose

This document describes the final Gold analytical and governance model used by Lab 06.

Target schema:

```text
dbr_dev.parvinbadalov_lab06_ext
```

Physical external Delta root:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2
```

---

## Logical Model

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

The base Gold model is complemented by secure governance views for controlled consumption.

---

## Dimensions

| Table | Grain |
|---|---|
| `dim_date` | One row per calendar date in the configured date range |
| `dim_patient` | One row per synthetic patient |
| `dim_provider` | One row per healthcare provider |
| `dim_organization` | One row per healthcare organization |
| `dim_payer` | One row per payer |
| `dim_condition` | One row per medical condition code/description |

`dim_date` is generated from the configured range rather than only from dates observed in fact data.

---

## Facts

### `fact_encounters`

Grain: one row per healthcare encounter.

Main foreign-key relationships:

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

Patient responsibility is derived as:

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

Validated row count:

```text
17,019
```

### `agg_organization_performance`

Grain: one row per healthcare organization.

### `agg_payer_performance`

Grain: one row per payer.

### `agg_condition_summary`

Grain: one row per medical condition.

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

Controlled demonstration state:

```text
baseline_encounter_count = 326
observed_encounter_count = 65
volume_drop_pct          = 80.06
drop_threshold_pct       = 30
should_alert             = 1
alert_status             = TRIGGERED
```

---

## Governance Model

The pipeline writes the Gold base tables directly to external ADLS Delta paths. Governance is therefore implemented through secure views and helper mapping tables, leaving the base tables free of native row filters and column masks.

### Organization access mapping

```text
lab06_user_organization_access
```

Columns:

```text
user_email
organization_id
```

Semantics:

- a concrete organization ID allows that organization;
- `*` represents full organization access;
- no mapping returns no rows through the secure encounter view.

### Secure encounter view

```text
vw_fact_encounters_secure
```

Purpose: organization-based row-level security using `SESSION_USER()` and the organization access mapping.

Consumers who require governed encounter access should query this view instead of `fact_encounters`.

### Privileged patient-data mapping

```text
lab06_patient_data_privileged_users
```

Column:

```text
user_email
```

### Secure patient view

```text
vw_dim_patient_secure
```

Purpose: dynamic masking of sensitive patient fields for users not present in the privileged-user mapping.

Masked attributes:

```text
ssn
first_name
last_name
address
```

Masked output:

```text
***MASKED***
```

Privileged users receive the original values.


### Secure-view evaluation behavior

`vw_dim_patient_secure` evaluates the current session principal. A privileged user sees the original patient attributes; a non-privileged user sees `***MASKED***` for the protected fields.

The governance notebook's `run_demo=true` mode temporarily removes the invoking user's privileged mapping to prove the masked result, validates the output, and restores the mapping before completion. Consequently, a later `SELECT * FROM vw_dim_patient_secure` by the same restored user shows the original values. This is expected and is separate from the masked-demo evidence.


### Access design

A governed consumer should receive:

```text
USE CATALOG
USE SCHEMA
SELECT on vw_fact_encounters_secure
SELECT on vw_dim_patient_secure
```

and should not receive direct `SELECT` access to the governed base tables when the secure views are intended to enforce access.

---

## Physical Storage

Each Gold table is stored as Delta under the shared external root, for example:

```text
.../lab06_gold_external_v2/dim_patient
.../lab06_gold_external_v2/fact_encounters
.../lab06_gold_external_v2/agg_daily_encounters
.../lab06_gold_external_v2/lab06_data_volume_metrics
```

Example full path:

```text
abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2/fact_encounters
```

Because all deployment targets use the same physical External V2 Gold root, Lab 06 target executions should be run sequentially.

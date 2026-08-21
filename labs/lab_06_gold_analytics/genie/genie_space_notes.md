# Lab 06 — Genie Agent Notes

## Agent name
**Lab 06 — Healthcare Analytics Genie**

## Purpose
Allow business users to ask natural-language questions about healthcare operations, encounters, costs, payers, organizations, and medical conditions using the Lab 06 Gold model.

## Recommended data sources

Use these Unity Catalog tables:

- `dbr_dev.parvinbadalov.fact_encounters`
- `dbr_dev.parvinbadalov.fact_conditions`
- `dbr_dev.parvinbadalov.dim_date`
- `dbr_dev.parvinbadalov.dim_patient`
- `dbr_dev.parvinbadalov.dim_provider`
- `dbr_dev.parvinbadalov.dim_organization`
- `dbr_dev.parvinbadalov.dim_payer`
- `dbr_dev.parvinbadalov.dim_condition`
- `dbr_dev.parvinbadalov.agg_daily_encounters`
- `dbr_dev.parvinbadalov.agg_organization_performance`
- `dbr_dev.parvinbadalov.agg_payer_performance`
- `dbr_dev.parvinbadalov.agg_condition_summary`

## General instructions

Use the Gold fact and dimension tables for detailed analysis and the aggregate tables for common dashboard-style questions.

Business definitions:

- An encounter is one healthcare encounter in `fact_encounters`.
- A condition event is one patient-condition occurrence in `fact_conditions`.
- Total healthcare cost for encounter analysis is `total_claim_cost`.
- Payer contribution is `payer_coverage`.
- Patient out-of-pocket responsibility is `patient_responsibility`.
- Encounter duration is measured in `duration_minutes`.
- Emergency encounter percentage is the number of encounters where `encounter_class = 'emergency'` divided by total encounters.
- Organization performance should use `agg_organization_performance` when possible.
- Payer performance should use `agg_payer_performance` when possible.
- Condition rankings should use `agg_condition_summary` when possible.
- Time-series encounter questions should use `agg_daily_encounters` when possible.
- Prefer organization names, payer names, condition descriptions, and readable dates in final answers instead of surrogate keys.
- Do not expose masked patient-identifying values to users who are not authorized by Unity Catalog policies.
- Respect the Unity Catalog row filter and column masks already applied by Lab 06 governance.

## Recommended joins

- `fact_encounters.patient_key = dim_patient.patient_key`
- `fact_encounters.provider_key = dim_provider.provider_key`
- `fact_encounters.organization_key = dim_organization.organization_key`
- `fact_encounters.payer_key = dim_payer.payer_key`
- `fact_encounters.date_key = dim_date.date_key`
- `fact_conditions.patient_key = dim_patient.patient_key`
- `fact_conditions.condition_key = dim_condition.condition_key`
- `fact_conditions.encounter_key = fact_encounters.encounter_key`
- `fact_conditions.condition_start_date_key = dim_date.date_key`

## Sample questions

1. Which healthcare organizations had the highest number of encounters?
2. Which organizations had the highest total claim cost?
3. What is the monthly trend in encounter volume?
4. What percentage of encounters were emergency encounters each month?
5. Which payers covered the highest total amount?
6. Which payer had the highest average claim cost?
7. What are the most common medical conditions?
8. Which conditions affected the largest number of unique patients?
9. What is the average encounter duration by encounter class?
10. Which organizations had the highest patient responsibility?
11. Show total claim cost, payer coverage, and patient responsibility by month.
12. Which encounter classes are most common?

## Suggested validation questions

Use these after creating the Genie Agent and review the generated SQL:

- **Which organizations had the most encounters?**
  - Expected source: `agg_organization_performance`
  - Expected ordering: `encounter_count DESC`

- **What were the top 10 medical conditions by number of events?**
  - Expected source: `agg_condition_summary`
  - Expected ordering: `condition_event_count DESC`

- **Show monthly encounter volume in 2021.**
  - Expected source: `agg_daily_encounters`
  - Expected aggregation: month + `SUM(encounter_count)`

- **Which payer covered the most healthcare cost?**
  - Expected source: `agg_payer_performance`
  - Expected ordering: `payer_coverage DESC`

- **What is the average encounter duration by encounter class?**
  - Expected source: `fact_encounters`
  - Expected aggregation: `AVG(duration_minutes)` grouped by `encounter_class`

## Source control

After creating and testing the Genie Agent in the Databricks UI, export it into the bundle:

```bash
databricks bundle generate genie-space \
  --existing-id <GENIE_AGENT_ID> \
  --genie-space-dir labs/lab_06_gold_analytics/genie \
  --resource-dir resources \
  -t personal_dev
```

Then bind the generated resource to the existing Genie Agent before future deployments if the generated command/version does not bind automatically.

Databricks bundle support for Genie Agents requires the direct deployment engine.

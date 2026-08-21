-- LAB 06 — AI/BI Dashboard datasets
-- Dashboard: Healthcare Operations & Cost Analytics
-- Development objects: dbr_dev.parvinbadalov
--
-- In the AI/BI dashboard Data tab, add each SELECT below as a separate
-- SQL dataset and give it the dataset name shown in the section header.
-- These are read-only dashboard queries.

-- ============================================================
-- DATASET 1: overview_kpis
-- ============================================================
SELECT
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_key) AS unique_patients,
    COUNT(DISTINCT organization_key) AS organizations,
    ROUND(SUM(total_claim_cost), 2) AS total_claim_cost,
    ROUND(SUM(payer_coverage), 2) AS payer_coverage,
    ROUND(SUM(patient_responsibility), 2) AS patient_responsibility,
    ROUND(AVG(total_claim_cost), 2) AS avg_claim_cost,
    ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
    ROUND(
        100.0 * SUM(CASE
            WHEN LOWER(encounter_class) = 'emergency' THEN 1
            ELSE 0
        END) / COUNT(*),
        2
    ) AS emergency_encounter_pct
FROM dbr_dev.parvinbadalov.fact_encounters;


-- ============================================================
-- DATASET 2: daily_encounters
-- ============================================================
SELECT
    encounter_date,
    encounter_count,
    unique_patients,
    organizations_active,
    providers_active,
    avg_duration_minutes,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    patient_responsibility,
    emergency_encounters,
    emergency_encounter_pct
FROM dbr_dev.parvinbadalov.agg_daily_encounters
ORDER BY encounter_date;


-- ============================================================
-- DATASET 3: organization_performance
-- ============================================================
SELECT
    organization_key,
    organization_id,
    organization_name,
    city,
    state,
    encounter_count,
    unique_patients,
    unique_providers,
    avg_duration_minutes,
    total_claim_cost,
    avg_claim_cost,
    payer_coverage,
    patient_responsibility,
    first_encounter_date,
    last_encounter_date
FROM dbr_dev.parvinbadalov.agg_organization_performance;


-- ============================================================
-- DATASET 4: payer_performance
-- ============================================================
SELECT
    payer_key,
    payer_id,
    payer_name,
    state_headquartered,
    encounter_count,
    unique_patients,
    total_claim_cost,
    payer_coverage,
    patient_responsibility,
    avg_claim_cost,
    avg_payer_coverage,
    coverage_pct
FROM dbr_dev.parvinbadalov.agg_payer_performance;


-- ============================================================
-- DATASET 5: condition_summary
-- ============================================================
SELECT
    condition_key,
    condition_code,
    condition_description,
    condition_event_count,
    unique_patients,
    linked_encounters,
    active_condition_events,
    active_condition_pct,
    avg_condition_duration_days,
    first_recorded_date,
    last_recorded_date
FROM dbr_dev.parvinbadalov.agg_condition_summary;


-- ============================================================
-- DATASET 6: encounter_class_summary
-- ============================================================
SELECT
    encounter_class,
    COUNT(*) AS encounter_count,
    COUNT(DISTINCT patient_key) AS unique_patients,
    ROUND(SUM(total_claim_cost), 2) AS total_claim_cost,
    ROUND(AVG(total_claim_cost), 2) AS avg_claim_cost,
    ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes
FROM dbr_dev.parvinbadalov.fact_encounters
GROUP BY encounter_class
ORDER BY encounter_count DESC;


-- ============================================================
-- DATASET 7: monthly_trend
-- ============================================================
SELECT
    DATE_TRUNC('MONTH', encounter_date) AS encounter_month,
    SUM(encounter_count) AS encounter_count,
    SUM(total_claim_cost) AS total_claim_cost,
    SUM(payer_coverage) AS payer_coverage,
    SUM(patient_responsibility) AS patient_responsibility,
    ROUND(
        SUM(emergency_encounters) * 100.0 / SUM(encounter_count),
        2
    ) AS emergency_encounter_pct
FROM dbr_dev.parvinbadalov.agg_daily_encounters
GROUP BY DATE_TRUNC('MONTH', encounter_date)
ORDER BY encounter_month;

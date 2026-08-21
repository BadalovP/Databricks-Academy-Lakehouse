-- Lab 06 External V2 - cross-workspace validation
-- Run after the registration Job in the second workspace.

DESCRIBE DETAIL dbr_dev.parvinbadalov_lab06_ext.fact_encounters;

SELECT COUNT(*) AS fact_encounter_rows
FROM dbr_dev.parvinbadalov_lab06_ext.fact_encounters;

SELECT COUNT(*) AS fact_condition_rows
FROM dbr_dev.parvinbadalov_lab06_ext.fact_conditions;

SELECT COUNT(*) AS daily_aggregate_rows
FROM dbr_dev.parvinbadalov_lab06_ext.agg_daily_encounters;

SELECT *
FROM dbr_dev.parvinbadalov_lab06_ext.lab06_data_volume_metrics
LIMIT 1;

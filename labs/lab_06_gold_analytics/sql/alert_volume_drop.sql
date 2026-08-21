-- LAB 06 — Volume-drop SQL Alert query
-- The simulation notebook creates this metrics table.
--
-- Alert configuration:
--   Source column       : should_alert
--   Comparison operator : EQUAL
--   Threshold           : 1
--
-- The query intentionally always returns one row so that OK/TRIGGERED state
-- does not depend on empty-result handling.

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
FROM dbr_dev.parvinbadalov.lab06_data_volume_metrics
LIMIT 1;

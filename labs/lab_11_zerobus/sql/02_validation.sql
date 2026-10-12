-- Lab 11: reconciliation for one producer run (read-only).
-- Expected after a run with U unique events and R replays: bronze_rows = U + R,
-- unique_events = silver_rows = U, duplicate_deliveries = R, every other count = 0.

WITH b AS (
  SELECT * FROM ${catalog}.${schema}.${bronze} WHERE run_id = '${run_id}'
),
per_event AS (
  SELECT
    event_id,
    COUNT(*) AS deliveries,
    COUNT(DISTINCT concat_ws('|', device_id, producer_id, CAST(sequence_number AS STRING),
      CAST(event_timestamp AS STRING), CAST(temperature AS STRING), CAST(humidity AS STRING))) AS payloads
  FROM b
  GROUP BY event_id
),
s AS (
  SELECT * FROM ${catalog}.${schema}.${silver} WHERE run_id = '${run_id}'
)
SELECT
  (SELECT COUNT(*) FROM b) AS bronze_rows,
  (SELECT COUNT(*) FROM per_event) AS unique_events,
  (SELECT COALESCE(SUM(deliveries - 1), 0) FROM per_event) AS duplicate_deliveries,
  (SELECT COUNT(*) FROM per_event WHERE deliveries > 1) AS events_with_duplicates,
  (SELECT COUNT(*) FROM per_event WHERE payloads > 1) AS conflicting_events,
  (SELECT COUNT(*) FROM b WHERE event_id IS NULL OR device_id IS NULL OR event_timestamp IS NULL
     OR temperature IS NULL OR humidity IS NULL OR batch_id IS NULL OR produced_at IS NULL) AS null_required_fields,
  (SELECT COUNT(*) FROM s) AS silver_rows,
  (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM s) AS silver_duplicate_keys,
  (SELECT COUNT(*) FROM (SELECT event_id FROM per_event EXCEPT SELECT event_id FROM s)) AS missing_in_silver,
  (SELECT COUNT(*) FROM (SELECT event_id FROM s EXCEPT SELECT event_id FROM per_event)) AS unexpected_in_silver,
  (SELECT COUNT(*) FROM s JOIN per_event p USING (event_id) WHERE s.delivery_count <> p.deliveries) AS wrong_delivery_counts;

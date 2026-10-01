-- UrbanFlow AI/BI dashboard - DATA QUALITY section.
-- Prepared only; read-only SELECTs against persisted tables.
--
-- This section exists so a supervisor can tell a healthy pipeline from a quiet failure. The
-- most important property is that every layer's counts RECONCILE: a row that vanished between
-- Bronze and Silver is a bug, and a row that was rejected is a finding. Those are different,
-- and only an explicit identity can tell them apart.

-- 1. Medallion row counts and the reconciliation identity.
--    Bronze must equal Silver + Quarantine + duplicates. Anything else means a lost row.
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.bronze_station_status)     AS bronze_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status)     AS silver_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.quarantine_station_status) AS quarantine_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.duplicate_station_status)  AS duplicate_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.fact_station_availability) AS fact_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.gold_station_shortage)     AS shortage_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority) AS priority_rows
)
SELECT
  *,
  silver_rows + quarantine_rows + duplicate_rows                    AS accounted_rows,
  bronze_rows = silver_rows + quarantine_rows + duplicate_rows      AS bronze_reconciles,
  fact_rows = silver_rows                                           AS fact_matches_silver,
  -- The shortage and priority tables share a grain, so a divergence means one of them was
  -- written without the other's stale-row cleanup.
  shortage_rows = priority_rows                                     AS derived_tables_agree
FROM counts;

-- 2. Quarantined rows by the rule they failed. A growing quarantine is a finding to explain,
--    not necessarily a failure: a feed that genuinely degrades should show up here.
SELECT
  rule                                                              AS failed_rule,
  COUNT(*)                                                          AS rows_failing
FROM dbr_dev.parvinbadalov_urbanflow.quarantine_station_status
LATERAL VIEW EXPLODE(failed_rules) exploded AS rule
GROUP BY rule
ORDER BY rows_failing DESC;

-- 3. Freshness. The snapshot's own source timestamp, not the time it was loaded, because a
--    recently-loaded stale feed is still stale.
SELECT
  MIN(observed_at)                                                  AS oldest_observation,
  MAX(observed_at)                                                  AS newest_observation,
  MAX(ingested_at)                                                  AS last_ingested_at,
  BIGINT(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(observed_at)))
                                                                    AS age_seconds,
  -- One hour is the configured ceiling; the stored snapshot is deliberately older than that
  -- and reports is_fresh = false, which is the honest answer rather than a moved goalpost.
  (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(observed_at))) <= 3600 AS is_fresh
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status;

-- 4. Reference match rate: how many observations the 40-station development sample can name.
--    A low rate here is EXPECTED and is not a defect. The dimension is a committed sample, and
--    the fact join is a LEFT join precisely so a reference gap never deletes a real reading.
SELECT
  COUNT(*)                                                          AS fact_rows,
  SUM(CASE WHEN station_name IS NOT NULL THEN 1 ELSE 0 END)         AS named_from_reference_sample,
  SUM(CASE WHEN station_name IS NULL     THEN 1 ELSE 0 END)         AS not_in_reference_sample,
  ROUND(
    100.0 * SUM(CASE WHEN station_name IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2
  )                                                                 AS reference_match_percent,
  'the dimension is a committed 40-station sample, so a low rate is expected'
                                                                    AS reading_note
FROM dbr_dev.parvinbadalov_urbanflow.fact_station_availability;

-- 5. Uniqueness and completeness of the Silver key. event_id must be unique and non-null, or
--    the idempotent MERGE that writes this table could not be trusted.
SELECT
  COUNT(*)                                                          AS rows,
  COUNT(DISTINCT event_id)                                          AS distinct_event_ids,
  SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END)                 AS null_event_ids,
  COUNT(*) = COUNT(DISTINCT event_id)                               AS event_ids_unique,
  -- Execution attribution must be complete: a row belonging to no execution cannot be
  -- corrected or deleted by an execution-scoped operation. This is exactly the condition that
  -- let 89 stale priority rows survive a correction once.
  SUM(CASE WHEN execution_id IS NULL THEN 1 ELSE 0 END)             AS unassigned_rows
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status;

-- 6. Historical trip quality, by outcome. Same reconciliation discipline as station status.
SELECT
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips)    AS valid_trips,
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.quarantine_historical_trips) AS quarantined_trips,
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.duplicate_historical_trips)  AS duplicate_trips,
  (SELECT COUNT(DISTINCT ride_id) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips)
                                                                    AS distinct_ride_ids,
  (SELECT COUNT(*) = COUNT(DISTINCT ride_id)
   FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips)    AS ride_ids_unique;

-- 7. Lakeflow pipeline health from its own event log. Replace the pipeline ID when the
--    pipeline has actually been created; it is not deployed at the time of writing.
-- SELECT timestamp, level, event_type, message
-- FROM event_log(TABLE(dbr_dev.parvinbadalov_urbanflow.urbanflow_pipeline))
-- WHERE level IN ('WARN', 'ERROR')
-- ORDER BY timestamp DESC
-- LIMIT 50;

-- 8. Expectation results from the pipeline event log, once the pipeline exists. Each
--    expectation declared in pipeline/silver.py and pipeline/gold.py reports passed and failed
--    record counts here, which is how a declarative quality rule becomes auditable evidence.
-- SELECT
--   timestamp,
--   details:flow_progress.data_quality.expectations                 AS expectations
-- FROM event_log(TABLE(dbr_dev.parvinbadalov_urbanflow.urbanflow_pipeline))
-- WHERE event_type = 'flow_progress'
--   AND details:flow_progress.data_quality IS NOT NULL
-- ORDER BY timestamp DESC;

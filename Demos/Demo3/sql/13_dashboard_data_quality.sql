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
  :historical_execution_id                                          AS execution_id,
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
   WHERE execution_id = :historical_execution_id)                   AS valid_trips,
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.quarantine_historical_trips
   WHERE execution_id = :historical_execution_id)                   AS quarantined_trips,
  (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.duplicate_historical_trips
   WHERE execution_id = :historical_execution_id)                   AS duplicate_trips,
  (SELECT COUNT(DISTINCT ride_id) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
   WHERE execution_id = :historical_execution_id)                   AS distinct_ride_ids,
  (SELECT COUNT(*) = COUNT(DISTINCT ride_id) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
   WHERE execution_id = :historical_execution_id)                   AS ride_ids_unique;

-- 7. Historical quarantine reasons for ONE execution. Every quarantined trip carries at least
--    one named rule, so the reasons explain the whole quarantine rather than a sample of it.
SELECT
  execution_id,
  rule                                                              AS failed_rule,
  COUNT(*)                                                          AS trips_failing
FROM dbr_dev.parvinbadalov_urbanflow.quarantine_historical_trips
LATERAL VIEW EXPLODE(failed_rules) exploded AS rule
WHERE execution_id = :historical_execution_id
GROUP BY execution_id, rule
ORDER BY trips_failing DESC;

-- 8. Lakeflow expectation results from the isolated pipeline's own event log. Each expectation
--    declared in pipeline/silver.py and pipeline/gold.py reports passed and failed record counts,
--    which is how a declarative quality rule becomes auditable evidence. The pipeline writes only
--    to the isolated schema; this reads its event log and nothing else.
SELECT
  expectation.dataset                                               AS dataset,
  expectation.name                                                  AS expectation,
  SUM(expectation.passed_records)                                   AS passed_records,
  SUM(expectation.failed_records)                                   AS failed_records
FROM (
  SELECT EXPLODE(FROM_JSON(
           details:flow_progress.data_quality.expectations,
           'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>'
         )) AS expectation
  FROM EVENT_LOG(TABLE(dbr_dev.parvinbadalov_urbanflow_lakeflow.silver_station_status))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
    AND origin.update_id = (
      SELECT origin.update_id
      FROM EVENT_LOG(TABLE(dbr_dev.parvinbadalov_urbanflow_lakeflow.silver_station_status))
      WHERE event_type = 'update_progress' AND details:update_progress.state = 'COMPLETED'
      ORDER BY timestamp DESC LIMIT 1
    )
)
GROUP BY ALL
ORDER BY dataset, expectation;

-- 9. Monthly historical reconciliation and the trip-weighted reference match, for ONE execution.
--    Bronze carries no execution column, so each source namespace lands in its own Bronze table
--    with one owning execution (notebook 06 enforces that before streaming). This statement reads
--    the January 2024 namespace's table, so it must only be paired with that namespace's execution.
--    The match rate is weighted by trips, not by station-day rows: it is the share of rides that
--    start at one of the 40 stations in the DEVELOPMENT REFERENCE DIMENSION, i.e. coverage of
--    that dimension, not a data-quality measure.
SELECT
  :historical_execution_id                                          AS execution_id,
  landed_rows,
  valid_trips,
  quarantined_trips,
  duplicate_trips,
  landed_rows = valid_trips + quarantined_trips + duplicate_trips   AS reconciles,
  matched_trips,
  ROUND(100.0 * matched_trips / NULLIF(valid_trips, 0), 2)          AS reference_coverage_percent,
  'coverage of the 40-station development reference dimension, not data quality' AS reading_note
FROM (
  SELECT
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.bronze_historical_trips_202401_full)
                                                                    AS landed_rows,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
     WHERE execution_id = :historical_execution_id)                 AS valid_trips,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.quarantine_historical_trips
     WHERE execution_id = :historical_execution_id)                 AS quarantined_trips,
    (SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.duplicate_historical_trips
     WHERE execution_id = :historical_execution_id)                 AS duplicate_trips,
    (SELECT SUM(trips_started) FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand
     WHERE execution_id = :historical_execution_id AND start_station_uuid IS NOT NULL)
                                                                    AS matched_trips
);

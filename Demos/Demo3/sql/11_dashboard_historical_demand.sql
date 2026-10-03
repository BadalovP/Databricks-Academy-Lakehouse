-- UrbanFlow AI/BI dashboard - HISTORICAL DEMAND section.
-- Prepared only; read-only SELECTs against persisted tables.
--
-- This is the ONE section of the dashboard that may legitimately be charted over time. A
-- monthly Citi Bike archive contains weeks of real rides, so trips-per-day is a genuine trend.
-- The availability tables are a single snapshot and must never be presented this way.
-- Every query keeps execution_id in its grain. That prevents the validated 40-row sample from
-- being added to a later full-month execution, and it keeps the DEVELOPMENT SAMPLE label in the
-- returned dataset where a dashboard author cannot crop it out accidentally.
-- `:historical_execution_id` is a required dashboard parameter. A dataset cannot silently show
-- every execution when the author forgets to configure a filter.
--
-- JOIN KEY NOTE, which is the easiest thing to get wrong in this whole project: historical
-- trips carry values like '7407.13' in start_station_id. That is the GBFS SHORT NAME, not the
-- UUID station_id. Joining to station_id matches nothing and produces an empty chart that
-- looks like missing data rather than a join bug. Every join below uses station_short_name.

-- 1. Trips per day: the headline trend line.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  trip_date,
  SUM(trips_started)                                                AS trips,
  SUM(member_trips)                                                 AS member_trips,
  SUM(casual_trips)                                                  AS casual_trips,
  ROUND(AVG(avg_trip_minutes), 2)                                   AS avg_trip_minutes,
  COUNT(DISTINCT station_short_name)                                AS stations_with_trips
FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand
WHERE execution_id = :historical_execution_id
GROUP BY execution_id, trip_date
ORDER BY execution_id, trip_date;

-- 2. Busiest stations over the whole archive window.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  station_short_name,
  MAX(start_station_uuid)                                           AS current_station_uuid,
  SUM(trips_started)                                                AS trips,
  SUM(member_trips)                                                 AS member_trips,
  SUM(casual_trips)                                                  AS casual_trips,
  ROUND(AVG(avg_trip_minutes), 2)                                   AS avg_trip_minutes,
  -- A null UUID means this station is absent from the current GBFS feed, which is a real
  -- fact about a renamed or retired station rather than a data error.
  MAX(CASE WHEN start_station_uuid IS NULL THEN 'retired or renamed' ELSE 'current' END)
                                                                    AS station_status
FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand
WHERE execution_id = :historical_execution_id
GROUP BY execution_id, station_short_name
ORDER BY execution_id, trips DESC;

-- 3. Member versus casual mix, as a share rather than only as counts.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  trip_date,
  SUM(member_trips)                                                 AS member_trips,
  SUM(casual_trips)                                                  AS casual_trips,
  ROUND(
    100.0 * SUM(member_trips) / NULLIF(SUM(member_trips) + SUM(casual_trips), 0), 2
  )                                                                 AS member_percent
FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand
WHERE execution_id = :historical_execution_id
GROUP BY execution_id, trip_date
ORDER BY execution_id, trip_date;

-- 4. Demand by hour of day and day of week, from the trip table itself.
--    This needs trip-level granularity, which the daily aggregate has already collapsed.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  DATE_FORMAT(started_at, 'EEEE')                                   AS day_of_week,
  DAYOFWEEK(started_at)                                             AS day_of_week_number,
  HOUR(started_at)                                                  AS hour_of_day,
  COUNT(*)                                                          AS trips,
  SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END)         AS member_trips,
  SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END)         AS casual_trips
FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
WHERE execution_id = :historical_execution_id
GROUP BY ALL
ORDER BY execution_id, day_of_week_number, hour_of_day;

-- 5. Trip duration distribution, bucketed. Shows why the quality rules exist: the sub-minute
--    bucket is false starts and the over-a-day bucket is unreturned bikes, and both are
--    quarantined rather than silently averaged into the headline figures.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  CASE
    WHEN duration_minutes <  1   THEN '00 under 1 min (false start)'
    WHEN duration_minutes <  5   THEN '01 1-5 min'
    WHEN duration_minutes < 15   THEN '02 5-15 min'
    WHEN duration_minutes < 30   THEN '03 15-30 min'
    WHEN duration_minutes < 60   THEN '04 30-60 min'
    WHEN duration_minutes < 1440 THEN '05 1-24 hours'
    ELSE                              '06 over 24 hours (not returned)'
  END                                                               AS duration_bucket,
  COUNT(*)                                                          AS trips,
  ROUND(
    100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY execution_id), 2
  )                                                                 AS percent_of_trips
FROM (
  SELECT execution_id,
         (UNIX_TIMESTAMP(ended_at) - UNIX_TIMESTAMP(started_at)) / 60.0 AS duration_minutes
  FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
  WHERE execution_id = :historical_execution_id
)
GROUP BY execution_id, duration_bucket
ORDER BY execution_id, duration_bucket;

-- 6. Historical-to-current station match rate. A rate of exactly zero is the signature of
--    joining on the UUID instead of short_name, so it is surfaced on the dashboard rather
--    than left in a log.
SELECT
  execution_id,
  CASE WHEN execution_id LIKE 'urbanflow-hist-devsample40-%'
       THEN '40-ROW DEVELOPMENT SAMPLE' ELSE 'FULL ARCHIVE EXECUTION' END AS data_scope,
  COUNT(*)                                                          AS demand_rows,
  SUM(CASE WHEN start_station_uuid IS NOT NULL THEN 1 ELSE 0 END)   AS matched_to_current_station,
  SUM(CASE WHEN start_station_uuid IS NULL     THEN 1 ELSE 0 END)   AS unmatched,
  ROUND(
    100.0 * SUM(CASE WHEN start_station_uuid IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    2
  )                                                                 AS match_percent
FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand
WHERE execution_id = :historical_execution_id
GROUP BY execution_id
ORDER BY execution_id;

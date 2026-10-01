-- UrbanFlow AI/BI dashboard - CURRENT OPERATIONS section.
-- Prepared only. These are SELECT statements against already-persisted tables; none of them
-- writes, and none creates the dashboard object itself. See docs/DASHBOARD.md for the exact
-- creation steps and for which figures are real today.
--
-- HONESTY CONSTRAINT, and it governs this whole file: these tables hold ONE real GBFS
-- snapshot. Every number below is a point-in-time count, not a trend, and nothing here may be
-- charted over time. The HISTORICAL DEMAND section is where genuine trends live, because a
-- monthly trip archive really does contain weeks of rides.

-- 1. Operational headline counts. One row, one snapshot, deliberately labelled as such.
--    The labels are part of the result so the caption cannot drift from the data.
SELECT
  COUNT(*)                                                          AS total_stations_observed,
  SUM(CASE WHEN availability_status = 'AVAILABLE'            THEN 1 ELSE 0 END) AS available_stations,
  SUM(CASE WHEN availability_status = 'OUT_OF_SERVICE'        THEN 1 ELSE 0 END) AS out_of_service_stations,
  SUM(CASE WHEN availability_status = 'LOW_BIKES'             THEN 1 ELSE 0 END) AS low_bike_stations,
  SUM(CASE WHEN availability_status = 'LOW_DOCKS'             THEN 1 ELSE 0 END) AS low_dock_stations,
  SUM(CASE WHEN availability_status = 'LOW_BIKES_AND_DOCKS'   THEN 1 ELSE 0 END) AS both_low_stations,
  -- Actionable excludes out-of-service on purpose: a station that is not installed or not
  -- renting cannot be fixed by moving bikes, so counting it as actionable sends a van out
  -- for nothing. This is the defect the first live Gold run contained.
  SUM(CASE WHEN availability_status IN ('LOW_BIKES','LOW_DOCKS','LOW_BIKES_AND_DOCKS')
           THEN 1 ELSE 0 END)                                       AS actionable_shortages,
  MIN(observed_at)                                                  AS snapshot_observed_from,
  MAX(observed_at)                                                  AS snapshot_observed_to,
  COUNT(DISTINCT execution_id)                                      AS executions_included,
  'one GBFS snapshot; a point-in-time count, not a trend'           AS reading_note
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status;

-- 2. Availability breakdown for a pie or bar chart, with the share stated explicitly.
SELECT
  availability_status,
  COUNT(*)                                                          AS stations,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                AS percent_of_stations
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status
GROUP BY availability_status
ORDER BY stations DESC;

-- 3. Top rebalancing priorities: the list an operator would actually work from.
--    priority_score is a plain sum of its three components, which are shown beside it so the
--    ranking can be recomputed by hand. There is no model and no tuned weighting.
SELECT
  priority_score,
  action,
  station_name,
  station_id,
  availability_status,
  num_bikes_available,
  num_docks_available,
  capacity_estimate,
  severity_points,
  deficit_points,
  size_points,
  observed_at
FROM dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority
ORDER BY priority_score DESC, num_bikes_available ASC
LIMIT 25;

-- 4. Station availability detail, with coordinates for a map visual.
--    LEFT-joined reference data means a station missing from the 40-row development sample
--    still appears, with a null name, rather than vanishing from the map.
SELECT
  s.station_id,
  d.station_short_name,
  COALESCE(d.station_name, '(not in the 40-station development sample)') AS station_name,
  d.latitude,
  d.longitude,
  s.availability_status,
  s.is_operational,
  s.num_bikes_available,
  s.num_ebikes_available,
  s.num_docks_available,
  s.capacity_estimate,
  d.capacity                                                        AS reference_capacity,
  s.observed_at
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status AS s
LEFT JOIN dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample AS d
  ON s.station_id = d.station_id
ORDER BY s.availability_status, s.num_bikes_available;

-- 5. Action summary: how many vans, doing what. The counterpart to the priority list.
SELECT
  action,
  COUNT(*)                                                          AS stations,
  ROUND(AVG(priority_score), 2)                                     AS avg_priority_score,
  MAX(priority_score)                                               AS worst_priority_score
FROM dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority
GROUP BY action
ORDER BY stations DESC;

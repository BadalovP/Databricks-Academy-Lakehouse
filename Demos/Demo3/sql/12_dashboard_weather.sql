-- UrbanFlow AI/BI dashboard - WEATHER section.
-- Prepared only; read-only SELECTs against persisted tables.
--
-- TWO LIMITATIONS THAT MUST BE READ BEFORE ANY OF THESE CHARTS ARE SHOWN:
--
-- 1. The weather series is ONE COORDINATE for New York City, not per-station weather. Every
--    row carries weather_grid_label so the resolution is visible. Presenting this as
--    station-level weather would be the easiest mistake to make here.
-- 2. These are COMPARISONS, not predictions. Grouping trips by temperature bucket shows that
--    cold wet days have fewer rides; it does not model, forecast or control for anything else
--    (day of week, holidays, closures). The column names say "avg" and "total" rather than
--    "effect" for that reason.

-- 1. Trip volume against temperature, by day. The dashboard's main weather comparison.
SELECT
  trip_date,
  SUM(trips)                                                        AS trips,
  ROUND(AVG(avg_temperature_celsius), 2)                            AS avg_temperature_celsius,
  ROUND(SUM(total_precipitation_mm), 2)                             AS total_precipitation_mm,
  ROUND(AVG(avg_wind_speed_kmh), 2)                                 AS avg_wind_speed_kmh,
  -- Coverage is shown beside the figures because a chart built on half-covered hours looks
  -- exactly as convincing as one built on full coverage.
  ROUND(AVG(weather_coverage), 4)                                   AS weather_coverage
FROM dbr_dev.parvinbadalov_urbanflow.gold_weather_demand
GROUP BY trip_date
ORDER BY trip_date;

-- 2. Trips by temperature bucket. Coarse, labelled buckets rather than a fitted curve.
SELECT
  temperature_bucket,
  SUM(trips)                                                        AS trips,
  COUNT(DISTINCT trip_date)                                         AS days_in_bucket,
  ROUND(SUM(trips) / NULLIF(COUNT(DISTINCT trip_date), 0), 1)       AS avg_trips_per_day,
  ROUND(AVG(avg_temperature_celsius), 2)                            AS avg_temperature_celsius
FROM dbr_dev.parvinbadalov_urbanflow.gold_weather_demand
GROUP BY temperature_bucket
ORDER BY temperature_bucket;

-- 3. Precipitation against demand. Wet days are bucketed, not regressed.
SELECT
  CASE
    WHEN total_precipitation_mm IS NULL   THEN 'unknown'
    WHEN total_precipitation_mm =  0      THEN '0 dry'
    WHEN total_precipitation_mm <  2      THEN '1 light (under 2 mm)'
    WHEN total_precipitation_mm < 10      THEN '2 moderate (2-10 mm)'
    ELSE                                       '3 heavy (10 mm or more)'
  END                                                               AS precipitation_band,
  COUNT(DISTINCT trip_date)                                         AS days,
  SUM(trips)                                                        AS trips,
  ROUND(SUM(trips) / NULLIF(COUNT(DISTINCT trip_date), 0), 1)       AS avg_trips_per_day
FROM (
  SELECT trip_date, SUM(trips) AS trips, SUM(total_precipitation_mm) AS total_precipitation_mm
  FROM dbr_dev.parvinbadalov_urbanflow.gold_weather_demand
  GROUP BY trip_date
)
GROUP BY precipitation_band
ORDER BY precipitation_band;

-- 4. The weather series itself, so a reader can see its grain and its gaps directly.
SELECT
  weather_grid_label,
  MIN(weather_hour)                                                 AS covers_from,
  MAX(weather_hour)                                                 AS covers_to,
  COUNT(*)                                                          AS hours_retrieved,
  SUM(CASE WHEN has_temperature   THEN 1 ELSE 0 END)                AS hours_with_temperature,
  SUM(CASE WHEN has_precipitation THEN 1 ELSE 0 END)                AS hours_with_precipitation,
  SUM(CASE WHEN has_wind          THEN 1 ELSE 0 END)                AS hours_with_wind,
  -- A null reading stays null. It is never replaced with a zero, because zero degrees and
  -- "unknown" are different facts.
  SUM(CASE WHEN NOT has_temperature THEN 1 ELSE 0 END)              AS missing_temperature_hours,
  MAX(source)                                                       AS source,
  MAX(retrieved_at)                                                 AS retrieved_at
FROM dbr_dev.parvinbadalov_urbanflow.dim_weather_hourly
GROUP BY weather_grid_label;

-- 5. Trips that found no weather hour at all. Reported, not hidden: an uncovered hour is a
--    fact about the archive window, and the trips themselves are still real.
SELECT
  COUNT(*)                                                          AS trips,
  SUM(CASE WHEN has_weather THEN 1 ELSE 0 END)                      AS trips_with_weather,
  SUM(CASE WHEN NOT has_weather THEN 1 ELSE 0 END)                  AS trips_without_weather,
  ROUND(
    100.0 * SUM(CASE WHEN has_weather THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2
  )                                                                 AS weather_coverage_percent
FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips_weather;

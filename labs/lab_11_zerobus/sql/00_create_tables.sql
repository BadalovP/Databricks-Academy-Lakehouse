-- Lab 11: one isolated schema and two managed Delta tables.
-- Safe to re-run: nothing is dropped, replaced, truncated or overwritten.
-- Placeholders ${catalog}, ${schema}, ${bronze} and ${silver} are filled from config/lab11.toml.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Lab 11 - IoT telemetry ingested with Databricks Zerobus Ingest';

-- Bronze: written only by Zerobus Ingest, append-only, one row per delivery (duplicates kept).
-- The column list is the contract Zerobus validates every record against.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${bronze} (
  event_id STRING,
  device_id STRING,
  producer_id STRING,
  sequence_number BIGINT,
  event_timestamp TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  run_id STRING,
  batch_id STRING,
  produced_at TIMESTAMP
)
COMMENT 'Lab 11 Bronze - raw Zerobus deliveries (at-least-once, so an event may appear more than once)';

-- Silver: one row per event_id, maintained by sql/01_deduplication.sql.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${silver} (
  event_id STRING,
  device_id STRING,
  producer_id STRING,
  sequence_number BIGINT,
  event_timestamp TIMESTAMP,
  temperature DOUBLE,
  humidity DOUBLE,
  run_id STRING,
  first_batch_id STRING,
  first_produced_at TIMESTAMP,
  delivery_count BIGINT
)
COMMENT 'Lab 11 Silver - deduplicated IoT events, one row per event_id';

-- Lab 11: idempotent Bronze -> Silver materialization.
--
-- Policy per event_id: keep the earliest delivery (produced_at, then batch_id as tie-breaker)
-- and count all deliveries. A Silver row is rewritten only if a value actually changes, so
-- running this statement again on unchanged Bronze inserts 0 rows and updates 0 rows.

MERGE INTO ${catalog}.${schema}.${silver} AS t
USING (
  SELECT
    event_id,
    device_id,
    producer_id,
    sequence_number,
    event_timestamp,
    temperature,
    humidity,
    run_id,
    batch_id AS first_batch_id,
    produced_at AS first_produced_at,
    COUNT(*) OVER (PARTITION BY event_id) AS delivery_count
  FROM ${catalog}.${schema}.${bronze}
  WHERE event_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY produced_at, batch_id) = 1
) AS s
ON t.event_id = s.event_id
WHEN MATCHED AND NOT (
      t.device_id IS NOT DISTINCT FROM s.device_id
  AND t.producer_id IS NOT DISTINCT FROM s.producer_id
  AND t.sequence_number IS NOT DISTINCT FROM s.sequence_number
  AND t.event_timestamp IS NOT DISTINCT FROM s.event_timestamp
  AND t.temperature IS NOT DISTINCT FROM s.temperature
  AND t.humidity IS NOT DISTINCT FROM s.humidity
  AND t.run_id IS NOT DISTINCT FROM s.run_id
  AND t.first_batch_id IS NOT DISTINCT FROM s.first_batch_id
  AND t.first_produced_at IS NOT DISTINCT FROM s.first_produced_at
  AND t.delivery_count IS NOT DISTINCT FROM s.delivery_count
) THEN UPDATE SET
  device_id = s.device_id,
  producer_id = s.producer_id,
  sequence_number = s.sequence_number,
  event_timestamp = s.event_timestamp,
  temperature = s.temperature,
  humidity = s.humidity,
  run_id = s.run_id,
  first_batch_id = s.first_batch_id,
  first_produced_at = s.first_produced_at,
  delivery_count = s.delivery_count
WHEN NOT MATCHED THEN INSERT (
  event_id, device_id, producer_id, sequence_number, event_timestamp, temperature, humidity,
  run_id, first_batch_id, first_produced_at, delivery_count
) VALUES (
  s.event_id, s.device_id, s.producer_id, s.sequence_number, s.event_timestamp, s.temperature,
  s.humidity, s.run_id, s.first_batch_id, s.first_produced_at, s.delivery_count
);

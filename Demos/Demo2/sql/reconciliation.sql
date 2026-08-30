-- Physical V2 rows must reconcile to trusted facts plus quarantine.
WITH classified AS (
  SELECT _dq_status, order_line_id
  FROM dbr_dev.parvinbadalov.demo2_orders_classified
  WHERE _source_batch_id = 'DEMO2_V2_SCHEMA_EVOLUTION'
), counts AS (
  SELECT
    COUNT(*) AS bronze_physical_rows,
    COUNT_IF(_dq_status = 'VALID') AS valid_rows,
    COUNT_IF(_dq_status = 'WARN') AS warning_rows,
    COUNT_IF(_dq_status = 'QUARANTINE') AS quarantined_rows
  FROM classified
), facts AS (
  SELECT COUNT(*) AS trusted_fact_rows
  FROM dbr_dev.parvinbadalov.fact_order_lines
  WHERE _source_batch_id = 'DEMO2_V2_SCHEMA_EVOLUTION'
)
SELECT
  counts.*,
  facts.trusted_fact_rows,
  bronze_physical_rows = valid_rows + warning_rows + quarantined_rows AS physical_reconciles,
  trusted_fact_rows = valid_rows + warning_rows AS trusted_reconciles
FROM counts CROSS JOIN facts;

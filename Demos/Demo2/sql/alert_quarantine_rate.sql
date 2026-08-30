-- Latest logical ingestion batch, ordered by load timestamp rather than batch-id text.
SELECT
  _source_batch_id,
  _batch_loaded_at,
  total_rows,
  valid_rows,
  warning_rows,
  quarantined_rows,
  quarantine_rate_pct
FROM dbr_dev.parvinbadalov.demo2_dq_summary_gold
ORDER BY _batch_loaded_at DESC, _source_batch_id DESC
LIMIT 1;

-- UrbanFlow 2.0 AI/BI dashboard - MAINTENANCE & ASSISTANT section.
-- Prepared only; read-only SELECTs against persisted tables.
-- ${catalog}, ${schema} and ${lakeflow_schema} are the bundle target's values from databricks.yml;
-- scripts/build_dashboard.py substitutes them per target.
--
-- Two capabilities meet the operations data here. Station maintenance arrives through Delta
-- Change Data Feed (notebooks/11_federation_cdc.py); its tickets are FICTIONAL, SYNTHETIC DATA on
-- real sample station IDs. The assistant's evaluation scores (notebooks/23_evaluate_rag.py) exist
-- only after an approved live evaluation; until then these datasets are honestly empty.

-- 1. Maintenance tickets by status and severity: the current Silver state after CDC.
SELECT
  status,
  severity,
  COUNT(*)                                                          AS tickets,
  MAX(updated_at)                                                   AS last_updated_at,
  'FICTIONAL, SYNTHETIC MAINTENANCE DATA'                           AS data_scope
FROM ${catalog}.${schema}.silver_station_maintenance
GROUP BY status, severity
ORDER BY status, severity;

-- 2. Rebalancing priorities at stations with an open maintenance ticket. A van must not be sent
--    to a station that is closed for repair, so these rows need a dispatcher's decision first.
SELECT
  p.priority_score,
  p.action,
  p.station_name,
  p.availability_status,
  m.maintenance_id,
  m.status                                                          AS maintenance_status,
  m.severity                                                        AS maintenance_severity
FROM ${catalog}.${schema}.gold_rebalancing_priority AS p
JOIN ${catalog}.${schema}.silver_station_maintenance AS m
  ON p.station_id = m.station_id
WHERE m.status <> 'CLOSED'
ORDER BY p.priority_score DESC, p.station_name;

-- 3. CDC audit trail: every processed change event by source version and type.
SELECT
  commit_version,
  change_type,
  COUNT(*)                                                          AS events,
  MIN(commit_timestamp)                                             AS committed_at
FROM ${catalog}.${schema}.maintenance_cdc_history
GROUP BY commit_version, change_type
ORDER BY commit_version, change_type;

-- 4. CDC checkpoint: the last source version that passed reconciliation.
SELECT
  pipeline_key,
  processed_version,
  processed_at
FROM ${catalog}.${schema}.maintenance_cdc_checkpoint;

-- 5. Assistant evaluation: baseline versus RAG for the most recent live evaluation only.
--    Offline fixture scores are never written to this table, so every row here is a live run.
SELECT
  variant,
  COUNT(*)                                                          AS questions,
  ROUND(AVG(CAST(get_json_object(metrics_json, '$.answer_correctness') AS DOUBLE)), 3)
                                                                    AS answer_correctness,
  ROUND(AVG(CAST(get_json_object(metrics_json, '$.retrieval_recall') AS DOUBLE)), 3)
                                                                    AS retrieval_recall,
  ROUND(AVG(CAST(get_json_object(metrics_json, '$.source_coverage') AS DOUBLE)), 3)
                                                                    AS source_coverage,
  SUM(CASE WHEN error_status IS NOT NULL THEN 1 ELSE 0 END)         AS errors,
  MAX(evaluated_at)                                                 AS evaluated_at
FROM ${catalog}.${schema}.rag_evaluation_results
WHERE evaluation_id = (
  SELECT evaluation_id FROM ${catalog}.${schema}.rag_evaluation_results
  ORDER BY evaluated_at DESC LIMIT 1
)
GROUP BY variant
ORDER BY variant;

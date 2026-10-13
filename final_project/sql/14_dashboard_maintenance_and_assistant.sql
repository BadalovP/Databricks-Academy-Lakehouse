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

-- 2. Every station with an open maintenance ticket (OPEN or IN_PROGRESS): its current
--    availability and any pending rebalancing action. A van must not be sent to a station that
--    is closed for repair, so a pending action is a HOLD that needs a dispatcher's decision.
--    Stations without a pending action are listed too, so no open ticket is hidden. The latest
--    observation per station is used, so several executions cannot duplicate a ticket.
SELECT
  m.maintenance_id,
  COALESCE(d.station_name, '(not in the 40-station development sample)') AS station_name,
  m.status                                                          AS maintenance_status,
  m.severity                                                        AS maintenance_severity,
  m.updated_at                                                      AS maintenance_updated_at,
  s.availability_status,
  p.priority_score,
  p.action,
  CASE WHEN p.station_id IS NULL THEN 'no rebalancing action pending'
       ELSE 'HOLD - dispatcher decision before sending a van' END    AS dispatch_note
FROM ${catalog}.${schema}.silver_station_maintenance AS m
LEFT JOIN ${catalog}.${schema}.dim_station_development_sample AS d
  ON m.station_id = d.station_id
LEFT JOIN (
  SELECT station_id, availability_status FROM (
    SELECT station_id, availability_status,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY observed_at DESC) AS position
    FROM ${catalog}.${schema}.silver_station_status
  ) WHERE position = 1
) AS s
  ON m.station_id = s.station_id
LEFT JOIN (
  SELECT station_id, priority_score, action FROM (
    SELECT station_id, priority_score, action,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY observed_at DESC) AS position
    FROM ${catalog}.${schema}.gold_rebalancing_priority
  ) WHERE position = 1
) AS p
  ON m.station_id = p.station_id
WHERE m.status <> 'CLOSED'
ORDER BY p.priority_score DESC NULLS LAST,
         CASE m.severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
         m.maintenance_id;

-- 3. CDC audit trail: every processed change event by source version and type.
SELECT
  commit_version,
  change_type,
  COUNT(*)                                                          AS events,
  MIN(commit_timestamp)                                             AS committed_at
FROM ${catalog}.${schema}.maintenance_cdc_history
GROUP BY commit_version, change_type
ORDER BY commit_version, change_type;

-- 4. CDC checkpoint: the newest source version that was scanned, applied where needed and
--    reconciled. It also passes versions without data changes (an automatic OPTIMIZE), so it
--    can be ahead of the newest version with change events, shown beside it.
SELECT
  c.pipeline_key,
  c.processed_version,
  c.processed_at,
  (SELECT MAX(commit_version) FROM ${catalog}.${schema}.maintenance_cdc_history)
                                                                    AS last_data_change_version,
  (SELECT COUNT(*) FROM ${catalog}.${schema}.maintenance_cdc_history) AS history_events
FROM ${catalog}.${schema}.maintenance_cdc_checkpoint AS c;

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

-- Prepared only. Do not execute until the combined first-live-test request is approved.
-- These names are isolated from Labs, Demo1, Demo2, Demo2_Olist, and other students.

CREATE SCHEMA IF NOT EXISTS dbr_dev.parvinbadalov_urbanflow
COMMENT 'Isolated Unity Catalog schema for the UrbanFlow project';

CREATE VOLUME IF NOT EXISTS dbr_dev.parvinbadalov_urbanflow.urbanflow_landing
COMMENT 'Checkpoints and execution reports for the bounded UrbanFlow streaming test';

-- The notebook creates dbr_dev.parvinbadalov_urbanflow.bronze_station_status
-- on its first approved Delta streaming write. No destructive statement is used.

-- Optional manual bootstrap. The Asset Bundle normally creates the schema and Volume
-- (resources/urbanflow_final.infrastructure.yml); run this only where a bundle deploy is not used.
-- Substitute ${catalog} and ${schema} with the target's values from databricks.yml, for example
-- workspace / urbanflow_final_dev (DEV) or dbr_dev / parvinbadalov_urbanflow_final_prod (PROD).
-- Names are isolated from Demo 3, the Labs and other students. No destructive statement is used.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'UrbanFlow 2.0 Final Project data, quality, CDC, and RAG assets';

CREATE VOLUME IF NOT EXISTS ${catalog}.${schema}.urbanflow_final_landing
COMMENT 'UrbanFlow 2.0 landing files, schemas, checkpoints, and secret-safe reports';

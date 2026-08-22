-- Lab 06 External V2 governance reference / rollback
--
-- Replace <catalog> and <target_schema> before running manually.
--
-- Normal policy state:
--   fact_encounters -> row filter on organization_id
--   dim_patient     -> masks on ssn, first_name, last_name, address
--
-- IMPORTANT:
-- Remove policies from tables BEFORE dropping the referenced functions.

-- Inspect grants
SHOW GRANTS ON TABLE <catalog>.<target_schema>.fact_encounters;
SHOW GRANTS ON TABLE <catalog>.<target_schema>.dim_patient;

-- Optional rollback: remove RLS
ALTER TABLE <catalog>.<target_schema>.fact_encounters
DROP ROW FILTER;

-- Optional rollback: remove CLS
ALTER TABLE <catalog>.<target_schema>.dim_patient
ALTER COLUMN ssn DROP MASK;

ALTER TABLE <catalog>.<target_schema>.dim_patient
ALTER COLUMN first_name DROP MASK;

ALTER TABLE <catalog>.<target_schema>.dim_patient
ALTER COLUMN last_name DROP MASK;

ALTER TABLE <catalog>.<target_schema>.dim_patient
ALTER COLUMN address DROP MASK;

-- Drop policy functions only after detaching them.
DROP FUNCTION IF EXISTS <catalog>.<target_schema>.lab06_org_row_filter;
DROP FUNCTION IF EXISTS <catalog>.<target_schema>.lab06_mask_sensitive_string;

-- Mapping tables can be kept for audit/history.
-- Uncomment only when a complete governance teardown is intended.
--
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_user_organization_access;
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_patient_data_privileged_users;

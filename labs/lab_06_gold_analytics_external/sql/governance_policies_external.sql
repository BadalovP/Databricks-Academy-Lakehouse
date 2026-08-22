-- Lab 06 External V2 - Secure-View Governance Reference / Rollback
--
-- Governance is implemented with views, NOT native row filters / column masks
-- on the externally path-written Gold base tables.

-- Expected governed query surfaces:
--   <catalog>.<target_schema>.vw_fact_encounters_secure
--   <catalog>.<target_schema>.vw_dim_patient_secure
--
-- Mapping tables:
--   <catalog>.<target_schema>.lab06_user_organization_access
--   <catalog>.<target_schema>.lab06_patient_data_privileged_users

-- Inspect mappings:
SELECT *
FROM <catalog>.<target_schema>.lab06_user_organization_access
ORDER BY user_email, organization_id;

SELECT *
FROM <catalog>.<target_schema>.lab06_patient_data_privileged_users
ORDER BY user_email;

-- Validate secure views:
SELECT COUNT(*) AS secure_encounter_rows
FROM <catalog>.<target_schema>.vw_fact_encounters_secure;

SELECT patient_id, ssn, first_name, last_name, address
FROM <catalog>.<target_schema>.vw_dim_patient_secure
LIMIT 10;

-- Rollback only the secure governance layer:
DROP VIEW IF EXISTS <catalog>.<target_schema>.vw_fact_encounters_secure;
DROP VIEW IF EXISTS <catalog>.<target_schema>.vw_dim_patient_secure;

-- Optional: remove mapping tables as well.
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_user_organization_access;
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_patient_data_privileged_users;

-- Important:
-- Do not attach SET ROW FILTER / SET MASK policies to the External V2 base tables
-- while the Gold pipeline continues to use explicit ABFSS path-based writes.

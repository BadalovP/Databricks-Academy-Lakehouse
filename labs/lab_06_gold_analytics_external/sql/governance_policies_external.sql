-- Lab 06 External V2 - Secure-View Governance Reference / Rollback
--
-- Governance is implemented with secure Unity Catalog views, not native
-- SET ROW FILTER / SET MASK policies on the externally path-written Gold tables.

-- Governed query surfaces:
--   <catalog>.<target_schema>.vw_fact_encounters_secure
--   <catalog>.<target_schema>.vw_dim_patient_secure
--
-- Mapping tables:
--   <catalog>.<target_schema>.lab06_user_organization_access
--   <catalog>.<target_schema>.lab06_patient_data_privileged_users

-- Inspect organization mappings.
SELECT *
FROM <catalog>.<target_schema>.lab06_user_organization_access
ORDER BY user_email, organization_id;

-- Inspect patient-data privileged users.
SELECT *
FROM <catalog>.<target_schema>.lab06_patient_data_privileged_users
ORDER BY user_email;

-- Validate the secure encounter view.
SELECT COUNT(*) AS secure_encounter_rows
FROM <catalog>.<target_schema>.vw_fact_encounters_secure;

-- Validate the secure patient view.
SELECT
  patient_id,
  ssn,
  first_name,
  last_name,
  address
FROM <catalog>.<target_schema>.vw_dim_patient_secure
LIMIT 10;

-- Roll back only the secure governance layer if needed.
DROP VIEW IF EXISTS <catalog>.<target_schema>.vw_fact_encounters_secure;
DROP VIEW IF EXISTS <catalog>.<target_schema>.vw_dim_patient_secure;

-- Optional: remove mapping tables too.
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_user_organization_access;
-- DROP TABLE IF EXISTS <catalog>.<target_schema>.lab06_patient_data_privileged_users;

-- Important:
-- Do not attach native SET ROW FILTER / SET MASK policies to fact_encounters
-- or dim_patient while the External V2 Gold pipeline uses explicit ABFSS
-- path-based writes.

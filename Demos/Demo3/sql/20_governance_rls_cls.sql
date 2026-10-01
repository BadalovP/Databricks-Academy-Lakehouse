-- UrbanFlow governance: row filters, column masks and the ABAC explanation.
-- PREPARED ONLY. Nothing here has been executed.
--
-- READ THIS FIRST. Every statement below is DDL that changes who can see what. Applying a row
-- filter or column mask to a table silently changes query results for other principals, which
-- makes it exactly the kind of change that must not be run casually. Two consequences:
--
--   * These statements target UrbanFlow tables ONLY. Nothing here touches another student's
--     schema, the Labs schemas, Demo1, Demo2 or Demo2_Olist.
--   * Applying a mask to a column you then forget about looks identical to the data being
--     wrong. Section 5 is the inspection query that shows what is currently applied, and it
--     should be run before and after any change.
--
-- WHY A BIKE-SHARE PROJECT NEEDS THIS AT ALL: station status is public data, so the honest
-- teaching example is not pretending it is secret. The realistic case is operational: a
-- regional dispatcher should see their own region's stations, and the precise coordinates of a
-- station are commercially sensitive to some operators even when counts are not.

-- ---------------------------------------------------------------------------
-- 1. A row filter: a dispatcher sees only their own region.
-- ---------------------------------------------------------------------------
-- A row filter is a UDF returning BOOLEAN. It runs for every query against the table, so it
-- must stay cheap; is_account_group_member is evaluated by the engine, not by a lookup join.
CREATE OR REPLACE FUNCTION dbr_dev.parvinbadalov_urbanflow.region_row_filter(region_id STRING)
RETURNS BOOLEAN
COMMENT 'Dispatchers see their own region; urbanflow_admins see every region.'
RETURN
  is_account_group_member('urbanflow_admins')
  OR is_account_group_member(CONCAT('urbanflow_region_', region_id));

-- Applying it. NOTE the direction of failure: if the named groups do not exist, every
-- non-admin query returns ZERO rows rather than erroring, which reads as "the data is gone".
-- That is the single most important thing to understand before running this.
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   SET ROW FILTER dbr_dev.parvinbadalov_urbanflow.region_row_filter ON (region_id);

-- Removing it again, which is the rollback:
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   DROP ROW FILTER;

-- ---------------------------------------------------------------------------
-- 2. A column mask: coordinates are rounded for non-admins, not hidden.
-- ---------------------------------------------------------------------------
-- Returning a COARSER value rather than NULL is the better teaching example: the column stays
-- usable for a city-level map while the exact dock location is withheld. A mask must return
-- the same type as the column it masks.
CREATE OR REPLACE FUNCTION dbr_dev.parvinbadalov_urbanflow.coordinate_mask(value DOUBLE)
RETURNS DOUBLE
COMMENT 'Full precision for urbanflow_admins; two decimal places (about 1 km) otherwise.'
RETURN
  CASE
    WHEN is_account_group_member('urbanflow_admins') THEN value
    ELSE ROUND(value, 2)
  END;

-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN latitude  SET MASK dbr_dev.parvinbadalov_urbanflow.coordinate_mask;
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN longitude SET MASK dbr_dev.parvinbadalov_urbanflow.coordinate_mask;

-- Rollback:
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN latitude  DROP MASK;
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN longitude DROP MASK;

-- ---------------------------------------------------------------------------
-- 3. A rider-privacy mask on historical trips.
-- ---------------------------------------------------------------------------
-- Citi Bike's public archive is already de-identified: there is no rider ID, which is itself
-- the lesson. ride_id is still a per-trip identifier, and combined with start time, end time
-- and coordinates it is a re-identification vector for anyone with one known trip. So the
-- analyst-facing view keeps the aggregate columns and hashes the identifier.
CREATE OR REPLACE FUNCTION dbr_dev.parvinbadalov_urbanflow.ride_id_mask(value STRING)
RETURNS STRING
COMMENT 'Admins see ride_id; everyone else sees a stable one-way hash of it.'
RETURN
  CASE
    WHEN is_account_group_member('urbanflow_admins') THEN value
    -- SHA-256 is one-way and stable, so joins still work while the original is withheld.
    ELSE SHA2(value, 256)
  END;

-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
--   ALTER COLUMN ride_id SET MASK dbr_dev.parvinbadalov_urbanflow.ride_id_mask;

-- ---------------------------------------------------------------------------
-- 4. ABAC: what it is, and why this project uses groups instead.
-- ---------------------------------------------------------------------------
-- Attribute-based access control governs by TAGS rather than by object name: a policy says
-- "mask any column tagged pii" and then applies automatically to every such column in the
-- catalog, including ones created tomorrow. Compared with the per-column masks above, that is
-- the difference between a rule and a list.
--
-- Tagging is available and is genuinely useful documentation on its own:
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN latitude  SET TAGS ('sensitivity' = 'location');
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   ALTER COLUMN longitude SET TAGS ('sensitivity' = 'location');
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
--   ALTER COLUMN ride_id SET TAGS ('sensitivity' = 'identifier');
--
-- The honest limitation: tag-driven ABAC policies are a workspace- and account-level feature
-- whose availability depends on the metastore's configuration and the account's entitlements,
-- and this project has confirmed neither. The tags above are therefore presented as
-- documentation plus the mechanism a policy would key on, NOT as a working policy. Claiming a
-- functioning ABAC policy without having applied one would be the kind of unverified claim
-- this project avoids everywhere else.

-- ---------------------------------------------------------------------------
-- 5. Inspection: what is actually applied right now.
-- ---------------------------------------------------------------------------
-- Run this BEFORE and AFTER any change above. A mask you forgot about is indistinguishable
-- from the data being wrong.
SELECT table_name, column_name, mask_name
FROM dbr_dev.information_schema.column_masks
WHERE table_schema = 'parvinbadalov_urbanflow'
ORDER BY table_name, column_name;

SELECT table_name, filter_name, target_columns
FROM dbr_dev.information_schema.row_filters
WHERE table_schema = 'parvinbadalov_urbanflow'
ORDER BY table_name;

SELECT table_name, column_name, tag_name, tag_value
FROM dbr_dev.information_schema.column_tags
WHERE schema_name = 'parvinbadalov_urbanflow'
ORDER BY table_name, column_name;

-- ---------------------------------------------------------------------------
-- 6. Grants, least privilege first.
-- ---------------------------------------------------------------------------
-- An analyst needs SELECT on the Gold outputs and nothing on Bronze: raw ingestion carries
-- Kafka offsets and raw JSON that no analyst needs, and withholding it is both simpler and
-- safer than masking it column by column.
-- GRANT USE SCHEMA ON SCHEMA dbr_dev.parvinbadalov_urbanflow TO `urbanflow_analysts`;
-- GRANT SELECT ON TABLE dbr_dev.parvinbadalov_urbanflow.gold_station_shortage     TO `urbanflow_analysts`;
-- GRANT SELECT ON TABLE dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority TO `urbanflow_analysts`;
-- GRANT SELECT ON TABLE dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand    TO `urbanflow_analysts`;

-- Verify what was granted rather than assuming the statement implied it:
SELECT grantee, privilege_type, table_name
FROM dbr_dev.information_schema.table_privileges
WHERE table_schema = 'parvinbadalov_urbanflow'
ORDER BY grantee, table_name;

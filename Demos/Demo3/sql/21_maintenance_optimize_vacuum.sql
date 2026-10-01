-- UrbanFlow Delta maintenance: OPTIMIZE, liquid clustering, deletion vectors, VACUUM, CDF.
-- PREPARED ONLY. Nothing here has been executed.
--
-- ============================================================================
-- VACUUM WARNING, read before anything else in this file.
-- ============================================================================
-- VACUUM permanently deletes the data files that time travel and RESTORE depend on. Once run
-- with a short retention, "RESTORE TABLE ... VERSION AS OF" stops working for the versions
-- whose files are gone, and there is no undo. Specifically:
--
--   * The 7-day default retention exists to protect readers whose queries are still running
--     against an older snapshot. Lowering it can break a concurrent reader.
--   * VACUUM with RETAIN 0 HOURS requires disabling a safety check, and disabling that check
--     is exactly the kind of action this project does not take without explicit approval.
--
-- So the VACUUM section below is commented out, uses DRY RUN first, and keeps the default
-- retention. Nothing in this file is destructive as written.

-- ---------------------------------------------------------------------------
-- 1. Inspect before changing anything.
-- ---------------------------------------------------------------------------
-- File count and sizes are the reason to run OPTIMIZE; running it without looking first is
-- cargo-culting. A table written by one bounded run of 2,520 rows does NOT need compaction, and
-- saying so is more useful than demonstrating the command for its own sake.
DESCRIBE DETAIL dbr_dev.parvinbadalov_urbanflow.silver_station_status;
DESCRIBE DETAIL dbr_dev.parvinbadalov_urbanflow.fact_station_availability;
DESCRIBE DETAIL dbr_dev.parvinbadalov_urbanflow.silver_historical_trips;

-- History shows which operations produced which versions, and is the audit trail a supervisor
-- can read. It is also how the Phase 2 correction was proven: the priority table's history
-- showed it was created by one execution and written by nothing else since.
DESCRIBE HISTORY dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority;

-- ---------------------------------------------------------------------------
-- 2. OPTIMIZE and Z-ORDER, the classic approach.
-- ---------------------------------------------------------------------------
-- OPTIMIZE compacts many small files into fewer large ones. The small-file problem is real for
-- a streaming table written every few minutes; it is NOT a problem for a single bounded write.
-- OPTIMIZE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips;

-- Z-ORDER co-locates rows by the columns you filter on, so the engine skips more files. It must
-- be chosen from actual query patterns: the dashboard filters trips by date and station, so
-- those are the candidates. Z-ordering on a high-cardinality column nobody filters on costs
-- rewrite time and buys nothing.
-- OPTIMIZE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
--   ZORDER BY (started_at, start_station_id);

-- ---------------------------------------------------------------------------
-- 3. Liquid clustering, and an honest comparison with Z-ORDER.
-- ---------------------------------------------------------------------------
-- Liquid clustering replaces both partitioning and Z-ORDER. The practical differences:
--
--   Z-ORDER                              Liquid clustering
--   ------------------------------------ ----------------------------------------------
--   Re-sorts on every OPTIMIZE run       Incremental; only new data is clustered
--   Clustering columns fixed by rerun    Columns changed with ALTER TABLE, no rewrite
--   Combined with partitioning, badly    Replaces partitioning entirely
--   Whole-file rewrite each time         Cheaper steady-state maintenance
--
-- The trade-off worth stating: liquid clustering cannot be combined with partitioning, and
-- converting an existing partitioned table means a rewrite. For a new table it is the better
-- default; for an existing partitioned one the migration has a real cost.
--
-- On a new table:
-- CREATE TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips_clustered
-- CLUSTER BY (started_at, start_station_id)
-- AS SELECT * FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips;
--
-- Changing the clustering columns later, with no rewrite:
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips_clustered
--   CLUSTER BY (trip_date);
--
-- Automatic clustering, where Databricks chooses the columns from observed query history:
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips_clustered
--   CLUSTER BY AUTO;

-- ---------------------------------------------------------------------------
-- 4. Deletion vectors.
-- ---------------------------------------------------------------------------
-- With deletion vectors on, a DELETE or UPDATE records which ROWS are logically removed instead
-- of rewriting whole Parquet files. That makes the Phase 2 stale-row correction much cheaper:
-- deleting 89 rows from a 746-row table touches a vector rather than rewriting the files.
--
-- The cost is on the read side: readers must apply the vectors, and files accumulate logically
-- deleted rows until an OPTIMIZE purges them. Worth knowing rather than assuming it is free.
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority
--   SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
--
-- Purging them physically, which is what actually reclaims the space:
-- REORG TABLE dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority APPLY (PURGE);

-- ---------------------------------------------------------------------------
-- 5. Change Data Feed.
-- ---------------------------------------------------------------------------
-- CDF makes a table's row-level changes readable, which is what a downstream consumer needs to
-- stay in sync without recomputing everything. It is the natural companion to the SCD2
-- dimension: the change feed says what changed, and SCD2 records what it changed from.
--
-- Enabling it is NOT retroactive: changes are only captured from the version where the property
-- is set, so turning it on after the fact does not recover history.
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample
--   SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
--
-- Reading the changes, once enabled and once a later version exists:
-- SELECT _change_type, _commit_version, _commit_timestamp, station_id, station_name, capacity
-- FROM table_changes('dbr_dev.parvinbadalov_urbanflow.dim_station_development_sample', 1)
-- ORDER BY _commit_version, station_id;

-- ---------------------------------------------------------------------------
-- 6. Column mapping, which is what makes a rename safe.
-- ---------------------------------------------------------------------------
-- Without column mapping, renaming a column rewrites the table, because Parquet files store
-- physical names. With it, Delta keeps a logical-to-physical mapping and a rename is metadata
-- only. Enabling it requires a reader/writer version upgrade, and that upgrade is ONE-WAY: the
-- table can no longer be read by older clients. That is the cost to weigh.
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips SET TBLPROPERTIES (
--   'delta.minReaderVersion' = '2',
--   'delta.minWriterVersion' = '5',
--   'delta.columnMapping.mode' = 'name'
-- );
-- ALTER TABLE dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
--   RENAME COLUMN member_casual TO rider_type;

-- ---------------------------------------------------------------------------
-- 7. VACUUM. Dry run only, default retention, nothing uncommented.
-- ---------------------------------------------------------------------------
-- DRY RUN lists the files that WOULD be deleted and deletes nothing. This is the only form that
-- should ever be run without a specific approval.
-- VACUUM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips DRY RUN;

-- The real thing, keeping the 7-day default that protects time travel and concurrent readers:
-- VACUUM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips RETAIN 168 HOURS;

-- NOT RUN, and deliberately left as a warning rather than an example: a short retention
-- requires disabling a safety check and permanently destroys the ability to time travel to the
-- affected versions.
--   SET spark.databricks.delta.retentionDurationCheck.enabled = false;  -- do not do this
--   VACUUM ... RETAIN 0 HOURS;                                         -- irreversible

-- ---------------------------------------------------------------------------
-- 8. Time travel, which is what VACUUM takes away.
-- ---------------------------------------------------------------------------
-- Worth running BEFORE any VACUUM, to see what would be lost.
-- SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority VERSION AS OF 0;
-- SELECT COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority VERSION AS OF 1;
--
-- And the recovery path that depends on those files still existing:
-- RESTORE TABLE dbr_dev.parvinbadalov_urbanflow.gold_rebalancing_priority TO VERSION AS OF 1;

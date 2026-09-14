-- LAB 08 - TravelOps
--
-- Query:
-- Production Health
--
-- Purpose:
-- Provides a single operational health result for CI/CD validation and optional
-- Databricks SQL alerting.
--
-- Source tables:
-- - target-specific gold_production_health
-- - target-specific gold_payment_reconciliation
--
-- Result:
-- Exactly one current health row with counts that can be read by humans and
-- automation.
--
-- Expected use:
-- Run after the Lakeflow pipeline to prove the deployed bundle produced usable
-- Gold data.
--
-- Environment behavior:
-- Replace ${target_catalog} and ${target_schema} in CI or Databricks SQL before execution.

SELECT
  current_booking_count,
  duplicate_current_booking_count,
  payment_mismatch_count,
  health_passed,
  evaluated_at
FROM ${target_catalog}.${target_schema}.gold_production_health;

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
-- automation. no_payment_record_count and pending_or_failed_payment_count are
-- informational only (a booking with no payment yet, or only a pending/failed
-- payment, is a normal lifecycle state); invalid_booking_amount_count and
-- payment_amount_mismatch_count are the counts that gate health_passed.
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
  invalid_booking_amount_count,
  no_payment_record_count,
  pending_or_failed_payment_count,
  matched_payment_count,
  payment_amount_mismatch_count,
  health_passed,
  evaluated_at
FROM ${target_catalog}.${target_schema}.gold_production_health;

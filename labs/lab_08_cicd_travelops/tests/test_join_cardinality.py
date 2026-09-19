"""
LAB 08 - TravelOps

Test module:
tests/test_join_cardinality.py

Purpose:
Demonstrates, with plain Python data structures, why an undeduplicated
dimension table inflates a summed metric when joined to bookings -- the
mechanism behind the ~7x booking_value inflation verified live in
gold_property_performance/gold_destination_performance before
properties_silver/destinations_silver deduplicated on their primary key --
and why a one-to-many table (reviews per booking) must be pre-aggregated
before joining to avoid inflating the same metric even with zero duplicate
ingestion.

Inputs:
Small in-memory Python records.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are side-effect free.

Local unit tests vs Databricks integration:
These tests simulate a left join and revenue sum with plain Python loops,
not pipeline/gold.py's actual PySpark joins. They demonstrate the join-
cardinality mechanism and its fix in isolation; they do not execute
pipeline/gold.py itself (no pyspark dependency in this environment), so they
cannot catch a regression introduced directly in that file's join logic. The
read-only verification against live Azure PROD data (see
evidence/lab08_photon_fix_and_reconciliation_observation.md and
evidence/lab08_production_remediation_plan.md) is what actually confirmed
the ~7x inflation and measured its fix; only a Databricks bundle run can
reconfirm that end to end.
"""

from travelops.ingestion import deduplicate_by_primary_key


def _left_join_sum(bookings, dimension_rows, join_key, amount_field):
    """Simulate SUM(amount_field) over bookings LEFT JOIN dimension_rows USING (join_key)."""

    total = 0
    for booking in bookings:
        matches = [d for d in dimension_rows if d[join_key] == booking[join_key]]
        match_count = max(len(matches), 1)  # a left join keeps the row even with 0 matches
        total += booking[amount_field] * match_count
    return total


def test_undeduplicated_dimension_join_inflates_summed_amount() -> None:
    bookings = [{"booking_id": 1, "property_id": 100, "booking_amount": 50}]
    duplicated_properties = [{"property_id": 100}, {"property_id": 100}, {"property_id": 100}]

    inflated_total = _left_join_sum(
        bookings, duplicated_properties, "property_id", "booking_amount"
    )

    assert inflated_total == 150  # 50 x 3 duplicate property rows -- the bug


def test_deduplicating_dimension_by_primary_key_fixes_join_cardinality() -> None:
    bookings = [{"booking_id": 1, "property_id": 100, "booking_amount": 50}]
    duplicated_properties = [{"property_id": 100}, {"property_id": 100}, {"property_id": 100}]

    deduped_properties = deduplicate_by_primary_key(duplicated_properties, "property_id")
    correct_total = _left_join_sum(bookings, deduped_properties, "property_id", "booking_amount")

    assert correct_total == 50


def test_revenue_reconciles_to_distinct_current_booking_total() -> None:
    # Mirrors "test join cardinality and revenue reconciliation against
    # distinct current bookings": the ground truth for total booking value
    # is the sum over current_bookings_silver alone (one row per
    # booking_id), regardless of which dimension tables are joined in.
    bookings = [
        {"booking_id": 1, "property_id": 100, "booking_amount": 50},
        {"booking_id": 2, "property_id": 100, "booking_amount": 30},
    ]
    ground_truth_total = sum(b["booking_amount"] for b in bookings)

    deduped_properties = deduplicate_by_primary_key(
        [{"property_id": 100}, {"property_id": 100}], "property_id"
    )
    joined_total = _left_join_sum(bookings, deduped_properties, "property_id", "booking_amount")

    assert joined_total == ground_truth_total


def test_one_to_many_table_must_be_pre_aggregated_before_joining() -> None:
    # A booking can legitimately have more than one review. Joining reviews
    # directly (not pre-aggregated) fans out that booking's row once per
    # review, inflating a summed booking_amount even with zero duplicate
    # ingestion -- this is the bug fixed in gold_destination_performance by
    # aggregating reviews to one row per booking_id before joining.
    bookings = [{"booking_id": 1, "booking_amount": 50}]
    reviews = [
        {"booking_id": 1, "rating": 4.0},
        {"booking_id": 1, "rating": 5.0},
    ]  # two genuinely distinct reviews for the same booking

    naive_total = _left_join_sum(bookings, reviews, "booking_id", "booking_amount")
    assert naive_total == 100  # the bug: fanned out by review count

    pre_aggregated_reviews = [{"booking_id": 1}]  # one row per booking_id, as gold.py now does
    fixed_total = _left_join_sum(bookings, pre_aggregated_reviews, "booking_id", "booking_amount")
    assert fixed_total == 50

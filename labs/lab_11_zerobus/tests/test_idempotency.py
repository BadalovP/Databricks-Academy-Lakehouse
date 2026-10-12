from conftest import START_MICROS

from lab11.idempotency import canonical_rows, duplicate_stats, merge
from lab11.producer import build_plan, fixed_clock


def test_unique_events_produce_one_silver_row_each(plan):
    primary = [r for r in plan.records if "primary" in r["batch_id"]]
    stats = duplicate_stats(primary)
    assert (stats.raw_rows, stats.unique_events, stats.duplicate_deliveries) == (24, 24, 0)
    assert merge({}, primary).inserted == 24


def test_duplicate_deliveries_are_counted_but_not_multiplied(plan):
    stats = duplicate_stats(plan.records)
    assert stats.raw_rows == 30
    assert stats.unique_events == 24
    assert stats.duplicate_deliveries == 6
    assert stats.events_with_duplicates == 6
    assert stats.conflicting_events == 0
    result = merge({}, plan.records)
    assert (result.inserted, result.updated, len(result.silver)) == (24, 0, 24)
    assert sum(row["delivery_count"] for row in result.silver.values()) == 30


def test_the_earliest_delivery_wins_and_the_rule_is_deterministic(plan):
    silver = canonical_rows(plan.records)
    for event_id in plan.replayed_event_ids:
        assert "primary" in silver[event_id]["first_batch_id"]
        assert silver[event_id]["delivery_count"] == 2
    assert canonical_rows(list(reversed(plan.records))) == silver  # arrival order does not matter


def test_equal_produced_at_falls_back_to_batch_id(plan):
    record = plan.records[0]
    a = {**record, "batch_id": "b-2", "produced_at": 5}
    b = {**record, "batch_id": "b-1", "produced_at": 5}
    assert canonical_rows([a, b])[record["event_id"]]["first_batch_id"] == "b-1"


def test_rerunning_the_merge_changes_nothing(plan):
    first = merge({}, plan.records)
    second = merge(first.silver, plan.records)
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 24)
    assert second.silver == first.silver


def test_a_retried_event_arriving_later_only_updates_its_delivery_count(plan):
    first = merge({}, plan.records)
    retried = {
        **plan.records[3],
        "batch_id": "late-retry",
        "produced_at": plan.records[3]["produced_at"] + 10**9,
    }
    second = merge(first.silver, [*plan.records, retried])
    assert (second.inserted, second.updated) == (0, 1)
    assert (
        second.silver[retried["event_id"]]["delivery_count"]
        == first.silver[retried["event_id"]]["delivery_count"] + 1
    )


def test_overlapping_batches_across_runs(config):
    run1 = build_plan(config.producer, "run-1", clock=fixed_clock(START_MICROS))
    run2 = build_plan(config.producer, "run-2", clock=fixed_clock(START_MICROS + 10**9))
    after_one = merge({}, run1.records)
    overlap = run1.batches[1].records + run2.batches[0].records  # a re-sent batch plus new data
    after_two = merge(after_one.silver, [*run1.records, *overlap])
    assert after_two.inserted == 10  # only run-2's new events
    assert len(after_two.silver) == 34


def test_conflicting_payloads_for_one_event_are_reported(plan):
    record = plan.records[0]
    conflicting = {**record, "batch_id": "bad", "temperature": record["temperature"] + 1}
    assert duplicate_stats([record, conflicting]).conflicting_events == 1

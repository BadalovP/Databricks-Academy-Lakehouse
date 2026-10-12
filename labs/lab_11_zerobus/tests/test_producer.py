from conftest import START_MICROS

from lab11.events import BUSINESS_FIELDS
from lab11.producer import PRIMARY, REPLAY, build_plan, fixed_clock


def test_plan_sends_every_event_once_then_replays_a_subset(plan, config):
    summary = plan.summary()
    assert summary["unique_events"] == 24
    assert summary["deliveries"] == 30
    assert summary["replayed_events"] == 6
    primary_ids = [eid for b in plan.batches if b.delivery == PRIMARY for eid in b.event_ids]
    assert sorted(primary_ids) == sorted(e.event_id for e in plan.events)
    assert set(plan.replayed_event_ids) <= set(primary_ids)
    assert len(set(plan.replayed_event_ids)) == config.producer.replay_count


def test_batches_respect_the_batch_size_and_have_unique_ids(plan, config):
    assert all(len(b.records) <= config.producer.batch_size for b in plan.batches)
    assert len({b.batch_id for b in plan.batches}) == len(plan.batches)
    assert [b.delivery for b in plan.batches] == [PRIMARY, PRIMARY, PRIMARY, REPLAY]


def test_replayed_records_carry_the_same_identity_and_payload(plan):
    first_delivery = {}
    for record in plan.records:
        if record["event_id"] in first_delivery:
            original = first_delivery[record["event_id"]]
            assert all(record[f] == original[f] for f in BUSINESS_FIELDS)
            assert record["batch_id"] != original["batch_id"]
            assert record["produced_at"] > original["produced_at"]
        else:
            first_delivery[record["event_id"]] = record


def test_the_same_run_id_rebuilds_the_same_plan(config):
    one = build_plan(config.producer, "repeat", clock=fixed_clock(START_MICROS))
    two = build_plan(config.producer, "repeat", clock=fixed_clock(START_MICROS))
    assert one == two

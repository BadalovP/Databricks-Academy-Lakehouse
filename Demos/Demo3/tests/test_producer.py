from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

from urbanflow.gbfs_client import GBFSFeed
from urbanflow.producer import build_events, event_id, run_bounded


def feed(last_reported: int = 100) -> GBFSFeed:
    return GBFSFeed(
        name="station_status",
        url="sample",
        version="2.3",
        ttl_seconds=60,
        source_last_updated=100,
        collected_at=datetime.now(UTC).isoformat(),
        stations=[
            {
                "station_id": "station-1",
                "last_reported": last_reported,
                "num_bikes_available": 2,
                "num_docks_available": 8,
                "collected_at": "2026-09-28T00:00:00Z",
            }
        ],
    )


def test_event_id_changes_when_source_observation_changes() -> None:
    first = feed(100).stations[0]
    second = feed(101).stations[0]
    assert event_id(first) != event_id(second)


def test_build_events_skips_duplicate_observation() -> None:
    seen: set[str] = set()
    assert len(build_events(feed(), seen)) == 1
    assert build_events(feed(), seen) == []


def test_bounded_run_respects_ttl_and_does_not_publish_duplicates() -> None:
    client = Mock()
    client.fetch_station_status.side_effect = [feed(), feed()]
    publisher = Mock()
    publisher.send.side_effect = lambda rows: len(list(rows))
    sleep = Mock()
    assert run_bounded(client, publisher, poll_count=2, sleep_fn=sleep) == 1
    sleep.assert_called_once_with(60)

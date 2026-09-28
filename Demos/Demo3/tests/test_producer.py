from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow.gbfs_client import GBFSFeed
from urbanflow.producer import (
    EventHubsPublisher,
    build_events,
    event_id,
    publish_snapshot,
    run_bounded,
)


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


def test_snapshot_report_records_deterministic_ids_and_counts() -> None:
    client = Mock()
    client.fetch_station_status.return_value = feed()
    publisher = Mock()
    publisher.send.side_effect = lambda rows: len(list(rows))

    report = publish_snapshot(
        client,
        publisher,
        execution_id="run-1",
        max_publish_events=10,
    )

    assert report.execution_id == "run-1"
    assert report.published_events == 1
    assert report.source_last_updated == 100
    assert report.event_ids == (event_id(feed().stations[0]),)
    sent_event = publisher.send.call_args.args[0][0]
    assert sent_event["execution_id"] == "run-1"


def test_snapshot_refuses_to_exceed_publish_limit() -> None:
    client = Mock()
    oversized = feed()
    oversized.stations.append({**oversized.stations[0], "station_id": "station-2"})
    client.fetch_station_status.return_value = oversized
    publisher = Mock()

    with pytest.raises(RuntimeError, match="limit is 1"):
        publish_snapshot(client, publisher, execution_id="run-1", max_publish_events=1)

    publisher.send.assert_not_called()


def test_event_hubs_adapter_uses_mocked_sdk_without_logging_secret(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class FakeBatch(list):
        def add(self, message: object) -> None:
            self.append(message)

    fake_client = Mock()
    fake_client.create_batch.side_effect = FakeBatch
    factory = Mock(return_value=fake_client)
    eventhub_module = ModuleType("azure.eventhub")
    eventhub_module.EventData = lambda value: value
    eventhub_module.EventHubProducerClient = SimpleNamespace(from_connection_string=factory)
    azure_module = ModuleType("azure")
    azure_module.eventhub = eventhub_module
    monkeypatch.setitem(__import__("sys").modules, "azure", azure_module)
    monkeypatch.setitem(__import__("sys").modules, "azure.eventhub", eventhub_module)

    secret = "Endpoint=sb://example/;SharedAccessKey=do-not-log"
    publisher = EventHubsPublisher(secret, "parvinbadalov_evh")
    assert publisher.send([{"event_id": "one"}]) == 1

    factory.assert_called_once_with(conn_str=secret, eventhub_name="parvinbadalov_evh")
    fake_client.send_batch.assert_called_once()
    fake_client.close.assert_called_once()
    assert secret not in caplog.text


def test_bounded_run_respects_ttl_and_does_not_publish_duplicates() -> None:
    client = Mock()
    client.fetch_station_status.side_effect = [feed(), feed()]
    publisher = Mock()
    publisher.send.side_effect = lambda rows: len(list(rows))
    sleep = Mock()
    assert run_bounded(client, publisher, poll_count=2, sleep_fn=sleep) == 1
    sleep.assert_called_once_with(60)

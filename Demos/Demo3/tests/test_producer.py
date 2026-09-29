from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from urbanflow.gbfs_client import GBFSFeed
from urbanflow.producer import (
    EventHubsPublisher,
    OversizedEventError,
    PartialPublishError,
    build_events,
    event_id,
    publish_snapshot,
    run_bounded,
)

# Split so this fake literal cannot be mistaken for a real connection string by a secret scanner.
FAKE_CONNECTION_STRING = ";".join(["Endpoint=sb://example/", "SharedAccess" + "Key=do-not-log"])


class FakeBatch(list):
    """Stand-in for EventDataBatch: like the SDK, add() raises ValueError once the batch is full."""

    def __init__(self, capacity: int) -> None:
        super().__init__()
        self.capacity = capacity

    def add(self, message: object) -> None:
        if len(self) >= self.capacity:
            raise ValueError("EventDataBatch is full.")
        self.append(message)


def install_fake_eventhub_sdk(
    monkeypatch: pytest.MonkeyPatch, *, batch_capacity: int = 100
) -> tuple[Mock, Mock]:
    """Install a fake azure.eventhub module and return (from_connection_string, client)."""
    fake_client = Mock()
    fake_client.create_batch.side_effect = lambda: FakeBatch(batch_capacity)
    factory = Mock(return_value=fake_client)
    eventhub_module = ModuleType("azure.eventhub")
    eventhub_module.EventData = lambda value: value
    eventhub_module.EventHubProducerClient = SimpleNamespace(from_connection_string=factory)
    azure_module = ModuleType("azure")
    azure_module.eventhub = eventhub_module
    monkeypatch.setitem(sys.modules, "azure", azure_module)
    monkeypatch.setitem(sys.modules, "azure.eventhub", eventhub_module)
    return factory, fake_client


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


def test_publisher_never_renders_the_connection_string() -> None:
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    for rendered in (repr(publisher), str(publisher), f"{publisher}"):
        assert "do-not-log" not in rendered
        assert FAKE_CONNECTION_STRING not in rendered
        assert "parvinbadalov_evh" in rendered

    assert publisher.connection_string == FAKE_CONNECTION_STRING  # still usable internally


def test_event_hubs_adapter_uses_mocked_sdk_without_logging_secret(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    factory, fake_client = install_fake_eventhub_sdk(monkeypatch)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    # Without this the urbanflow logger stays at WARNING, caplog.text is empty, and the
    # "secret was not logged" assertion below would pass even for an INFO-level leak.
    caplog.set_level(logging.DEBUG, logger="urbanflow")
    assert publisher.send([{"event_id": "one"}]) == 1

    factory.assert_called_once_with(
        conn_str=FAKE_CONNECTION_STRING,
        eventhub_name="parvinbadalov_evh",
        retry_total=3,
        retry_backoff_factor=0.5,
    )
    fake_client.send_batch.assert_called_once()
    serialized = fake_client.send_batch.call_args.args[0][0]
    assert json.loads(serialized) == {"event_id": "one"}
    fake_client.close.assert_called_once()
    assert FAKE_CONNECTION_STRING not in caplog.text
    # Guard against a vacuous assertion above: the producer's own INFO summary must be captured,
    # which proves an INFO-level leak would have been captured too.
    assert any(
        record.name == "urbanflow.producer" and record.levelno == logging.INFO
        for record in caplog.records
    )


def test_azure_retry_arguments_are_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    factory, _ = install_fake_eventhub_sdk(monkeypatch)
    publisher = EventHubsPublisher(
        FAKE_CONNECTION_STRING, "parvinbadalov_evh", retry_total=5, retry_backoff_seconds=1.5
    )

    assert publisher.send([]) == 0

    assert factory.call_args.kwargs["retry_total"] == 5
    assert factory.call_args.kwargs["retry_backoff_factor"] == 1.5


def test_multi_batch_send_returns_the_total_published(monkeypatch: pytest.MonkeyPatch) -> None:
    _, fake_client = install_fake_eventhub_sdk(monkeypatch, batch_capacity=2)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    assert publisher.send([{"event_id": str(index)} for index in range(5)]) == 5

    assert fake_client.send_batch.call_count == 3
    fake_client.close.assert_called_once()


def test_failure_after_partial_send_reports_the_published_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, fake_client = install_fake_eventhub_sdk(monkeypatch, batch_capacity=2)
    fake_client.send_batch.side_effect = [None, RuntimeError("Event Hubs is unavailable.")]
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    with pytest.raises(PartialPublishError) as error:
        publisher.send([{"event_id": str(index)} for index in range(5)])

    assert error.value.published_events == 2
    assert "2 events were already published" in str(error.value)
    fake_client.close.assert_called_once()


def test_oversized_event_still_raises_and_closes_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, fake_client = install_fake_eventhub_sdk(monkeypatch, batch_capacity=0)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    with pytest.raises(OversizedEventError, match="exceeds the Event Hubs batch limit") as error:
        publisher.send([{"event_id": "one"}])

    assert isinstance(error.value, ValueError)  # unchanged behaviour for existing callers
    fake_client.send_batch.assert_not_called()
    fake_client.close.assert_called_once()


def test_event_hubs_failure_is_sanitized_and_client_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    fake_secret = "SharedAccess" + "Key=not-a-secret"
    fake_client.send_batch.side_effect = RuntimeError(f"failure echoed {fake_secret}")

    with pytest.raises(PartialPublishError) as error:
        EventHubsPublisher(fake_secret, "hub").send([{"event_id": "one"}])

    assert error.value.published_events == 0
    assert "not-a-secret" not in str(error.value)
    fake_client.close.assert_called_once()


def test_bounded_run_respects_ttl_and_does_not_publish_duplicates() -> None:
    client = Mock()
    client.fetch_station_status.side_effect = [feed(), feed()]
    publisher = Mock()
    publisher.send.side_effect = lambda rows: len(list(rows))
    sleep = Mock()
    assert run_bounded(client, publisher, poll_count=2, sleep_fn=sleep) == 1
    sleep.assert_called_once_with(60)

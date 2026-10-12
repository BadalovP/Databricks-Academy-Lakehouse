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
    DEFAULT_SOCKET_TIMEOUT_SECONDS,
    BatchTooLargeError,
    EventHubsPublisher,
    OversizedEventError,
    PartialPublishError,
    PublishOutcome,
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


def confirmed_outcome(rows: object) -> PublishOutcome:
    """Stand in for a fully successful publish of every supplied row."""
    from urbanflow.producer import BatchOutcome

    ids = tuple(str(row["event_id"]) for row in list(rows))
    return PublishOutcome((BatchOutcome(0, ids, "confirmed"),))


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
    publisher.send.side_effect = confirmed_outcome

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
    assert publisher.send([{"event_id": "one"}]).confirmed_events == 1

    factory.assert_called_once_with(
        conn_str=FAKE_CONNECTION_STRING,
        eventhub_name="parvinbadalov_evh",
        retry_total=3,
        retry_backoff_factor=0.5,
        socket_timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS,
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

    assert publisher.send([]).confirmed_events == 0

    assert factory.call_args.kwargs["retry_total"] == 5
    assert factory.call_args.kwargs["retry_backoff_factor"] == 1.5


def test_multi_batch_send_returns_the_total_published(monkeypatch: pytest.MonkeyPatch) -> None:
    """Batches are cut by max_events_per_batch, so one write never carries everything."""
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    publisher = EventHubsPublisher(
        FAKE_CONNECTION_STRING, "parvinbadalov_evh", max_events_per_batch=2
    )

    outcome = publisher.send([{"event_id": str(index)} for index in range(5)])

    assert outcome.confirmed_events == 5
    assert outcome.complete is True
    assert [b.size for b in outcome.batches] == [2, 2, 1]
    assert fake_client.send_batch.call_count == 3
    fake_client.close.assert_called_once()


def test_failure_after_partial_send_reports_the_published_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    fake_client.send_batch.side_effect = [None, RuntimeError("Event Hubs is unavailable.")]
    publisher = EventHubsPublisher(
        FAKE_CONNECTION_STRING, "parvinbadalov_evh", max_events_per_batch=2
    )

    with pytest.raises(PartialPublishError) as error:
        publisher.send([{"event_id": str(index)} for index in range(5)])

    outcome = error.value.outcome
    assert error.value.published_events == 2
    assert (outcome.confirmed_events, outcome.uncertain_events) == (2, 2)
    assert outcome.not_attempted_events == 1
    assert outcome.complete is False
    # The batch that raised is UNCERTAIN, not failed: it may have reached the broker.
    assert [b.status for b in outcome.batches] == [
        "confirmed",
        "uncertain",
        "not_attempted",
    ]
    assert outcome.confirmed_event_ids == ("0", "1")
    assert outcome.uncertain_event_ids == ("2", "3")
    assert outcome.not_attempted_event_ids == ("4",)
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
    assert error.value.outcome.uncertain_events == 1
    assert "not-a-secret" not in str(error.value)
    fake_client.close.assert_called_once()


def test_bounded_run_respects_ttl_and_does_not_publish_duplicates() -> None:
    client = Mock()
    client.fetch_station_status.side_effect = [feed(), feed()]
    publisher = Mock()
    publisher.send.side_effect = confirmed_outcome
    sleep = Mock()
    assert run_bounded(client, publisher, poll_count=2, sleep_fn=sleep) == 1
    sleep.assert_called_once_with(60)


def install_fake_key_vault_sdk(
    monkeypatch: pytest.MonkeyPatch, *, secret_value: str | None
) -> tuple[Mock, Mock]:
    """Install fake azure.identity and azure.keyvault.secrets modules.

    Returns (SecretClient constructor mock, get_secret mock) so a test can assert
    which vault URL was built and which secret name was requested.
    """
    get_secret = Mock(return_value=SimpleNamespace(value=secret_value))
    secret_client_ctor = Mock(return_value=SimpleNamespace(get_secret=get_secret))

    identity_module = ModuleType("azure.identity")
    identity_module.AzureCliCredential = Mock(return_value="cli-credential")
    secrets_module = ModuleType("azure.keyvault.secrets")
    secrets_module.SecretClient = secret_client_ctor
    keyvault_module = ModuleType("azure.keyvault")
    keyvault_module.secrets = secrets_module
    azure_module = sys.modules.get("azure") or ModuleType("azure")
    azure_module.identity = identity_module
    azure_module.keyvault = keyvault_module

    monkeypatch.setitem(sys.modules, "azure", azure_module)
    monkeypatch.setitem(sys.modules, "azure.identity", identity_module)
    monkeypatch.setitem(sys.modules, "azure.keyvault", keyvault_module)
    monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", secrets_module)
    return secret_client_ctor, get_secret


def test_from_key_vault_reads_the_configured_vault_and_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The preferred credential path: no environment variable is involved at all."""
    monkeypatch.delenv("AZURE_EVENTHUB_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_EVENTHUB_NAME", raising=False)
    ctor, get_secret = install_fake_key_vault_sdk(monkeypatch, secret_value=FAKE_CONNECTION_STRING)

    publisher = EventHubsPublisher.from_key_vault(
        vault_name="kvpl24databricks2",
        secret_name="parvinbadalov-eventhub-cs",
        event_hub_name="parvinbadalov_evh",
    )

    assert ctor.call_args.kwargs["vault_url"] == "https://kvpl24databricks2.vault.azure.net/"
    get_secret.assert_called_once_with("parvinbadalov-eventhub-cs")
    assert publisher.connection_string == FAKE_CONNECTION_STRING
    assert publisher.event_hub_name == "parvinbadalov_evh"


def test_from_key_vault_never_renders_or_logs_the_secret(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Provenance may be logged; the value must not be, at any level."""
    caplog.set_level(logging.DEBUG, logger="urbanflow")
    install_fake_key_vault_sdk(monkeypatch, secret_value=FAKE_CONNECTION_STRING)

    publisher = EventHubsPublisher.from_key_vault(
        vault_name="kvpl24databricks2",
        secret_name="parvinbadalov-eventhub-cs",
        event_hub_name="parvinbadalov_evh",
    )

    assert FAKE_CONNECTION_STRING not in caplog.text
    assert FAKE_CONNECTION_STRING not in repr(publisher)
    assert FAKE_CONNECTION_STRING not in str(publisher)
    assert FAKE_CONNECTION_STRING not in f"{publisher}"
    # Guard against the capture itself silently going vacuous again.
    assert any(
        record.name == "urbanflow.producer" and "Key Vault" in record.getMessage()
        for record in caplog.records
    )


def test_from_key_vault_rejects_an_empty_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty secret must fail loudly rather than producing an unusable publisher."""
    install_fake_key_vault_sdk(monkeypatch, secret_value="")

    with pytest.raises(RuntimeError, match="present but empty"):
        EventHubsPublisher.from_key_vault(
            vault_name="kvpl24databricks2",
            secret_name="parvinbadalov-eventhub-cs",
            event_hub_name="parvinbadalov_evh",
        )


# --- transport, timeout and batch-size configuration -------------------------


def test_websocket_transport_is_passed_to_the_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """AMQP over WebSocket uses port 443, which survives proxies that break raw AMQP."""
    factory, _ = install_fake_eventhub_sdk(monkeypatch)
    # The websocket path is the only one that touches TransportType, so the fake
    # module only needs it here.
    sys.modules["azure.eventhub"].TransportType = SimpleNamespace(
        AmqpOverWebsocket="AmqpOverWebsocketSentinel"
    )
    publisher = EventHubsPublisher(
        FAKE_CONNECTION_STRING, "parvinbadalov_evh", transport="websocket"
    )

    publisher.send([{"event_id": "one"}])

    assert factory.call_args.kwargs["transport_type"] == "AmqpOverWebsocketSentinel"


def test_default_transport_stays_amqp_and_sets_no_transport_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, _ = install_fake_eventhub_sdk(monkeypatch)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "parvinbadalov_evh")

    publisher.send([{"event_id": "one"}])

    assert publisher.transport == "amqp"
    assert "transport_type" not in factory.call_args.kwargs


def test_socket_timeout_is_configurable_and_can_be_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, _ = install_fake_eventhub_sdk(monkeypatch)

    EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", socket_timeout_seconds=45.5).send(
        [{"event_id": "one"}]
    )
    assert factory.call_args.kwargs["socket_timeout"] == 45.5

    # None means "leave the SDK default alone" rather than "use zero".
    EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", socket_timeout_seconds=None).send(
        [{"event_id": "one"}]
    )
    assert "socket_timeout" not in factory.call_args.kwargs


def test_invalid_publisher_options_are_rejected() -> None:
    with pytest.raises(ValueError, match="transport must be one of"):
        EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", transport="carrier-pigeon")
    with pytest.raises(ValueError, match="max_events_per_batch must be positive"):
        EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=0)
    with pytest.raises(ValueError, match="socket_timeout_seconds must be positive"):
        EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", socket_timeout_seconds=0)


def test_default_batch_size_is_in_the_intended_range() -> None:
    """The live retry targets roughly 200-400 events per network write."""
    assert 200 <= EventHubsPublisher(FAKE_CONNECTION_STRING, "hub").max_events_per_batch <= 400


def test_a_batch_that_exceeds_the_size_limit_asks_for_a_smaller_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A size overflow must name the fix rather than silently resplitting."""
    _, fake_client = install_fake_eventhub_sdk(monkeypatch, batch_capacity=2)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=5)

    with pytest.raises(BatchTooLargeError, match="lower max_events_per_batch"):
        publisher.send([{"event_id": str(i)} for i in range(5)])

    fake_client.close.assert_called_once()


# --- the whole snapshot must survive batching --------------------------------


def test_every_station_in_the_snapshot_is_published_across_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batching must never truncate the snapshot: all 2,520 stations are published.

    This is the regression guard for the tempting-but-wrong fix of shrinking the
    snapshot instead of shrinking the batch.
    """
    # Fake batch capacity is raised above max_events_per_batch so this test exercises
    # count-based batching rather than the fake's stand-in for the 1 MiB size limit.
    _, fake_client = install_fake_eventhub_sdk(monkeypatch, batch_capacity=1000)
    station_count = 2520
    events = [{"event_id": f"evt-{i:05d}", "station_id": str(i)} for i in range(station_count)]
    publisher = EventHubsPublisher(
        FAKE_CONNECTION_STRING, "parvinbadalov_evh", max_events_per_batch=300
    )

    outcome = publisher.send(events)

    assert outcome.confirmed_events == station_count
    assert outcome.complete is True
    # 2520 / 300 = 8 full batches plus a remainder of 120.
    assert [b.size for b in outcome.batches] == [300] * 8 + [120]
    assert sum(b.size for b in outcome.batches) == station_count
    # Every original ID is present exactly once, in order.
    assert outcome.confirmed_event_ids == tuple(e["event_id"] for e in events)
    assert len(set(outcome.confirmed_event_ids)) == station_count
    assert fake_client.send_batch.call_count == 9


# --- retry semantics: identical IDs so duplicates stay detectable -------------


def test_retrying_the_unconfirmed_batches_reuses_the_original_event_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry must resend the SAME event IDs, so Bronze can spot duplicates.

    Event IDs are a pure function of the observation, so an event resent after an
    uncertain delivery keeps its identity instead of looking like a new station
    reading. That is what makes downstream deduplication possible.
    """
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    events = [{"event_id": f"evt-{i}"} for i in range(5)]
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=2)

    fake_client.send_batch.side_effect = [None, RuntimeError("uplink stalled")]
    with pytest.raises(PartialPublishError) as first:
        publisher.send(events)
    outcome = first.value.outcome

    # Resend only what was not confirmed, preserving order and identity.
    unconfirmed = outcome.uncertain_event_ids + outcome.not_attempted_event_ids
    assert unconfirmed == ("evt-2", "evt-3", "evt-4")
    resend = [e for e in events if e["event_id"] in set(unconfirmed)]
    assert [e["event_id"] for e in resend] == list(unconfirmed)

    fake_client.send_batch.side_effect = None
    second = publisher.send(resend)

    assert second.complete is True
    assert second.confirmed_event_ids == ("evt-2", "evt-3", "evt-4")
    # Union of both attempts covers the snapshot exactly once, with no ID invented.
    assert set(outcome.confirmed_event_ids) | set(second.confirmed_event_ids) == {
        e["event_id"] for e in events
    }


def test_duplicate_event_ids_within_one_send_are_reported_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publisher does not silently dedupe; Bronze/Silver is where dedup belongs."""
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=2)

    outcome = publisher.send([{"event_id": "same"}, {"event_id": "same"}])

    assert outcome.confirmed_events == 2
    assert outcome.confirmed_event_ids == ("same", "same")


def test_outcome_as_dict_is_json_safe_and_secret_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """The delivery evidence is written to the producer report, so it must be clean."""
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    fake_client.send_batch.side_effect = [None, RuntimeError("uplink stalled")]
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=2)

    with pytest.raises(PartialPublishError) as error:
        publisher.send([{"event_id": f"evt-{i}"} for i in range(4)])

    payload = json.dumps(error.value.outcome.as_dict())
    assert FAKE_CONNECTION_STRING not in payload
    assert "do-not-log" not in payload
    restored = json.loads(payload)
    assert restored["confirmed_events"] == 2
    assert restored["uncertain_events"] == 2
    assert restored["complete"] is False
    assert [b["status"] for b in restored["batches"]] == ["confirmed", "uncertain"]


def test_a_local_error_is_not_attempted_rather_than_uncertain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observed live: a missing optional WebSocket dependency raised ImportError from
    inside send_batch, and the batch was wrongly reported as uncertain delivery.

    A local error happens before any byte reaches the broker, so nothing can have been
    delivered. Reporting it as uncertain would send the operator hunting for orphan
    events that cannot exist.
    """
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    fake_client.send_batch.side_effect = ImportError("No module named 'websocket'")
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=2)

    with pytest.raises(PartialPublishError) as error:
        publisher.send([{"event_id": str(i)} for i in range(5)])

    outcome = error.value.outcome
    assert outcome.uncertain_events == 0
    assert outcome.not_attempted_events == 5
    assert all(b.status == "not_attempted" for b in outcome.batches)


def test_a_transport_error_is_still_uncertain(monkeypatch: pytest.MonkeyPatch) -> None:
    """The conservative classification must survive for genuine network failures."""
    _, fake_client = install_fake_eventhub_sdk(monkeypatch)
    fake_client.send_batch.side_effect = ConnectionError("write operation timed out")
    publisher = EventHubsPublisher(FAKE_CONNECTION_STRING, "hub", max_events_per_batch=2)

    with pytest.raises(PartialPublishError) as error:
        publisher.send([{"event_id": str(i)} for i in range(5)])

    outcome = error.value.outcome
    assert outcome.uncertain_events == 2
    assert outcome.not_attempted_events == 3

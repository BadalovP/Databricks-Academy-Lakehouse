"""Bounded Citi Bike GBFS producer for Azure Event Hubs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Iterable, Protocol
from uuid import uuid4

from urbanflow.gbfs_client import GBFSClient, GBFSFeed

logger = logging.getLogger(__name__)

DEFAULT_EVENTHUB_RETRY_TOTAL = 3
DEFAULT_EVENTHUB_RETRY_BACKOFF_SECONDS = 0.5

# One batch is one send_batch call, so this also bounds how much data a single
# network write has to move. 300 events of roughly 680 bytes is about 204 KiB,
# leaving a wide margin under the 1 MiB Event Hubs batch ceiling.
DEFAULT_EVENTS_PER_BATCH = 300

# The Azure SDK's own default socket timeout is short. A slow or lossy uplink can
# exceed it on a large write, so it is configurable rather than fixed.
DEFAULT_SOCKET_TIMEOUT_SECONDS = 120.0

TRANSPORT_AMQP = "amqp"
TRANSPORT_WEBSOCKET = "websocket"
SUPPORTED_TRANSPORTS = (TRANSPORT_AMQP, TRANSPORT_WEBSOCKET)

# Delivery status of one batch. "uncertain" is the important one: a send that
# raised may still have reached the broker, because the failure can happen after
# the frame left this machine but before the acknowledgement came back.
BATCH_CONFIRMED = "confirmed"
BATCH_UNCERTAIN = "uncertain"
BATCH_NOT_ATTEMPTED = "not_attempted"

# Failures that provably happen on this machine before any byte reaches the broker,
# so the batch cannot have been delivered. Calling these "uncertain" would send the
# operator hunting for orphan events that cannot exist. Observed live: a missing
# optional WebSocket dependency surfaces as ImportError from inside send_batch,
# because the SDK imports its transport lazily when it opens the connection.
LOCAL_FAILURE_TYPES = (ImportError, TypeError, AttributeError, NameError)


class Publisher(Protocol):
    def send(self, events: Iterable[dict[str, Any]]) -> "PublishOutcome": ...


@dataclass(frozen=True)
class BatchOutcome:
    """What happened to one batch, and exactly which event IDs it carried."""

    index: int
    event_ids: tuple[str, ...]
    status: str

    @property
    def size(self) -> int:
        return len(self.event_ids)

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "status": self.status,
            "size": self.size,
            "event_ids": list(self.event_ids),
        }


@dataclass(frozen=True)
class PublishOutcome:
    """Per-batch delivery evidence for one publish attempt.

    Separating confirmed from uncertain from never-attempted is what lets a retry
    resend only the uncertain and unsent batches. Because event IDs are a pure
    function of the observation, a resent event keeps its original ID and the
    Bronze layer can recognise it as a duplicate rather than a new observation.
    """

    batches: tuple[BatchOutcome, ...]

    def _ids(self, status: str) -> tuple[str, ...]:
        return tuple(i for b in self.batches if b.status == status for i in b.event_ids)

    @property
    def confirmed_event_ids(self) -> tuple[str, ...]:
        return self._ids(BATCH_CONFIRMED)

    @property
    def uncertain_event_ids(self) -> tuple[str, ...]:
        return self._ids(BATCH_UNCERTAIN)

    @property
    def not_attempted_event_ids(self) -> tuple[str, ...]:
        return self._ids(BATCH_NOT_ATTEMPTED)

    @property
    def confirmed_events(self) -> int:
        return len(self.confirmed_event_ids)

    @property
    def uncertain_events(self) -> int:
        return len(self.uncertain_event_ids)

    @property
    def not_attempted_events(self) -> int:
        return len(self.not_attempted_event_ids)

    @property
    def complete(self) -> bool:
        return all(b.status == BATCH_CONFIRMED for b in self.batches)

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_count": len(self.batches),
            "confirmed_events": self.confirmed_events,
            "uncertain_events": self.uncertain_events,
            "not_attempted_events": self.not_attempted_events,
            "complete": self.complete,
            "batches": [b.as_dict() for b in self.batches],
        }


def event_id(row: dict[str, Any]) -> str:
    """Stable ID for one station/source timestamp observation."""
    raw = "|".join(
        str(row.get(field, ""))
        for field in (
            "station_id",
            "last_reported",
            "num_bikes_available",
            "num_docks_available",
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def new_execution_id() -> str:
    """Return a sortable, non-secret identifier shared by producer and consumer evidence."""
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"urbanflow-{timestamp}-{uuid4().hex[:8]}"


def build_events(
    feed: GBFSFeed,
    seen_event_ids: set[str] | None = None,
    *,
    execution_id: str = "local-test",
) -> list[dict[str, Any]]:
    """Build JSON-safe events and omit duplicates already seen by this bounded process."""
    if not execution_id.strip():
        raise ValueError("execution_id must be non-empty.")
    seen = seen_event_ids if seen_event_ids is not None else set()
    events: list[dict[str, Any]] = []
    for station in feed.stations:
        identifier = event_id(station)
        if identifier in seen:
            continue
        event = dict(station)
        event["event_id"] = identifier
        event["execution_id"] = execution_id
        event["gbfs_version"] = feed.version
        events.append(event)
        seen.add(identifier)
    return events


class OversizedEventError(ValueError):
    """One prepared event is larger than a single Event Hubs batch can hold."""


class BatchTooLargeError(ValueError):
    """A whole batch exceeded the Event Hubs size limit, so max_events_per_batch is too high."""


class PartialPublishError(RuntimeError):
    """An Event Hubs send failed part-way; `outcome` says exactly which batches got through.

    Carries no SDK exception message, because an upstream message could echo
    connection details. Only the exception type name is surfaced.
    """

    def __init__(self, outcome: PublishOutcome, cause: BaseException) -> None:
        message = (
            f"Event Hubs send failed ({type(cause).__name__}): "
            f"{outcome.confirmed_events} events confirmed, "
            f"{outcome.uncertain_events} of uncertain delivery, "
            f"{outcome.not_attempted_events} never attempted. "
            "Resend only the uncertain and not-attempted batches, keeping their original "
            "event IDs so duplicates stay detectable."
        )
        super().__init__(message)
        self.outcome = outcome
        # Retained for callers that only care about the confirmed count.
        self.published_events = outcome.confirmed_events


@dataclass
class EventHubsPublisher:
    """Azure SDK adapter; credentials stay in memory and are never logged or rendered."""

    # repr=False keeps the SAS key out of repr(), str(), f-strings and pytest tracebacks.
    connection_string: str = field(repr=False)
    event_hub_name: str
    retry_total: int = DEFAULT_EVENTHUB_RETRY_TOTAL
    retry_backoff_seconds: float = DEFAULT_EVENTHUB_RETRY_BACKOFF_SECONDS
    max_events_per_batch: int = DEFAULT_EVENTS_PER_BATCH
    # "websocket" tunnels AMQP over port 443, which traverses firewalls and proxies
    # that interfere with raw AMQP on 5671.
    transport: str = TRANSPORT_AMQP
    socket_timeout_seconds: float | None = DEFAULT_SOCKET_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if self.transport not in SUPPORTED_TRANSPORTS:
            raise ValueError(f"transport must be one of {SUPPORTED_TRANSPORTS}.")
        if self.max_events_per_batch < 1:
            raise ValueError("max_events_per_batch must be positive.")
        if self.socket_timeout_seconds is not None and self.socket_timeout_seconds <= 0:
            raise ValueError("socket_timeout_seconds must be positive when set.")

    def _client_kwargs(self) -> dict[str, Any]:
        """Build the SDK keyword arguments, including transport and socket timeout."""
        kwargs: dict[str, Any] = {
            "conn_str": self.connection_string,
            "eventhub_name": self.event_hub_name,
            "retry_total": self.retry_total,
            "retry_backoff_factor": self.retry_backoff_seconds,
        }
        if self.transport == TRANSPORT_WEBSOCKET:
            # Imported only on the path that needs it, so the default AMQP path does
            # not depend on this symbol existing.
            from azure.eventhub import TransportType

            kwargs["transport_type"] = TransportType.AmqpOverWebsocket
        if self.socket_timeout_seconds is not None:
            kwargs["socket_timeout"] = self.socket_timeout_seconds
        return kwargs

    @classmethod
    def from_environment(cls, **options: Any) -> "EventHubsPublisher":
        """Read the credential from the process environment.

        Kept for non-interactive callers such as CI, but `from_key_vault` is the
        preferred path for an operator running this by hand: an environment
        variable has to be typed or pasted somewhere first, which tends to leave
        the live SAS key in a shell history file and in a process environment
        other programs on the machine can read.
        """
        connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
        event_hub_name = os.getenv("AZURE_EVENTHUB_NAME")
        if not connection_string or not event_hub_name:
            raise RuntimeError(
                "AZURE_EVENTHUB_CONNECTION_STRING and AZURE_EVENTHUB_NAME are required."
            )
        return cls(connection_string=connection_string, event_hub_name=event_hub_name, **options)

    @classmethod
    def from_key_vault(
        cls,
        *,
        vault_name: str,
        secret_name: str,
        event_hub_name: str,
        credential: Any | None = None,
        **options: Any,
    ) -> "EventHubsPublisher":
        """Fetch the connection string from Azure Key Vault for this run only.

        This is the preferred credential path. The secret is read straight into
        memory using the operator's existing Azure CLI sign-in, so it is never
        typed into a terminal, never stored in a shell variable or history file,
        and never written to this repository. It is the same Key Vault secret that
        already backs the `azure-secrets` Databricks scope, so the notebook and the
        producer authenticate against one source of truth rather than two copies.
        """
        from azure.identity import AzureCliCredential
        from azure.keyvault.secrets import SecretClient

        client = SecretClient(
            vault_url=f"https://{vault_name}.vault.azure.net/",
            credential=credential if credential is not None else AzureCliCredential(),
        )
        secret = client.get_secret(secret_name)
        if not secret.value:
            raise RuntimeError(
                f"Key Vault secret {secret_name!r} in vault {vault_name!r} is present but empty."
            )
        # Deliberately logs only provenance, never the value or any part of it.
        logger.info(
            "Loaded the Event Hubs credential from Key Vault %s, secret %s.",
            vault_name,
            secret_name,
        )
        return cls(connection_string=secret.value, event_hub_name=event_hub_name, **options)

    def send(self, events: Iterable[dict[str, Any]]) -> PublishOutcome:
        """Publish events in small batches, recording each batch's delivery status.

        One chunk is one batch is one send_batch call, so a single network write
        never has to move more than max_events_per_batch events. If a send raises,
        that batch's delivery is UNCERTAIN (it may have reached the broker before
        the failure) and later batches were never attempted; both are reported so a
        retry can resend exactly those, unchanged event IDs included.
        """
        from azure.eventhub import EventData, EventHubProducerClient

        pairs = [
            (
                str(event.get("event_id", "")),
                json.dumps(event, separators=(",", ":"), sort_keys=True),
            )
            for event in events
        ]
        chunks = [
            pairs[start : start + self.max_events_per_batch]
            for start in range(0, len(pairs), self.max_events_per_batch)
        ]

        client = EventHubProducerClient.from_connection_string(**self._client_kwargs())
        outcomes: list[BatchOutcome] = []
        failure: BaseException | None = None
        failed_index: int | None = None
        try:
            for index, chunk in enumerate(chunks):
                batch = client.create_batch()
                for _, text in chunk:
                    try:
                        batch.add(EventData(text))
                    except ValueError as error:
                        if len(batch) == 0:
                            raise OversizedEventError(
                                "One UrbanFlow event exceeds the Event Hubs batch limit."
                            ) from error
                        raise BatchTooLargeError(
                            f"A batch of {len(chunk)} events exceeded the Event Hubs size "
                            f"limit; lower max_events_per_batch (currently "
                            f"{self.max_events_per_batch})."
                        ) from error
                client.send_batch(batch)
                outcomes.append(BatchOutcome(index, tuple(i for i, _ in chunk), BATCH_CONFIRMED))
        except Exception as error:
            failure = error
            failed_index = len(outcomes)
        finally:
            client.close()

        if failure is not None:
            if isinstance(failure, (OversizedEventError, BatchTooLargeError)):
                # Our own messages about our own data; safe to surface unchanged.
                raise failure
            assert failed_index is not None
            # A local error cannot have delivered anything, so the in-flight batch is
            # not_attempted rather than uncertain.
            in_flight_status = (
                BATCH_NOT_ATTEMPTED if isinstance(failure, LOCAL_FAILURE_TYPES) else BATCH_UNCERTAIN
            )
            for index in range(failed_index, len(chunks)):
                status = in_flight_status if index == failed_index else BATCH_NOT_ATTEMPTED
                outcomes.append(BatchOutcome(index, tuple(i for i, _ in chunks[index]), status))
            outcome = PublishOutcome(tuple(outcomes))
            logger.warning(
                "Event Hubs publish incomplete: %s confirmed, %s uncertain, %s not attempted "
                "across %s batches.",
                outcome.confirmed_events,
                outcome.uncertain_events,
                outcome.not_attempted_events,
                len(outcome.batches),
            )
            # Suppress the SDK exception chain: an upstream message could echo
            # connection details. Only the type name and the outcome survive.
            raise PartialPublishError(outcome, failure) from None

        outcome = PublishOutcome(tuple(outcomes))
        logger.info(
            "Published %s UrbanFlow station observations in %s batches of at most %s.",
            outcome.confirmed_events,
            len(outcome.batches),
            self.max_events_per_batch,
        )
        return outcome


@dataclass(frozen=True)
class PublishReport:
    """Secret-free evidence for one bounded GBFS snapshot publication."""

    execution_id: str
    feed_name: str
    source_url: str
    source_last_updated: int
    collected_at: str
    ttl_seconds: int
    candidate_events: int
    duplicate_events_skipped: int
    published_events: int
    event_ids: tuple[str, ...]
    outcome: PublishOutcome | None = None

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "execution_id": self.execution_id,
            "feed_name": self.feed_name,
            "source_url": self.source_url,
            "source_last_updated": self.source_last_updated,
            "collected_at": self.collected_at,
            "ttl_seconds": self.ttl_seconds,
            "candidate_events": self.candidate_events,
            "duplicate_events_skipped": self.duplicate_events_skipped,
            "published_events": self.published_events,
            "event_ids": list(self.event_ids),
        }
        if self.outcome is not None:
            data["delivery"] = self.outcome.as_dict()
        return data


def publish_snapshot(
    client: GBFSClient,
    publisher: Publisher,
    *,
    execution_id: str,
    max_publish_events: int,
    seen_event_ids: set[str] | None = None,
) -> PublishReport:
    """Fetch, validate, and publish exactly one bounded station-status snapshot."""
    if max_publish_events < 1:
        raise ValueError("max_publish_events must be positive.")
    feed = client.fetch_station_status()
    events = build_events(feed, seen_event_ids, execution_id=execution_id)
    if len(events) > max_publish_events:
        raise RuntimeError(
            f"Refusing to publish {len(events)} events; limit is {max_publish_events}."
        )
    outcome = publisher.send(events)
    published = outcome.confirmed_events
    if published != len(events):
        raise RuntimeError(
            f"Publisher confirmed {published} events, but {len(events)} were prepared."
        )
    return PublishReport(
        execution_id=execution_id,
        feed_name=feed.name,
        source_url=feed.url,
        source_last_updated=feed.source_last_updated,
        collected_at=feed.collected_at,
        ttl_seconds=feed.ttl_seconds,
        candidate_events=len(feed.stations),
        duplicate_events_skipped=len(feed.stations) - len(events),
        published_events=published,
        event_ids=tuple(str(event["event_id"]) for event in events),
        outcome=outcome,
    )


def publish_once(
    client: GBFSClient,
    publisher: Publisher,
    *,
    seen_event_ids: set[str] | None = None,
    execution_id: str = "bounded-local",
    max_publish_events: int = 5000,
) -> tuple[int, int]:
    report = publish_snapshot(
        client,
        publisher,
        execution_id=execution_id,
        max_publish_events=max_publish_events,
        seen_event_ids=seen_event_ids,
    )
    return report.published_events, report.ttl_seconds


def run_bounded(
    client: GBFSClient,
    publisher: Publisher,
    *,
    poll_count: int,
    minimum_interval_seconds: int = 60,
    max_publish_events: int = 5000,
    execution_id: str | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    """Publish a finite number of polls and honor the feed-provided TTL."""
    if poll_count < 1:
        raise ValueError("poll_count must be at least 1.")
    seen: set[str] = set()
    total = 0
    run_execution_id = execution_id or new_execution_id()
    for index in range(poll_count):
        sent, ttl = publish_once(
            client,
            publisher,
            seen_event_ids=seen,
            execution_id=run_execution_id,
            max_publish_events=max_publish_events,
        )
        total += sent
        if index < poll_count - 1:
            sleep_fn(max(ttl, minimum_interval_seconds))
    return total

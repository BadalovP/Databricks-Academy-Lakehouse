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


class Publisher(Protocol):
    def send(self, events: Iterable[dict[str, Any]]) -> int: ...


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


class PartialPublishError(RuntimeError):
    """A batch send failed after earlier events had already reached the Event Hub."""

    def __init__(self, published_events: int, cause: BaseException) -> None:
        if published_events:
            message = (
                f"Event Hubs send failed after {published_events} events were already published "
                f"({type(cause).__name__}); reconcile those orphan events before retrying."
            )
        else:
            message = (
                f"Event Hubs send failed before any event was confirmed ({type(cause).__name__})."
            )
        super().__init__(message)
        self.published_events = published_events


@dataclass
class EventHubsPublisher:
    """Azure SDK adapter; credentials stay in memory and are never logged or rendered."""

    # repr=False keeps the SAS key out of repr(), str(), f-strings and pytest tracebacks.
    connection_string: str = field(repr=False)
    event_hub_name: str
    retry_total: int = DEFAULT_EVENTHUB_RETRY_TOTAL
    retry_backoff_seconds: float = DEFAULT_EVENTHUB_RETRY_BACKOFF_SECONDS

    @classmethod
    def from_environment(cls) -> "EventHubsPublisher":
        connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
        event_hub_name = os.getenv("AZURE_EVENTHUB_NAME")
        if not connection_string or not event_hub_name:
            raise RuntimeError(
                "AZURE_EVENTHUB_CONNECTION_STRING and AZURE_EVENTHUB_NAME are required."
            )
        return cls(connection_string=connection_string, event_hub_name=event_hub_name)

    def send(self, events: Iterable[dict[str, Any]]) -> int:
        """Publish events in batches and report how many were accepted before any failure."""
        from azure.eventhub import EventData, EventHubProducerClient

        client = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.event_hub_name,
            retry_total=self.retry_total,
            retry_backoff_factor=self.retry_backoff_seconds,
        )
        sent = 0
        failure: BaseException | None = None
        try:
            batch = client.create_batch()
            for event in events:
                message = EventData(json.dumps(event, separators=(",", ":"), sort_keys=True))
                try:
                    batch.add(message)
                except ValueError as error:
                    if len(batch) == 0:
                        raise ValueError(
                            "One UrbanFlow event exceeds the Event Hubs batch limit."
                        ) from error
                    client.send_batch(batch)
                    sent += len(batch)
                    batch = client.create_batch()
                    batch.add(message)
            if len(batch):
                client.send_batch(batch)
                sent += len(batch)
        except Exception as error:
            # Remember the failure so the client is closed before it is re-raised.
            failure = error
        finally:
            client.close()
        if failure is not None:
            # Suppress the SDK exception chain because an upstream exception message could echo
            # connection details. The sanitized error preserves the type and confirmed count.
            raise PartialPublishError(sent, failure) from None
        logger.info("Published %s UrbanFlow station observations.", sent)
        return sent


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

    def as_dict(self) -> dict[str, Any]:
        return {
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
    published = publisher.send(events)
    if published != len(events):
        raise RuntimeError(
            f"Publisher reported {published} events, but {len(events)} were prepared."
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

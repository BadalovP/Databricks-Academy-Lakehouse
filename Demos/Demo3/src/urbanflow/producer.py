"""Bounded Citi Bike GBFS producer for Azure Event Hubs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Protocol

from urbanflow.gbfs_client import GBFSClient, GBFSFeed

logger = logging.getLogger(__name__)


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


def build_events(feed: GBFSFeed, seen_event_ids: set[str] | None = None) -> list[dict[str, Any]]:
    """Build JSON-safe events and omit duplicates already seen by this bounded process."""
    seen = seen_event_ids if seen_event_ids is not None else set()
    events: list[dict[str, Any]] = []
    for station in feed.stations:
        identifier = event_id(station)
        if identifier in seen:
            continue
        event = dict(station)
        event["event_id"] = identifier
        event["gbfs_version"] = feed.version
        events.append(event)
        seen.add(identifier)
    return events


@dataclass
class EventHubsPublisher:
    """Azure SDK adapter; credentials stay in memory and are never logged."""

    connection_string: str
    event_hub_name: str

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
        from azure.eventhub import EventData, EventHubProducerClient

        client = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string, eventhub_name=self.event_hub_name
        )
        sent = 0
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
        finally:
            client.close()
        logger.info("Published %s UrbanFlow station observations.", sent)
        return sent


def publish_once(
    client: GBFSClient,
    publisher: Publisher,
    *,
    seen_event_ids: set[str] | None = None,
) -> tuple[int, int]:
    feed = client.fetch_station_status()
    events = build_events(feed, seen_event_ids)
    return publisher.send(events), feed.ttl_seconds


def run_bounded(
    client: GBFSClient,
    publisher: Publisher,
    *,
    poll_count: int,
    minimum_interval_seconds: int = 60,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    """Publish a finite number of polls and honor the feed-provided TTL."""
    if poll_count < 1:
        raise ValueError("poll_count must be at least 1.")
    seen: set[str] = set()
    total = 0
    for index in range(poll_count):
        sent, ttl = publish_once(client, publisher, seen_event_ids=seen)
        total += sent
        if index < poll_count - 1:
            sleep_fn(max(ttl, minimum_interval_seconds))
    return total

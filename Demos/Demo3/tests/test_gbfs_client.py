from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from urbanflow.gbfs_client import (
    REQUIRED_STATUS_FIELDS,
    GBFSClient,
    GBFSError,
    discover_feed_urls,
    parse_station_feed,
)


def test_discover_feed_urls_supports_official_language_shape() -> None:
    payload = {
        "data": {
            "en": {
                "feeds": [
                    {"name": "station_information", "url": "https://example/info"},
                    {"name": "station_status", "url": "https://example/status"},
                ]
            }
        }
    }
    assert discover_feed_urls(payload) == {
        "station_information": "https://example/info",
        "station_status": "https://example/status",
    }


def test_saved_real_status_sample_parses(status_payload: dict) -> None:
    feed = parse_station_feed(
        status_payload,
        name="station_status",
        url="saved-sample",
        required_fields=REQUIRED_STATUS_FIELDS,
        collected_at=datetime(2026, 9, 28, tzinfo=UTC),
    )
    assert len(feed.stations) == 40
    assert feed.ttl_seconds == 60
    assert feed.stations[0]["collected_at"] == "2026-09-28T00:00:00Z"


def test_missing_required_field_is_rejected(status_payload: dict) -> None:
    status_payload = dict(status_payload)
    status_payload["data"] = {"stations": [dict(status_payload["data"]["stations"][0])]}
    status_payload["data"]["stations"][0].pop("station_id")
    with pytest.raises(GBFSError, match="station_id"):
        parse_station_feed(
            status_payload,
            name="station_status",
            url="saved-sample",
            required_fields=REQUIRED_STATUS_FIELDS,
            collected_at=datetime.now(UTC),
        )


def test_requests_timeout_becomes_domain_error() -> None:
    import requests

    session = Mock()
    session.headers = {}
    session.get.side_effect = requests.Timeout("network stalled")
    client = GBFSClient(session=session)
    with pytest.raises(GBFSError, match="Could not retrieve"):
        client.discover()

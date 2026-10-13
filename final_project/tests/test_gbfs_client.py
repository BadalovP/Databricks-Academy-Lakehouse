from __future__ import annotations

import io
import json
import time
from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
import requests
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import ProtocolError
from urllib3.response import HTTPResponse

from urbanflow.gbfs_client import (
    DEFAULT_DISCOVERY_URL,
    REQUIRED_STATUS_FIELDS,
    GBFSClient,
    GBFSError,
    build_retrying_session,
    discover_feed_urls,
    parse_station_feed,
)

DISCOVERY_URL = "https://example.test/gbfs.json"
DISCOVERY_PAYLOAD = {
    "data": {
        "en": {
            "feeds": [
                {"name": "station_information", "url": "https://example.test/info"},
                {"name": "station_status", "url": "https://example.test/status"},
            ]
        }
    }
}


def test_default_discovery_url_uses_the_working_versioned_citi_bike_feed() -> None:
    assert DEFAULT_DISCOVERY_URL == "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"


def stub_transport(
    monkeypatch: pytest.MonkeyPatch,
    steps: list[object],
    *,
    payload: dict | None = None,
    headers: dict[str, str] | None = None,
) -> list[str]:
    """Replace urllib3's lowest transport call so the real retry logic runs with no network.

    Each step is either an exception to raise or a status code to return, and the last step
    repeats. The returned list records the HTTP method of every attempt actually made.
    """
    attempts: list[str] = []

    def fake_make_request(pool: object, conn: object, method: str, url: str, **kwargs: object):
        attempts.append(method)
        step = steps[min(len(attempts) - 1, len(steps) - 1)]
        if isinstance(step, Exception):
            raise step
        body = json.dumps(payload if payload is not None else {}).encode("utf-8")
        return HTTPResponse(
            body=io.BytesIO(body),
            headers={"Content-Type": "application/json", **(headers or {})},
            status=int(str(step)),
            version=11,
            reason="transport stub",
            preload_content=False,
        )

    monkeypatch.setattr(HTTPConnectionPool, "_make_request", fake_make_request)
    return attempts


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


@pytest.mark.parametrize(
    ("field", "value"),
    [("last_updated", 0), ("last_updated", "invalid"), ("ttl", -1), ("ttl", "invalid")],
)
def test_invalid_feed_metadata_is_rejected(status_payload: dict, field: str, value: object) -> None:
    invalid_payload = deepcopy(status_payload)
    invalid_payload[field] = value
    with pytest.raises(GBFSError, match="invalid last_updated or ttl"):
        parse_station_feed(
            invalid_payload,
            name="station_status",
            url="saved-sample",
            required_fields=REQUIRED_STATUS_FIELDS,
            collected_at=datetime.now(UTC),
        )


def test_requests_timeout_becomes_domain_error() -> None:
    session = Mock()
    session.headers = {}
    session.get.side_effect = requests.Timeout("network stalled")
    client = GBFSClient(session=session)
    with pytest.raises(GBFSError, match="Could not retrieve"):
        client.discover()
    # An injected session is used exactly as the caller built it, so we add no retries to it.
    assert session.get.call_count == 1


def test_timeout_is_passed_on_every_request(status_payload: dict) -> None:
    session = Mock()
    session.headers = {}
    session.get.side_effect = [
        Mock(json=Mock(return_value=DISCOVERY_PAYLOAD), raise_for_status=Mock()),
        Mock(json=Mock(return_value=status_payload), raise_for_status=Mock()),
    ]
    client = GBFSClient(DISCOVERY_URL, timeout_seconds=7, session=session)

    client.fetch_station_status()

    assert [call.kwargs["timeout"] for call in session.get.call_args_list] == [7, 7]


def test_default_session_retries_only_transient_gets() -> None:
    client = GBFSClient(retry_attempts=4, retry_backoff_seconds=0.25)
    retry = client.session.get_adapter("https://gbfs.citibikenyc.com").max_retries

    assert retry.total == 3  # four attempts means three retries
    assert retry.backoff_factor == 0.25
    assert retry.respect_retry_after_header is True
    assert retry.is_retry("GET", 429)
    assert retry.is_retry("GET", 503)
    assert not retry.is_retry("GET", 404)
    assert not retry.is_retry("POST", 503)


@pytest.mark.parametrize(
    ("attempts", "backoff"),
    [(0, 0.5), (3, -0.1)],
)
def test_retry_configuration_rejects_invalid_bounds(attempts: int, backoff: float) -> None:
    with pytest.raises(ValueError):
        build_retrying_session(attempts=attempts, backoff_seconds=backoff)


def test_injected_session_keeps_its_own_transport() -> None:
    session = requests.Session()
    original_adapter = session.get_adapter("https://example.test")

    client = GBFSClient(session=session)

    assert client.session is session
    assert client.session.get_adapter("https://example.test") is original_adapter
    assert original_adapter.max_retries.total == 0


def test_transient_failure_then_success_returns_the_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = stub_transport(
        monkeypatch, [ProtocolError("connection reset"), 200], payload=DISCOVERY_PAYLOAD
    )
    client = GBFSClient(DISCOVERY_URL, retry_backoff_seconds=0)

    assert client.discover()["station_status"] == "https://example.test/status"
    assert attempts == ["GET", "GET"]


def test_transient_failures_are_bounded_and_then_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = stub_transport(monkeypatch, [503], payload=DISCOVERY_PAYLOAD)
    client = GBFSClient(DISCOVERY_URL, retry_attempts=3, retry_backoff_seconds=0)

    with pytest.raises(GBFSError, match="Could not retrieve"):
        client.discover()

    assert len(attempts) == 3


def test_not_found_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = stub_transport(monkeypatch, [404], payload=DISCOVERY_PAYLOAD)
    client = GBFSClient(DISCOVERY_URL, retry_attempts=3, retry_backoff_seconds=0)

    with pytest.raises(GBFSError, match="Could not retrieve"):
        client.discover()

    assert len(attempts) == 1


def test_retry_after_header_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    attempts = stub_transport(
        monkeypatch, [429, 200], payload=DISCOVERY_PAYLOAD, headers={"Retry-After": "2"}
    )
    client = GBFSClient(DISCOVERY_URL, retry_backoff_seconds=0)

    client.discover()

    assert len(attempts) == 2
    assert slept == [2.0]  # the server's Retry-After wins over our own backoff

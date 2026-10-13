"""Citi Bike GBFS discovery, retrieval, and response validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

DEFAULT_DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5
TRANSIENT_STATUS_CODES = (429, 500, 502, 503, 504)
REQUIRED_INFORMATION_FIELDS = {"station_id", "name", "lat", "lon", "capacity"}
REQUIRED_STATUS_FIELDS = {
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
}


class GBFSError(RuntimeError):
    """Raised when GBFS discovery, transport, or validation fails."""


@dataclass(frozen=True)
class GBFSFeed:
    name: str
    url: str
    version: str
    ttl_seconds: int
    source_last_updated: int
    collected_at: str
    stations: list[dict[str, Any]]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_retrying_session(
    *,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> requests.Session:
    """Return a session that retries only transient GET failures, with backoff."""
    if attempts < 1:
        raise ValueError("attempts must be at least 1.")
    if backoff_seconds < 0:
        raise ValueError("backoff_seconds must be non-negative.")
    retry = Retry(
        total=max(attempts - 1, 0),
        status_forcelist=TRANSIENT_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        backoff_factor=backoff_seconds,
        respect_retry_after_header=True,
        # Hand the last transient response back so raise_for_status() reports the real status
        # instead of a generic urllib3 retry error.
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def discover_feed_urls(payload: dict[str, Any], language: str = "en") -> dict[str, str]:
    """Return feed-name to URL mappings for GBFS 2.x and 3.x discovery shapes."""
    data = payload.get("data")
    if not isinstance(data, dict):
        raise GBFSError("GBFS discovery response is missing the data object.")

    language_node = data.get(language)
    if isinstance(language_node, dict):
        feeds = language_node.get("feeds")
    else:
        feeds = data.get("feeds")
    if not isinstance(feeds, list):
        raise GBFSError(f"GBFS discovery response has no feed list for language {language!r}.")

    result: dict[str, str] = {}
    for feed in feeds:
        if (
            isinstance(feed, dict)
            and isinstance(feed.get("name"), str)
            and isinstance(feed.get("url"), str)
        ):
            result[feed["name"]] = feed["url"]
    if "station_information" not in result or "station_status" not in result:
        raise GBFSError("GBFS discovery did not publish both required station feeds.")
    return result


def parse_station_feed(
    payload: dict[str, Any],
    *,
    name: str,
    url: str,
    required_fields: set[str],
    collected_at: datetime,
) -> GBFSFeed:
    """Validate one station feed envelope and preserve source/collection timestamps."""
    try:
        source_last_updated = int(payload["last_updated"])
        ttl_seconds = int(payload["ttl"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GBFSError(f"{name} response has invalid last_updated or ttl metadata.") from exc
    if source_last_updated <= 0 or ttl_seconds < 0:
        raise GBFSError(f"{name} response has invalid last_updated or ttl metadata.")
    data = payload.get("data")
    stations = data.get("stations") if isinstance(data, dict) else None
    if not isinstance(stations, list):
        raise GBFSError(f"{name} response is missing data.stations.")

    normalized: list[dict[str, Any]] = []
    for index, station in enumerate(stations):
        if not isinstance(station, dict):
            raise GBFSError(f"{name} station at index {index} is not an object.")
        missing = sorted(required_fields - station.keys())
        if missing:
            raise GBFSError(f"{name} station at index {index} is missing fields: {missing}")
        record = dict(station)
        record["source_last_updated"] = source_last_updated
        record["collected_at"] = _iso_utc(collected_at)
        record["source_url"] = url
        normalized.append(record)

    return GBFSFeed(
        name=name,
        url=url,
        version=str(payload.get("version", "unknown")),
        ttl_seconds=ttl_seconds,
        source_last_updated=source_last_updated,
        collected_at=_iso_utc(collected_at),
        stations=normalized,
    )


class GBFSClient:
    """Small requests-based client that discovers rather than guesses feed URLs.

    Every request carries an explicit timeout, and transient GET failures are retried a
    bounded number of times with backoff unless the caller injects their own session.
    """

    def __init__(
        self,
        discovery_url: str = DEFAULT_DISCOVERY_URL,
        *,
        language: str = "en",
        timeout_seconds: float = 20,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        session: requests.Session | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.discovery_url = discovery_url
        self.language = language
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        # An injected session belongs to the caller (tests), so we never re-mount its transport.
        self.session = session or build_retrying_session(
            attempts=retry_attempts, backoff_seconds=retry_backoff_seconds
        )
        self.clock = clock
        self.session.headers.setdefault(
            "User-Agent", "UrbanFlow educational project/0.1 (GBFS client)"
        )
        self._feed_urls: dict[str, str] | None = None

    def _get_json(self, url: str) -> dict[str, Any]:
        # Transient failures (connection errors, timeouts, 429/5xx) are retried by the session's
        # adapter; a non-transient status such as 404 fails here on the first attempt.
        try:
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise GBFSError(f"Could not retrieve valid JSON from {url}: {exc}") from exc
        if not isinstance(payload, dict):
            raise GBFSError(f"Expected a JSON object from {url}.")
        return payload

    def discover(self, *, refresh: bool = False) -> dict[str, str]:
        if self._feed_urls is None or refresh:
            self._feed_urls = discover_feed_urls(
                self._get_json(self.discovery_url), language=self.language
            )
        return dict(self._feed_urls)

    def fetch_station_information(self) -> GBFSFeed:
        url = self.discover()["station_information"]
        return parse_station_feed(
            self._get_json(url),
            name="station_information",
            url=url,
            required_fields=REQUIRED_INFORMATION_FIELDS,
            collected_at=self.clock(),
        )

    def fetch_station_status(self) -> GBFSFeed:
        url = self.discover()["station_status"]
        return parse_station_feed(
            self._get_json(url),
            name="station_status",
            url=url,
            required_fields=REQUIRED_STATUS_FIELDS,
            collected_at=self.clock(),
        )

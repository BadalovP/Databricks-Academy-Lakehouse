"""
Lab 05 — Citi Bike GBFS single-shot status producer.

Fetch the current Citi Bike station_status payload and write exactly one
immutable timestamped JSON snapshot into the EXTERNAL Lab 05 streaming volume.

The external volume is required because the Lakeflow Auto Loader source is
Unity Catalog governed.

Typical Databricks usage:
    python labs/lab_05_lakeflow/tools/citibike_status_producer.py
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
DEFAULT_CATALOG = "dbr_dev"
DEFAULT_SCHEMA = "parvinbadalov"
DEFAULT_STREAMING_VOLUME_NAME = "lab05_lakeflow_streaming"

REQUEST_TIMEOUT_SECONDS = 30


def get_json(url: str) -> dict[str, Any]:
    """Download JSON and fail clearly for HTTP/network errors."""
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": "Databricks-Lab05-Lakeflow/1.0",
            "Accept": "application/json",
        },
    )
    response.raise_for_status()
    return response.json()


def resolve_station_status_url(
    discovery_url: str = DEFAULT_DISCOVERY_URL,
) -> str:
    """Resolve the station_status endpoint from the GBFS discovery feed."""
    discovery = get_json(discovery_url)

    try:
        feeds = discovery["data"]["en"]["feeds"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "Unexpected GBFS discovery structure: expected data.en.feeds."
        ) from exc

    for feed in feeds:
        if feed.get("name") == "station_status":
            url = feed.get("url")
            if url:
                return str(url)

    raise ValueError(
        "The GBFS discovery document does not contain "
        "a usable 'station_status' feed."
    )


def validate_station_status_payload(payload: dict[str, Any]) -> int:
    """Validate the minimum structure required by Lab 05."""
    try:
        stations = payload["data"]["stations"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "Unexpected station_status structure: expected data.stations."
        ) from exc

    if not isinstance(stations, list):
        raise ValueError("data.stations must be an array.")

    if not stations:
        raise ValueError("station_status returned no stations.")

    missing_station_id = sum(
        1 for station in stations if not station.get("station_id")
    )

    if missing_station_id:
        raise ValueError(
            f"{missing_station_id} station_status records "
            "are missing station_id."
        )

    return len(stations)


def build_output_directory(
    catalog: str,
    schema: str,
    streaming_volume_name: str,
) -> Path:
    """Build the EXTERNAL streaming landing directory."""
    return Path(
        f"/Volumes/{catalog}/{schema}/{streaming_volume_name}"
        "/landing/station_status"
    )


def write_snapshot(
    payload: dict[str, Any],
    output_directory: Path,
) -> Path:
    """Write one snapshot through a temp file, then atomically rename it."""
    if not output_directory.exists():
        raise FileNotFoundError(
            f"Streaming landing directory does not exist: "
            f"{output_directory}. Run lab05_00_setup first."
        )

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%S%fZ")

    final_path = output_directory / (
        f"station_status_{timestamp}.json"
    )
    temp_path = output_directory / (
        f".station_status_{timestamp}.tmp"
    )

    temp_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    temp_path.replace(final_path)

    return final_path


def produce_snapshot(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    streaming_volume_name: str = DEFAULT_STREAMING_VOLUME_NAME,
    discovery_url: str = DEFAULT_DISCOVERY_URL,
) -> tuple[Path, int, str]:
    """Fetch, validate, and persist one Citi Bike status snapshot."""
    station_status_url = resolve_station_status_url(discovery_url)
    payload = get_json(station_status_url)
    station_count = validate_station_status_payload(payload)

    output_directory = build_output_directory(
        catalog=catalog,
        schema=schema,
        streaming_volume_name=streaming_volume_name,
    )

    snapshot_path = write_snapshot(
        payload=payload,
        output_directory=output_directory,
    )

    return snapshot_path, station_count, station_status_url


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments while tolerating Databricks kernel arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Write one Citi Bike station_status snapshot "
            "to the Lab 05 external streaming volume."
        )
    )

    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument(
        "--streaming-volume-name",
        default=DEFAULT_STREAMING_VOLUME_NAME,
    )
    parser.add_argument(
        "--discovery-url",
        default=DEFAULT_DISCOVERY_URL,
    )

    args, _ = parser.parse_known_args()
    return args


def main() -> None:
    """CLI entry point."""
    args = parse_args()

    snapshot_path, station_count, status_url = produce_snapshot(
        catalog=args.catalog,
        schema=args.schema,
        streaming_volume_name=args.streaming_volume_name,
        discovery_url=args.discovery_url,
    )

    print("✅ CITI BIKE STATUS SNAPSHOT CREATED")
    print()
    print(f"Stations        : {station_count:,}")
    print(f"Source feed     : {status_url}")
    print(f"Snapshot file   : {snapshot_path}")
    print()
    print(
        "Producer behavior: one API poll -> "
        "one immutable JSON file -> exit"
    )


if __name__ == "__main__":
    main()

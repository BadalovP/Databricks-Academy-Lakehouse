"""Refresh small attributed real-data samples without downloading the full trip ZIP."""

from __future__ import annotations

import argparse
import csv
import io
import json
import struct
import sys
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.config import load_config  # noqa: E402
from urbanflow.gbfs_client import GBFSClient  # noqa: E402

HISTORICAL_URL = "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip"
SAMPLE_SIZE = 40
USER_AGENT = "UrbanFlow educational project/0.1 (small attributed sample)"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _get_json(session: requests.Session, url: str, params: dict[str, Any] | None = None) -> Any:
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _fetch_gbfs(
    session: requests.Session, cfg: Any
) -> tuple[dict[str, str], dict[str, Any], dict[str, Any]]:
    client = GBFSClient(
        cfg.sources.gbfs_discovery_url,
        language=cfg.sources.gbfs_language,
        session=session,
    )
    urls = client.discover(refresh=True)
    information_payload = _get_json(session, urls["station_information"])
    status_payload = _get_json(session, urls["station_status"])
    return urls, information_payload, status_payload


def _write_gbfs_samples(
    output_dir: Path,
    cfg: Any,
    urls: dict[str, str],
    information_payload: dict[str, Any],
    status_payload: dict[str, Any],
    preferred_short_names: set[str],
) -> set[str]:
    information_rows = information_payload["data"]["stations"]
    status_rows = status_payload["data"]["stations"]
    status_ids = {str(row["station_id"]) for row in status_rows}
    ordered_information = sorted(
        information_rows,
        key=lambda item: (
            str(item.get("short_name")) not in preferred_short_names,
            str(item["station_id"]),
        ),
    )
    station_ids = [
        str(row["station_id"])
        for row in ordered_information
        if str(row["station_id"]) in status_ids
    ][:SAMPLE_SIZE]
    selected = set(station_ids)

    information_sample = {
        key: information_payload[key]
        for key in ("last_updated", "ttl", "version")
        if key in information_payload
    }
    information_sample["data"] = {
        "stations": [row for row in information_rows if str(row["station_id"]) in selected]
    }
    status_sample = {
        key: status_payload[key]
        for key in ("last_updated", "ttl", "version")
        if key in status_payload
    }
    status_sample["data"] = {
        "stations": [row for row in status_rows if str(row["station_id"]) in selected]
    }
    _write_json(output_dir / "station_information.sample.json", information_sample)
    _write_json(output_dir / "station_status.sample.json", status_sample)
    _write_json(
        output_dir / "gbfs_discovery.sample.json",
        {
            "verified_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "discovery_url": cfg.sources.gbfs_discovery_url,
            "feeds": {
                "station_information": urls["station_information"],
                "station_status": urls["station_status"],
            },
            "sample_station_count": len(selected),
        },
    )
    return selected


def _sample_weather(session: requests.Session, output_dir: Path, cfg: Any) -> None:
    weather = _get_json(
        session,
        cfg.sources.open_meteo_url,
        params={
            "latitude": cfg.sources.weather_latitude,
            "longitude": cfg.sources.weather_longitude,
            "current": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "America/New_York",
        },
    )
    _write_json(output_dir / "open_meteo.sample.json", weather)


def _range_get(session: requests.Session, url: str, start: int, end: int) -> bytes:
    response = session.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=60, stream=True)
    try:
        if response.status_code != 206:
            raise RuntimeError(
                f"Historical source did not honor the bounded byte range: {response.status_code}"
            )
        return response.content
    finally:
        response.close()


def _zip_entries(session: requests.Session, url: str) -> tuple[int, list[dict[str, Any]]]:
    head = session.head(url, timeout=30)
    head.raise_for_status()
    size = int(head.headers["Content-Length"])
    tail_start = max(size - 131_072, 0)
    tail = _range_get(session, url, tail_start, size - 1)
    eocd_index = tail.rfind(b"PK\x05\x06")
    if eocd_index < 0:
        raise RuntimeError("Could not locate the historical ZIP central directory.")
    eocd = struct.unpack_from("<4s4H2LH", tail, eocd_index)
    entry_count, central_size, central_offset = eocd[4], eocd[5], eocd[6]
    central = _range_get(session, url, central_offset, central_offset + central_size - 1)

    entries: list[dict[str, Any]] = []
    offset = 0
    for _ in range(entry_count):
        values = struct.unpack_from("<4s6H3L5H2L", central, offset)
        if values[0] != b"PK\x01\x02":
            raise RuntimeError("Unexpected ZIP central-directory signature.")
        compression = values[4]
        compressed_size = values[8]
        uncompressed_size = values[9]
        name_length, extra_length, comment_length = values[10], values[11], values[12]
        local_offset = values[16]
        name_start = offset + 46
        name = central[name_start : name_start + name_length].decode("utf-8")
        entries.append(
            {
                "name": name,
                "compression": compression,
                "compressed_size": compressed_size,
                "uncompressed_size": uncompressed_size,
                "local_offset": local_offset,
            }
        )
        offset += 46 + name_length + extra_length + comment_length
    return size, entries


def _read_csv_prefix(
    session: requests.Session, url: str, entry: dict[str, Any], compressed_bytes: int = 8_000_000
) -> str:
    local_offset = int(entry["local_offset"])
    header = _range_get(session, url, local_offset, local_offset + 29)
    values = struct.unpack("<4s5H3L2H", header)
    if values[0] != b"PK\x03\x04":
        raise RuntimeError("Unexpected ZIP local-file signature.")
    name_length, extra_length = values[9], values[10]
    data_start = local_offset + 30 + name_length + extra_length
    amount = min(int(entry["compressed_size"]), compressed_bytes)
    compressed = _range_get(session, url, data_start, data_start + amount - 1)
    if int(entry["compression"]) == 8:
        uncompressed = zlib.decompressobj(-15).decompress(compressed)
    elif int(entry["compression"]) == 0:
        uncompressed = compressed
    else:
        raise RuntimeError(f"Unsupported ZIP compression method: {entry['compression']}")
    return uncompressed.decode("utf-8-sig", errors="strict")


def _sample_historical(
    session: requests.Session, output_dir: Path, current_short_names: set[str]
) -> tuple[dict[str, Any], set[str]]:
    archive_size, entries = _zip_entries(session, HISTORICAL_URL)
    csv_entry = next(entry for entry in entries if str(entry["name"]).lower().endswith(".csv"))
    prefix = _read_csv_prefix(session, HISTORICAL_URL, csv_entry)
    complete_lines = prefix.splitlines()
    reader = csv.DictReader(io.StringIO("\n".join(complete_lines[:-1])))
    rows: list[dict[str, str]] = []
    matched = 0
    historical_station_ids: set[str] = set()
    for row in reader:
        if not row.get("ride_id") or not row.get("started_at"):
            continue
        historical_id = str(row.get("start_station_id", ""))
        historical_station_ids.add(historical_id)
        if historical_id in current_short_names:
            matched += 1
        rows.append(row)
        if len(rows) >= SAMPLE_SIZE:
            break
    if not rows:
        raise RuntimeError("No complete historical CSV rows were decoded from the bounded range.")

    output_path = output_dir / "historical_trips_202401_sample.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    metadata = {
        "source_url": HISTORICAL_URL,
        "archive_object": Path(HISTORICAL_URL).name,
        "archive_size_bytes": archive_size,
        "archive_member": csv_entry["name"],
        "sample_rows": len(rows),
        "sample_rows_matching_current_short_names": matched,
        "join_note": "Historical start_station_id maps to current GBFS short_name, not the UUID station_id.",
        "method": "HTTP byte-range read of the first CSV member; full archive not downloaded",
        "verified_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    _write_json(output_dir / "historical_trips_202401_sample.metadata.json", metadata)
    return metadata, historical_station_ids


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    output_dir = PROJECT_ROOT / "data" / "samples"
    output_dir.mkdir(parents=True, exist_ok=True)
    expected = output_dir / "station_status.sample.json"
    if expected.exists() and not args.overwrite:
        raise SystemExit("Samples already exist. Pass --overwrite to refresh them deliberately.")

    cfg = load_config(PROJECT_ROOT / "config" / "dev.yml")
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    urls, information_payload, status_payload = _fetch_gbfs(session, cfg)
    current_short_names = {
        str(row["short_name"])
        for row in information_payload["data"]["stations"]
        if row.get("short_name")
    }
    history, historical_station_ids = _sample_historical(session, output_dir, current_short_names)
    selected_station_ids = _write_gbfs_samples(
        output_dir,
        cfg,
        urls,
        information_payload,
        status_payload,
        historical_station_ids,
    )
    _sample_weather(session, output_dir, cfg)
    print(
        json.dumps(
            {
                "gbfs_station_sample": len(selected_station_ids),
                "historical_trip_sample": history["sample_rows"],
                "historical_current_short_name_matches": history[
                    "sample_rows_matching_current_short_names"
                ],
                "weather_sample": 1,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

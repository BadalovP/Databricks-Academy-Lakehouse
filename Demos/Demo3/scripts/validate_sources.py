"""Validate saved samples by default; use --live for read-only public API checks."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from urbanflow.config import load_config  # noqa: E402
from urbanflow.gbfs_client import GBFSClient, parse_station_feed  # noqa: E402


def validate_saved_samples(sample_dir: Path) -> dict[str, int]:
    information_payload = json.loads(
        (sample_dir / "station_information.sample.json").read_text(encoding="utf-8")
    )
    status_payload = json.loads(
        (sample_dir / "station_status.sample.json").read_text(encoding="utf-8")
    )
    from datetime import UTC, datetime

    collected = datetime.now(UTC)
    information = parse_station_feed(
        information_payload,
        name="station_information",
        url="saved-sample",
        required_fields={"station_id", "name", "lat", "lon", "capacity"},
        collected_at=collected,
    )
    status = parse_station_feed(
        status_payload,
        name="station_status",
        url="saved-sample",
        required_fields={
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning",
            "last_reported",
        },
        collected_at=collected,
    )
    with (sample_dir / "historical_trips_202401_sample.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        trip_count = sum(1 for _ in csv.DictReader(handle))
    weather = json.loads((sample_dir / "open_meteo.sample.json").read_text(encoding="utf-8"))
    required_weather = {"temperature_2m", "precipitation", "wind_speed_10m", "time"}
    if not required_weather.issubset(weather.get("current", {})):
        raise ValueError("Saved Open-Meteo sample is missing required current fields.")
    return {
        "station_information": len(information.stations),
        "station_status": len(status.stations),
        "historical_trips": trip_count,
        "weather_observations": 1,
    }


def validate_live(config_path: Path) -> dict[str, int]:
    cfg = load_config(config_path)
    client = GBFSClient(cfg.sources.gbfs_discovery_url, language=cfg.sources.gbfs_language)
    information = client.fetch_station_information()
    status = client.fetch_station_status()
    return {
        "station_information": len(information.stations),
        "station_status": len(status.stations),
        "ttl_seconds": status.ttl_seconds,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    if args.live:
        result = validate_live(PROJECT_ROOT / "config" / "dev.yml")
    else:
        result = validate_saved_samples(PROJECT_ROOT / "data" / "samples")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

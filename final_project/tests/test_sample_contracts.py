from __future__ import annotations

import csv
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = PROJECT_ROOT / "data" / "samples"


def test_historical_trip_ids_map_to_current_gbfs_short_names() -> None:
    information = json.loads(
        (SAMPLE_DIR / "station_information.sample.json").read_text(encoding="utf-8")
    )
    stations = information["data"]["stations"]
    station_uuids = {row["station_id"] for row in stations}
    short_names = {row["short_name"] for row in stations}

    with (SAMPLE_DIR / "historical_trips_202401_sample.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        trips = list(csv.DictReader(handle))

    assert len(trips) == 40
    assert {row["start_station_id"] for row in trips} <= short_names
    assert not ({row["start_station_id"] for row in trips} & station_uuids)


def test_samples_keep_source_and_capture_metadata() -> None:
    status = json.loads((SAMPLE_DIR / "station_status.sample.json").read_text(encoding="utf-8"))
    trip_metadata = json.loads(
        (SAMPLE_DIR / "historical_trips_202401_sample.metadata.json").read_text(encoding="utf-8")
    )

    assert status["last_updated"] > 0
    assert status["ttl"] > 0
    assert trip_metadata["source_url"].startswith("https://s3.amazonaws.com/tripdata/")
    assert trip_metadata["sample_rows"] == 40

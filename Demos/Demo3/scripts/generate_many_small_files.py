"""Generate many tiny synthetic CSV files, to demonstrate the small-file problem locally.

WHY THIS EXISTS. The Academy asks for an "approximately 1,000 files" exercise, which is really a
lesson about two things: Auto Loader's ability to discover files incrementally, and the cost of
many small files to a query engine. Both can be shown with ~1,000 files of a few hundred bytes
each - roughly half a megabyte in total. Generating gigabytes to make the same point would be
waste dressed up as realism.

WHAT THESE FILES ARE. Synthetic educational inputs, and labelled as such in every row: the
`ride_id` values are `synthetic-*` and the station identifiers are drawn from the committed GBFS
sample so the short_name join still works. They are NOT Citi Bike data and must never be mixed
with the real archive or the committed 40-row development sample.

SAFETY. This script writes only inside the directory it is given, refuses to touch a directory it
did not create unless `--force` is passed, and `--cleanup` removes exactly the files it generated
rather than emptying a directory. Nothing here uploads to Azure; the upload is a separate,
separately approved step.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

MARKER_NAME = "_synthetic_small_files.json"
FILE_PREFIX = "synthetic_trips_"
HEADER = (
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
)
# Short names taken from the committed GBFS reference sample, so the station join still resolves.
STATIONS = ("7407.13", "6526.01", "6346.07", "6364.07", "5470.10")
BASE = datetime(2024, 1, 2, 6, 0, 0)


def _rows(file_index: int, rows_per_file: int) -> list[tuple]:
    """Deterministic rows: the same index always produces the same content."""
    out = []
    for row_index in range(rows_per_file):
        serial = file_index * rows_per_file + row_index
        station = STATIONS[serial % len(STATIONS)]
        end_station = STATIONS[(serial + 1) % len(STATIONS)]
        started = BASE + timedelta(minutes=serial % (60 * 24 * 28))
        # Durations stay between 2 and 31 minutes so no row trips the quality rules; this file
        # set is for the small-file lesson, not for exercising quarantine.
        ended = started + timedelta(minutes=2 + (serial % 30))
        out.append(
            (
                f"synthetic-{serial:08d}",
                "classic_bike" if serial % 3 else "electric_bike",
                started.strftime("%Y-%m-%d %H:%M:%S"),
                ended.strftime("%Y-%m-%d %H:%M:%S"),
                f"Synthetic Station {station}",
                station,
                f"Synthetic Station {end_station}",
                end_station,
                40.70 + (serial % 50) / 1000.0,
                -73.99 - (serial % 50) / 1000.0,
                40.71 + (serial % 40) / 1000.0,
                -73.98 - (serial % 40) / 1000.0,
                "member" if serial % 4 else "casual",
            )
        )
    return out


def generate(directory: Path, *, files: int, rows_per_file: int, force: bool = False) -> dict:
    """Write `files` tiny CSVs into `directory`, and a marker recording exactly what was written."""
    if files < 1:
        raise ValueError("files must be at least 1.")
    if rows_per_file < 1:
        raise ValueError("rows_per_file must be at least 1.")

    marker = directory / MARKER_NAME
    if directory.exists() and any(directory.iterdir()) and not marker.exists() and not force:
        raise SystemExit(
            f"{directory} is not empty and was not created by this script. Pass --force only if "
            "you are certain nothing else lives there."
        )
    directory.mkdir(parents=True, exist_ok=True)

    # A regenerate must not leave last run's extra files behind, or the count would drift upward.
    for stale in sorted(directory.glob(f"{FILE_PREFIX}*.csv")):
        stale.unlink()

    written = []
    for index in range(files):
        path = directory / f"{FILE_PREFIX}{index:05d}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(HEADER)
            writer.writerows(_rows(index, rows_per_file))
        written.append(path.name)

    total_bytes = sum((directory / name).stat().st_size for name in written)
    manifest = {
        "generator": "scripts/generate_many_small_files.py",
        "label": "SYNTHETIC EDUCATIONAL DATA - not Citi Bike data, not the 40-row development sample",
        "files": len(written),
        "rows_per_file": rows_per_file,
        "total_rows": len(written) * rows_per_file,
        "total_bytes": total_bytes,
        "file_prefix": FILE_PREFIX,
        "deterministic": True,
    }
    marker.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def cleanup(directory: Path) -> dict:
    """Remove only what this script generated, identified by its marker and prefix."""
    marker = directory / MARKER_NAME
    if not marker.exists():
        return {
            "removed_files": 0,
            "removed_directory": False,
            "note": "no marker; nothing owned here",
        }
    removed = 0
    for path in sorted(directory.glob(f"{FILE_PREFIX}*.csv")):
        path.unlink()
        removed += 1
    marker.unlink()
    removed_directory = False
    if not any(directory.iterdir()):
        shutil.rmtree(directory)
        removed_directory = True
    return {"removed_files": removed, "removed_directory": removed_directory}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Target directory, created if absent.")
    parser.add_argument("--files", type=int, default=1000)
    parser.add_argument("--rows-per-file", type=int, default=5)
    parser.add_argument(
        "--force", action="store_true", help="Write into a non-empty foreign directory."
    )
    parser.add_argument("--cleanup", action="store_true", help="Remove generated files and exit.")
    args = parser.parse_args(argv)

    if args.cleanup:
        print(json.dumps(cleanup(args.directory), indent=2, sort_keys=True))
        return 0
    manifest = generate(
        args.directory, files=args.files, rows_per_file=args.rows_per_file, force=args.force
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

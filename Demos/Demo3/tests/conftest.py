from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def sample_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "samples"


@pytest.fixture(scope="session")
def information_payload(sample_dir: Path) -> dict:
    return json.loads((sample_dir / "station_information.sample.json").read_text())


@pytest.fixture(scope="session")
def status_payload(sample_dir: Path) -> dict:
    return json.loads((sample_dir / "station_status.sample.json").read_text())

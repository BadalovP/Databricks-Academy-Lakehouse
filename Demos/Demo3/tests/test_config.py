from __future__ import annotations

from pathlib import Path

import pytest

from urbanflow.config import load_config, normalize_host


def test_dev_config_is_safe_and_identity_prefixed() -> None:
    path = Path(__file__).resolve().parents[1] / "config" / "dev.yml"
    config = load_config(path)
    assert config.azure.schema == "parvinbadalov_urbanflow_dev"
    assert config.azure.event_hub_name == "parvinbadalov_evh"
    assert config.azure.volume_root.startswith("/Volumes/dbr_dev/parvinbadalov_")


def test_normalize_host_adds_scheme_and_removes_trailing_slash() -> None:
    assert normalize_host("ADB-1.AZUREDATABRICKS.NET/") == ("https://adb-1.azuredatabricks.net")


def test_negative_threshold_is_rejected(tmp_path: Path) -> None:
    source = (Path(__file__).resolve().parents[1] / "config" / "dev.yml").read_text()
    bad = tmp_path / "bad.yml"
    bad.write_text(source.replace("low_bike_threshold: 2", "low_bike_threshold: -1"))
    with pytest.raises(ValueError, match="non-negative"):
        load_config(bad)

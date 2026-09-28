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
    assert config.compute.preferred == "gp1"
    assert config.compute.preferred_cluster.cluster_id == "0702-132442-toro5spu"
    assert config.compute.fallback_cluster.cluster_id == "0702-171207-xo9bbc0y"
    assert config.compute.required_state == "RUNNING"
    assert config.compute.allow_start is False
    assert config.compute.allow_restart is False
    assert config.compute.allow_resize is False
    assert config.compute.allow_terminate is False
    assert config.compute.educational_cluster_creation_enabled is False
    assert config.compute.protected_from_termination is True
    assert config.streaming.starting_offsets == "earliest"
    assert config.streaming.max_publish_events == 5000
    assert config.streaming.checkpoint_subpath.startswith("checkpoints/")
    assert config.lakeflow.compute_mode == "serverless"
    assert config.lakeflow.shared_cluster_id is None


def test_normalize_host_adds_scheme_and_removes_trailing_slash() -> None:
    assert normalize_host("ADB-1.AZUREDATABRICKS.NET/") == ("https://adb-1.azuredatabricks.net")


def test_negative_threshold_is_rejected(tmp_path: Path) -> None:
    source = (Path(__file__).resolve().parents[1] / "config" / "dev.yml").read_text()
    bad = tmp_path / "bad.yml"
    bad.write_text(source.replace("low_bike_threshold: 2", "low_bike_threshold: -1"))
    with pytest.raises(ValueError, match="non-negative"):
        load_config(bad)


def test_shared_cluster_mutation_cannot_be_enabled(tmp_path: Path) -> None:
    source = (Path(__file__).resolve().parents[1] / "config" / "dev.yml").read_text()
    bad = tmp_path / "bad.yml"
    bad.write_text(source.replace("allow_restart: false", "allow_restart: true"))

    with pytest.raises(ValueError, match="mutation flags"):
        load_config(bad)


def test_lakeflow_cannot_attach_to_gp1_or_gp2(tmp_path: Path) -> None:
    source = (Path(__file__).resolve().parents[1] / "config" / "dev.yml").read_text()
    bad = tmp_path / "bad.yml"
    bad.write_text(source.replace("shared_cluster_id: null", "shared_cluster_id: GP1"))

    with pytest.raises(ValueError, match="managed compute"):
        load_config(bad)

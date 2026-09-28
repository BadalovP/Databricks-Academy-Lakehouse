"""Small, explicit configuration model for UrbanFlow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def normalize_host(host: str) -> str:
    """Normalize a Databricks host for safe equality checks."""
    value = host.strip().rstrip("/").lower()
    if "://" not in value:
        value = f"https://{value}"
    return value


@dataclass(frozen=True)
class BusinessRules:
    low_bike_threshold: int
    low_dock_threshold: int


@dataclass(frozen=True)
class AzureResources:
    expected_databricks_host: str
    catalog: str
    schema: str
    volume: str
    event_hubs_namespace: str
    event_hub_name: str
    consumer_group: str
    key_vault_name: str
    event_hubs_secret_name: str

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def kafka_bootstrap_servers(self) -> str:
        return f"{self.event_hubs_namespace}.servicebus.windows.net:9093"


@dataclass(frozen=True)
class SourceSettings:
    gbfs_discovery_url: str
    gbfs_language: str
    open_meteo_url: str
    weather_latitude: float
    weather_longitude: float
    historical_object: str


@dataclass(frozen=True)
class StreamingSettings:
    max_events_per_trigger: int
    bounded_trigger: str
    checkpoint_subpath: str
    schema_subpath: str
    minimum_poll_interval_seconds: int


@dataclass(frozen=True)
class UrbanFlowConfig:
    project_name: str
    environment: str
    business_rules: BusinessRules
    sources: SourceSettings
    azure: AzureResources
    streaming: StreamingSettings


def _require_mapping(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Configuration section {key!r} must be a mapping.")
    return value


def load_config(path: str | Path) -> UrbanFlowConfig:
    """Load and validate an UrbanFlow YAML file."""
    config_path = Path(path)
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"Configuration file {config_path} must contain a mapping.")

    project = _require_mapping(raw, "project")
    rules = _require_mapping(raw, "business_rules")
    sources = _require_mapping(raw, "sources")
    azure = _require_mapping(raw, "azure")
    streaming = _require_mapping(raw, "streaming")

    low_bikes = int(rules["low_bike_threshold"])
    low_docks = int(rules["low_dock_threshold"])
    if low_bikes < 0 or low_docks < 0:
        raise ValueError("Availability thresholds must be non-negative integers.")

    expected_host = normalize_host(str(azure["expected_databricks_host"]))
    if not expected_host.endswith(".azuredatabricks.net"):
        raise ValueError("The Azure target must use an azuredatabricks.net host.")

    return UrbanFlowConfig(
        project_name=str(project["name"]),
        environment=str(project["environment"]),
        business_rules=BusinessRules(low_bikes, low_docks),
        sources=SourceSettings(
            gbfs_discovery_url=str(sources["gbfs_discovery_url"]),
            gbfs_language=str(sources["gbfs_language"]),
            open_meteo_url=str(sources["open_meteo_url"]),
            weather_latitude=float(sources["weather_latitude"]),
            weather_longitude=float(sources["weather_longitude"]),
            historical_object=str(sources["historical_object"]),
        ),
        azure=AzureResources(
            expected_databricks_host=expected_host,
            catalog=str(azure["catalog"]),
            schema=str(azure["schema"]),
            volume=str(azure["volume"]),
            event_hubs_namespace=str(azure["event_hubs_namespace"]),
            event_hub_name=str(azure["event_hub_name"]),
            consumer_group=str(azure["consumer_group"]),
            key_vault_name=str(azure["key_vault_name"]),
            event_hubs_secret_name=str(azure["event_hubs_secret_name"]),
        ),
        streaming=StreamingSettings(
            max_events_per_trigger=int(streaming["max_events_per_trigger"]),
            bounded_trigger=str(streaming["bounded_trigger"]),
            checkpoint_subpath=str(streaming["checkpoint_subpath"]),
            schema_subpath=str(streaming["schema_subpath"]),
            minimum_poll_interval_seconds=int(streaming["minimum_poll_interval_seconds"]),
        ),
    )

"""Small, explicit configuration model for UrbanFlow."""

from __future__ import annotations

import re
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
    max_publish_events: int
    bounded_trigger: str
    starting_offsets: str
    checkpoint_subpath: str
    schema_subpath: str
    report_subpath: str
    minimum_poll_interval_seconds: int


@dataclass(frozen=True)
class ExistingCluster:
    alias: str
    cluster_id: str
    expected_name: str
    expected_spark_version: str
    expected_data_security_mode: str
    verified_state: str
    verified_at: str
    unity_catalog_compatible: bool
    kafka_compatible: bool


@dataclass(frozen=True)
class ComputeSettings:
    preferred: str
    fallback: str
    required_state: str
    allow_start: bool
    allow_restart: bool
    allow_resize: bool
    allow_terminate: bool
    educational_cluster_creation_enabled: bool
    protected_from_termination: bool
    clusters: dict[str, ExistingCluster]

    @property
    def preferred_cluster(self) -> ExistingCluster:
        return self.clusters[self.preferred]

    @property
    def fallback_cluster(self) -> ExistingCluster:
        return self.clusters[self.fallback]

    @property
    def protected_cluster_ids(self) -> frozenset[str]:
        return frozenset(cluster.cluster_id for cluster in self.clusters.values())


@dataclass(frozen=True)
class LakeflowSettings:
    compute_mode: str
    continuous: bool
    shared_cluster_id: str | None


@dataclass(frozen=True)
class UrbanFlowConfig:
    project_name: str
    environment: str
    business_rules: BusinessRules
    sources: SourceSettings
    azure: AzureResources
    streaming: StreamingSettings
    compute: ComputeSettings
    lakeflow: LakeflowSettings


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
    compute = _require_mapping(raw, "compute")
    cluster_nodes = _require_mapping(compute, "clusters")
    lakeflow = _require_mapping(raw, "lakeflow")

    low_bikes = int(rules["low_bike_threshold"])
    low_docks = int(rules["low_dock_threshold"])
    if low_bikes < 0 or low_docks < 0:
        raise ValueError("Availability thresholds must be non-negative integers.")

    expected_host = normalize_host(str(azure["expected_databricks_host"]))
    if not expected_host.endswith(".azuredatabricks.net"):
        raise ValueError("The Azure target must use an azuredatabricks.net host.")

    clusters: dict[str, ExistingCluster] = {}
    for alias, value in cluster_nodes.items():
        if not isinstance(alias, str) or not isinstance(value, dict):
            raise ValueError("Every compute cluster must be a named mapping.")
        cluster_id = str(value["cluster_id"])
        if not re.fullmatch(r"\d{4}-\d{6}-[a-z0-9]+", cluster_id):
            raise ValueError(f"Cluster {alias!r} has an invalid Databricks cluster ID.")
        clusters[alias] = ExistingCluster(
            alias=alias,
            cluster_id=cluster_id,
            expected_name=str(value["expected_name"]),
            expected_spark_version=str(value["expected_spark_version"]),
            expected_data_security_mode=str(value["expected_data_security_mode"]),
            verified_state=str(value["verified_state"]).upper(),
            verified_at=str(value["verified_at"]),
            unity_catalog_compatible=bool(value["unity_catalog_compatible"]),
            kafka_compatible=bool(value["kafka_compatible"]),
        )

    preferred = str(compute["preferred"])
    fallback = str(compute["fallback"])
    if preferred not in clusters or fallback not in clusters or preferred == fallback:
        raise ValueError("Compute preferred and fallback aliases must name two distinct clusters.")

    mutation_flags = {
        key: bool(compute[key])
        for key in ("allow_start", "allow_restart", "allow_resize", "allow_terminate")
    }
    if any(mutation_flags.values()):
        raise ValueError("UrbanFlow shared-cluster mutation flags must all remain false.")
    if bool(compute["educational_cluster_creation_enabled"]):
        raise ValueError("Educational cluster creation must remain disabled by configuration.")
    if not bool(compute["protected_from_termination"]):
        raise ValueError("Shared-cluster termination protection must remain enabled.")

    required_state = str(compute["required_state"]).upper()
    if required_state != "RUNNING":
        raise ValueError("A shared cluster must already be RUNNING before UrbanFlow can use it.")

    shared_pipeline_cluster = lakeflow.get("shared_cluster_id")
    if shared_pipeline_cluster not in (None, ""):
        raise ValueError("Lakeflow must use managed compute, not GP1 or GP2.")
    lakeflow_mode = str(lakeflow["compute_mode"])
    if lakeflow_mode != "serverless":
        raise ValueError("UrbanFlow Lakeflow configuration must remain serverless.")

    starting_offsets = str(streaming["starting_offsets"])
    if starting_offsets not in {"earliest", "latest"}:
        raise ValueError("Streaming starting_offsets must be 'earliest' or 'latest'.")
    max_publish_events = int(streaming["max_publish_events"])
    max_events_per_trigger = int(streaming["max_events_per_trigger"])
    if max_publish_events < 1 or max_events_per_trigger < 1:
        raise ValueError("Streaming event limits must be positive integers.")

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
            max_events_per_trigger=max_events_per_trigger,
            max_publish_events=max_publish_events,
            bounded_trigger=str(streaming["bounded_trigger"]),
            starting_offsets=starting_offsets,
            checkpoint_subpath=str(streaming["checkpoint_subpath"]),
            schema_subpath=str(streaming["schema_subpath"]),
            report_subpath=str(streaming["report_subpath"]),
            minimum_poll_interval_seconds=int(streaming["minimum_poll_interval_seconds"]),
        ),
        compute=ComputeSettings(
            preferred=preferred,
            fallback=fallback,
            required_state=required_state,
            allow_start=mutation_flags["allow_start"],
            allow_restart=mutation_flags["allow_restart"],
            allow_resize=mutation_flags["allow_resize"],
            allow_terminate=mutation_flags["allow_terminate"],
            educational_cluster_creation_enabled=False,
            protected_from_termination=True,
            clusters=clusters,
        ),
        lakeflow=LakeflowSettings(
            compute_mode=lakeflow_mode,
            continuous=bool(lakeflow["continuous"]),
            shared_cluster_id=None,
        ),
    )

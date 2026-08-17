"""
Lab 05 — shared configuration helpers.

Environment-specific values are supplied by the Lakeflow pipeline / bundle.

Required pipeline configuration:
    lab05.catalog
    lab05.schema
    lab05.volume_name
    lab05.streaming_volume_name
    lab05.run_checks
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


CONF_CATALOG = "lab05.catalog"
CONF_SCHEMA = "lab05.schema"
CONF_VOLUME_NAME = "lab05.volume_name"
CONF_STREAMING_VOLUME_NAME = "lab05.streaming_volume_name"
CONF_RUN_CHECKS = "lab05.run_checks"


STATION_STATUS_BRONZE = "station_status_bronze"
STATION_INFORMATION_BRONZE = "station_information_bronze"

STATION_STATUS_SILVER = "station_status_silver"
STATION_INFORMATION_SILVER = "station_information_silver"
STATION_STATUS_ENRICHED_SILVER = "station_status_enriched_silver"

STATION_SUMMARY_GOLD = "station_summary_gold"


STATION_STATUS_LANDING_DIR = "landing/station_status"
STATION_INFORMATION_FILE = "reference/station_information.json"
TEST_DATA_DIR = "test_data"


@dataclass(frozen=True)
class Lab05Config:
    """Resolved runtime configuration for Lab 05."""

    catalog: str
    schema: str
    volume_name: str
    streaming_volume_name: str
    run_checks: bool

    @property
    def volume_path(self) -> str:
        """Managed volume used for reference and controlled test data."""
        return (
            f"/Volumes/{self.catalog}/"
            f"{self.schema}/{self.volume_name}"
        )

    @property
    def streaming_volume_path(self) -> str:
        """External volume used by Auto Loader."""
        return (
            f"/Volumes/{self.catalog}/"
            f"{self.schema}/{self.streaming_volume_name}"
        )

    @property
    def station_status_landing_path(self) -> str:
        """External-volume folder consumed by Auto Loader."""
        return (
            f"{self.streaming_volume_path}/"
            f"{STATION_STATUS_LANDING_DIR}"
        )

    @property
    def station_information_path(self) -> str:
        """Managed-volume batch/reference JSON path."""
        return (
            f"{self.volume_path}/"
            f"{STATION_INFORMATION_FILE}"
        )

    @property
    def test_data_path(self) -> str:
        """Managed-volume location for controlled test fixtures."""
        return f"{self.volume_path}/{TEST_DATA_DIR}"


def _get_required_conf(
    spark_session: Any,
    key: str,
) -> str:
    """Read a required Spark/Lakeflow configuration value."""
    try:
        value = spark_session.conf.get(key)
    except Exception as exc:
        raise ValueError(
            f"Missing required pipeline configuration: {key}"
        ) from exc

    value = str(value).strip()

    if not value:
        raise ValueError(
            f"Pipeline configuration is empty: {key}"
        )

    return value


def _parse_bool(value: str, key: str) -> bool:
    """Parse a boolean configuration value."""
    normalized = value.strip().lower()

    if normalized in {"true", "1", "yes", "y"}:
        return True

    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(
        f"Invalid boolean value for {key}: {value!r}. "
        "Use true/false."
    )


def load_config(spark_session: Any) -> Lab05Config:
    """Resolve Lab 05 runtime configuration from Spark config."""
    catalog = _get_required_conf(
        spark_session,
        CONF_CATALOG,
    )
    schema = _get_required_conf(
        spark_session,
        CONF_SCHEMA,
    )
    volume_name = _get_required_conf(
        spark_session,
        CONF_VOLUME_NAME,
    )
    streaming_volume_name = _get_required_conf(
        spark_session,
        CONF_STREAMING_VOLUME_NAME,
    )
    run_checks_raw = _get_required_conf(
        spark_session,
        CONF_RUN_CHECKS,
    )

    return Lab05Config(
        catalog=catalog,
        schema=schema,
        volume_name=volume_name,
        streaming_volume_name=streaming_volume_name,
        run_checks=_parse_bool(
            run_checks_raw,
            CONF_RUN_CHECKS,
        ),
    )

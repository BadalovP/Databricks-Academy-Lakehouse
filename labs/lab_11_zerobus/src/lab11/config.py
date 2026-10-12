"""Typed configuration: producer settings, transport retry policy, and Unity Catalog targets.

Credentials never live in configuration files. They are read from environment variables and
their repr hides the secret, so they cannot leak into logs or tracebacks.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = LAB_ROOT / "config" / "lab11.toml"

# A live run is deliberately small: a lab demonstration, not a load test.
MAX_LIVE_RECORDS = 30

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


class ConfigError(ValueError):
    """The configuration or environment is incomplete or unsafe."""


def identifier(value: str, what: str) -> str:
    """Return ``value`` if it is a plain SQL identifier (Zerobus also requires ASCII names)."""
    if not _IDENTIFIER.fullmatch(value or ""):
        raise ConfigError(f"Invalid {what}: {value!r} (ASCII letters, digits and underscores only)")
    return value


def run_identifier(value: str) -> str:
    if not _RUN_ID.fullmatch(value or ""):
        raise ConfigError(f"Invalid run id: {value!r} (letters, digits, '-' and '_' only)")
    return value


@dataclass(frozen=True)
class ProducerSettings:
    producer_id: str
    devices: int
    readings_per_device: int
    replay_count: int
    batch_size: int
    base_timestamp: datetime
    interval_seconds: int
    seed: int

    @property
    def unique_events(self) -> int:
        return self.devices * self.readings_per_device

    @property
    def total_deliveries(self) -> int:
        return self.unique_events + self.replay_count

    def validate(self) -> ProducerSettings:
        for name in ("devices", "readings_per_device", "batch_size", "interval_seconds"):
            if getattr(self, name) < 1:
                raise ConfigError(f"producer.{name} must be at least 1")
        if not 0 <= self.replay_count <= self.unique_events:
            raise ConfigError(
                "producer.replay_count must be between 0 and the number of unique events"
            )
        if self.total_deliveries > MAX_LIVE_RECORDS:
            raise ConfigError(
                f"{self.total_deliveries} deliveries exceed the {MAX_LIVE_RECORDS}-record lab limit"
            )
        return self


@dataclass(frozen=True)
class TransportSettings:
    max_attempts: int
    backoff_initial_seconds: float
    backoff_max_seconds: float
    flush_timeout_seconds: int
    server_ack_timeout_seconds: int

    def backoff(self, attempt: int) -> float:
        """Exponential backoff before retry number ``attempt`` (1-based), capped."""
        return min(self.backoff_initial_seconds * 2 ** (attempt - 1), self.backoff_max_seconds)

    def validate(self) -> TransportSettings:
        if not 1 <= self.max_attempts <= 5:
            raise ConfigError("transport.max_attempts must be between 1 and 5")
        if (
            self.backoff_initial_seconds <= 0
            or self.backoff_max_seconds < self.backoff_initial_seconds
        ):
            raise ConfigError(
                "transport backoff must be positive and capped above the initial delay"
            )
        return self


@dataclass(frozen=True)
class Target:
    name: str
    workspace_url: str
    workspace_id: int
    cloud: str
    region: str
    catalog: str
    schema: str
    bronze: str
    silver: str
    cli_profile: str | None = None
    auth_type: str | None = None
    description: str = ""

    @property
    def zerobus_endpoint(self) -> str:
        """Zerobus server endpoint: ``<workspace-id>.zerobus.<region>.<cloud domain>``."""
        domain = {"azure": "azuredatabricks.net", "aws": "cloud.databricks.com"}.get(self.cloud)
        if domain is None:
            raise ConfigError(f"Unsupported cloud {self.cloud!r}")
        return f"https://{self.workspace_id}.zerobus.{self.region}.{domain}"

    @property
    def schema_name(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def bronze_table(self) -> str:
        return f"{self.schema_name}.{self.bronze}"

    @property
    def silver_table(self) -> str:
        return f"{self.schema_name}.{self.silver}"


@dataclass(frozen=True)
class Credentials:
    """OAuth machine-to-machine credentials of the ingesting service principal."""

    client_id: str
    client_secret: str = field(repr=False)

    def __repr__(self) -> str:
        return f"Credentials(client_id={self.client_id[:8]}..., client_secret=***)"

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> Credentials:
        env = os.environ if environ is None else environ
        client_id = env.get("ZEROBUS_CLIENT_ID", "").strip()
        client_secret = env.get("ZEROBUS_CLIENT_SECRET", "").strip()
        missing = [
            name
            for name, value in (
                ("ZEROBUS_CLIENT_ID", client_id),
                ("ZEROBUS_CLIENT_SECRET", client_secret),
            )
            if not value
        ]
        if missing:
            raise ConfigError(
                "Missing service principal credentials: set "
                + " and ".join(missing)
                + ". Zerobus accepts only Databricks OAuth M2M credentials of a service principal "
                "with USE CATALOG, USE SCHEMA, SELECT and MODIFY on the Bronze table."
            )
        return cls(client_id, client_secret)


@dataclass(frozen=True)
class Lab11Config:
    producer: ProducerSettings
    transport: TransportSettings
    targets: dict[str, Target]

    def target(self, name: str) -> Target:
        try:
            return self.targets[name]
        except KeyError:
            raise ConfigError(
                f"Unknown target {name!r}; choose one of {sorted(self.targets)}"
            ) from None


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ConfigError("producer.base_timestamp must include a time zone")
    return parsed.astimezone(UTC)


def load_config(path: Path = DEFAULT_CONFIG) -> Lab11Config:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    p, t, tables = raw["producer"], raw["transport"], raw["tables"]
    producer = ProducerSettings(
        producer_id=p["producer_id"],
        devices=int(p["devices"]),
        readings_per_device=int(p["readings_per_device"]),
        replay_count=int(p["replay_count"]),
        batch_size=int(p["batch_size"]),
        base_timestamp=_timestamp(p["base_timestamp"]),
        interval_seconds=int(p["interval_seconds"]),
        seed=int(p["seed"]),
    ).validate()
    transport = TransportSettings(
        max_attempts=int(t["max_attempts"]),
        backoff_initial_seconds=float(t["backoff_initial_seconds"]),
        backoff_max_seconds=float(t["backoff_max_seconds"]),
        flush_timeout_seconds=int(t["flush_timeout_seconds"]),
        server_ack_timeout_seconds=int(t["server_ack_timeout_seconds"]),
    ).validate()
    targets = {
        name: Target(
            name=name,
            workspace_url=spec["workspace_url"].rstrip("/"),
            workspace_id=int(spec["workspace_id"]),
            cloud=spec["cloud"],
            region=spec["region"],
            catalog=identifier(spec["catalog"], "catalog"),
            schema=identifier(spec["schema"], "schema"),
            bronze=identifier(tables["bronze"], "bronze table"),
            silver=identifier(tables["silver"], "silver table"),
            cli_profile=spec.get("cli_profile"),
            auth_type=spec.get("auth_type"),
            description=spec.get("description", ""),
        )
        for name, spec in raw["targets"].items()
    }
    return Lab11Config(producer, transport, targets)

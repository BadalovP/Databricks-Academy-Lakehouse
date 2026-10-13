"""Typed configuration loading and validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ChunkingSettings:
    max_chars: int
    overlap_chars: int
    min_chunk_chars: int


@dataclass(frozen=True)
class RetrievalSettings:
    top_k: int
    score_threshold: float
    max_context_chars: int


@dataclass(frozen=True)
class ProjectConfig:
    raw: dict[str, Any]

    @property
    def host(self) -> str:
        return str(self.raw["workspace"]["host"]).rstrip("/")

    @property
    def catalog(self) -> str:
        return str(self.raw["workspace"]["catalog"])

    @property
    def schema(self) -> str:
        return str(self.raw["workspace"]["schema"])

    @property
    def chunking(self) -> ChunkingSettings:
        value = self.raw["chunking"]
        return ChunkingSettings(
            max_chars=int(value["max_chars"]),
            overlap_chars=int(value["overlap_chars"]),
            min_chunk_chars=int(value["min_chunk_chars"]),
        )

    @property
    def retrieval(self) -> RetrievalSettings:
        value = self.raw["retrieval"]
        return RetrievalSettings(
            top_k=int(value["top_k"]),
            score_threshold=float(value["score_threshold"]),
            max_context_chars=int(value["max_context_chars"]),
        )

    def table_name(self, logical_name: str) -> str:
        table = str(self.raw["tables"][logical_name])
        return f"{self.catalog}.{self.schema}.{table}"


def load_config(path: str | Path) -> ProjectConfig:
    config_path = Path(path)
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Configuration root must be a mapping.")
    cfg = ProjectConfig(raw=data)
    _validate(cfg)
    return cfg


def _validate(cfg: ProjectConfig) -> None:
    if not cfg.host.startswith("https://"):
        raise ValueError("workspace.host must use HTTPS.")
    for label, value in (("catalog", cfg.catalog), ("schema", cfg.schema)):
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid Unity Catalog {label}: {value!r}")
    chunking = cfg.chunking
    if chunking.max_chars <= 0 or chunking.min_chunk_chars <= 0:
        raise ValueError("Chunk sizes must be positive.")
    if not 0 <= chunking.overlap_chars < chunking.max_chars:
        raise ValueError("overlap_chars must be non-negative and smaller than max_chars.")
    retrieval = cfg.retrieval
    if retrieval.top_k <= 0 or retrieval.max_context_chars <= 0:
        raise ValueError("Retrieval limits must be positive.")

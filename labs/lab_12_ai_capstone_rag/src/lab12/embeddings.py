"""Embedding abstractions; AI Search normally computes managed embeddings."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Protocol


class EmbeddingProvider(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


@dataclass(frozen=True)
class ManagedEmbeddingSpec:
    endpoint_name: str = "databricks-gte-large-en"
    source_column: str = "content"
    dimension: int = 1024


class DeterministicHashEmbedding:
    """Small offline fixture embedding; never presented as model-quality semantics."""

    def __init__(self, dimensions: int = 64) -> None:
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self.dimensions = dimensions

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        values = [0.0] * self.dimensions
        for token in text.lower().split():
            digest = hashlib.sha256(token.encode()).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimensions
            values[index] += -1.0 if digest[4] % 2 else 1.0
        norm = math.sqrt(sum(value * value for value in values)) or 1.0
        return [value / norm for value in values]


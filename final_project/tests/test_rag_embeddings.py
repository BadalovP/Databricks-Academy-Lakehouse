from __future__ import annotations

import pytest

from urbanflow.rag.embeddings import DeterministicHashEmbedding, ManagedEmbeddingSpec


def test_managed_embedding_defaults() -> None:
    spec = ManagedEmbeddingSpec()
    assert spec.endpoint_name == "databricks-gte-large-en"
    assert spec.dimension == 1024


def test_hash_embedding_is_deterministic_and_normalized() -> None:
    provider = DeterministicHashEmbedding(16)
    first, second = provider.embed(["alpha beta", "alpha beta"])
    assert first == second
    assert pytest.approx(sum(value * value for value in first), rel=1e-6) == 1.0


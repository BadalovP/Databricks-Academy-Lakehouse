from __future__ import annotations

import pytest

from urbanflow.rag.vector_search import DeltaSyncIndexSpec, VectorSearchManager


class MissingClient:
    def __init__(self) -> None:
        self.created_endpoint = False
        self.created_index = False

    def get_index(self, **kwargs):
        raise LookupError(kwargs)

    def get_endpoint(self, name):
        raise LookupError(name)

    def create_endpoint(self, **kwargs):
        self.created_endpoint = True
        return kwargs

    def create_delta_sync_index(self, **kwargs):
        self.created_index = True
        return kwargs


def _spec() -> DeltaSyncIndexSpec:
    return DeltaSyncIndexSpec(
        endpoint_name="urbanflow-final-rag-standard",
        index_name="dbr_dev.urbanflow_final_dev.rag_document_chunks_index",
        source_table_name="dbr_dev.urbanflow_final_dev.rag_document_chunks",
    )


def test_spec_is_triggered_standard_managed_embedding() -> None:
    kwargs = _spec().as_create_kwargs()
    assert kwargs["pipeline_type"] == "TRIGGERED"
    assert kwargs["embedding_model_endpoint_name"] == "databricks-gte-large-en"
    assert "content" in kwargs["columns_to_sync"]


def test_ensure_refuses_creation_by_default() -> None:
    client = MissingClient()
    with pytest.raises(PermissionError):
        VectorSearchManager(client).ensure(_spec())
    assert not client.created_endpoint
    assert not client.created_index


def test_ensure_creates_only_when_allowed() -> None:
    client = MissingClient()
    result = VectorSearchManager(client).ensure(_spec(), allow_create=True)
    assert client.created_endpoint and client.created_index
    assert result["pipeline_type"] == "TRIGGERED"


def test_sync_refuses_by_default() -> None:
    with pytest.raises(PermissionError):
        VectorSearchManager(MissingClient()).sync(_spec())

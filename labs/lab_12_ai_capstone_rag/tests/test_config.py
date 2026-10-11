from __future__ import annotations

from lab12.config import load_config


def test_default_config(project_root) -> None:
    config = load_config(project_root / "config" / "default.yml")
    # Local default only; deployed Jobs take host, catalog and schema from the bundle target.
    assert config.host == "https://dbc-1750318a-76a9.cloud.databricks.com"
    assert config.table_name("chunks") == "dbr_dev.parvinbadalov_lab12_rag_dev.document_chunks"
    assert config.raw["retrieval"]["query_type"] in ("ANN", "HYBRID")
    assert config.chunking.overlap_chars < config.chunking.max_chars


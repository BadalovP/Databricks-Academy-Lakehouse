"""Online AI Search and deterministic offline retrieval adapters."""

from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any, Protocol

from .models import Chunk, SearchResult

_TOKEN = re.compile(r"[a-z0-9][a-z0-9_-]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "be",
    "for",
    "give",
    "how",
    "is",
    "me",
    "of",
    "or",
    "should",
    "the",
    "to",
    "what",
    "when",
    "which",
    "with",
}


class Retriever(Protocol):
    def search(
        self, question: str, *, top_k: int, filters: dict[str, Any] | None = None
    ) -> list[SearchResult]: ...


class OfflineLexicalRetriever:
    """Deterministic token-overlap retriever for tests and local demonstrations."""

    def __init__(self, chunks: list[Chunk], score_threshold: float = 0.05) -> None:
        self._chunks = chunks
        self._threshold = score_threshold

    def search(
        self, question: str, *, top_k: int, filters: dict[str, Any] | None = None
    ) -> list[SearchResult]:
        query_tokens = _tokens(question)
        if not query_tokens or top_k <= 0:
            return []
        results: list[SearchResult] = []
        for chunk in self._chunks:
            row = asdict(chunk)
            if filters and any(str(row.get(key)) != str(value) for key, value in filters.items()):
                continue
            chunk_tokens = _tokens(chunk.content)
            score = len(query_tokens & chunk_tokens) / len(query_tokens)
            # A filtered search returns the best matches inside the filter even at a low score,
            # as AI Search does (the live retriever passes no score threshold); the threshold only
            # trims unfiltered fixture searches.
            if score >= self._threshold or filters:
                results.append(
                    SearchResult(
                        chunk_id=chunk.chunk_id,
                        document_id=chunk.document_id,
                        source_name=chunk.source_name,
                        section_heading=chunk.section_heading,
                        section_ref=chunk.section_ref,
                        content=chunk.content,
                        score=score,
                        metadata=chunk.metadata,
                    )
                )
        return sorted(results, key=lambda value: (-value.score, value.chunk_id))[:top_k]


class DatabricksAISearchRetriever:
    """Adapter over `databricks-ai-search` similarity_search results."""

    COLUMNS = [
        "chunk_id",
        "document_id",
        "source_name",
        "section_heading",
        "section_ref",
        "content",
    ]

    QUERY_TYPES = ("ANN", "HYBRID")

    def __init__(
        self,
        client: Any,
        endpoint_name: str,
        index_name: str,
        *,
        query_type: str = "HYBRID",
        total_retries: int = 0,
    ) -> None:
        if query_type not in self.QUERY_TYPES:
            raise ValueError(f"query_type must be one of {self.QUERY_TYPES}, got {query_type!r}.")
        self._client = client
        self._endpoint_name = endpoint_name
        self._index_name = index_name
        self._query_type = query_type
        # The SDK retries a query 3 times by default; 0 makes every failure visible once.
        self._total_retries = total_retries
        self._index: Any = None

    def search(
        self, question: str, *, top_k: int, filters: dict[str, Any] | None = None
    ) -> list[SearchResult]:
        # Resolve the index once; get_index makes a control-plane call each time.
        if self._index is None:
            self._index = self._client.get_index(
                endpoint_name=self._endpoint_name, index_name=self._index_name
            )
        # HYBRID combines vector similarity with BM25 keywords, which suits procedures full of
        # exact terms such as "DELIVER BIKES" or "quarantine" (Databricks recommends it for RAG).
        response = self._index.similarity_search(
            query_text=question,
            columns=self.COLUMNS,
            num_results=top_k,
            filters=filters or {},
            query_type=self._query_type,
            total_retries=self._total_retries,
        )
        return self._parse(response)

    @classmethod
    def _parse(cls, response: Any) -> list[SearchResult]:
        payload = response if isinstance(response, dict) else response.as_dict()
        manifest_columns = payload.get("manifest", {}).get("columns", [])
        names = [column.get("name") for column in manifest_columns]
        rows = payload.get("result", {}).get("data_array", [])
        parsed: list[SearchResult] = []
        for values in rows:
            row = dict(zip(names, values, strict=False))
            # AI Search appends the relevance score as the final "score" column. The previous
            # nested pop evaluated its default eagerly and dropped "similarity_score" even when
            # "score" was present; resolve the name explicitly instead.
            if "score" in row:
                raw_score = row.pop("score")
            elif "similarity_score" in row:
                raw_score = row.pop("similarity_score")
            else:
                raw_score = values[-1] if values else 0.0
            score = float(raw_score or 0.0)
            parsed.append(
                SearchResult(
                    chunk_id=str(row["chunk_id"]),
                    document_id=str(row["document_id"]),
                    source_name=str(row["source_name"]),
                    section_heading=str(row["section_heading"]),
                    section_ref=str(row["section_ref"]),
                    content=str(row["content"]),
                    score=score,
                    metadata={key: value for key, value in row.items() if key not in cls.COLUMNS},
                )
            )
        return parsed


def _tokens(text: str) -> set[str]:
    return set(_TOKEN.findall(text.lower())) - _STOPWORDS

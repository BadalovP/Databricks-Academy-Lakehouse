from __future__ import annotations

from urbanflow.rag.models import Chunk
from urbanflow.rag.retrieval import DatabricksAISearchRetriever, OfflineLexicalRetriever


def _chunk(chunk_id: str, text: str, source: str = "guide.md") -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        document_id="doc",
        source_name=source,
        section_heading="Recovery",
        section_ref=f"{source}#recovery",
        ordinal=0,
        content=text,
        content_hash=chunk_id,
    )


def test_offline_retrieval_top_k_and_filter() -> None:
    retriever = OfflineLexicalRetriever(
        [_chunk("a", "streaming checkpoint recovery"), _chunk("b", "incident recovery", "incident.md")]
    )
    result = retriever.search("streaming recovery", top_k=1)
    assert [item.chunk_id for item in result] == ["a"]
    filtered = retriever.search("recovery", top_k=5, filters={"source_name": "incident.md"})
    assert [item.chunk_id for item in filtered] == ["b"]


def test_offline_retrieval_zero_results() -> None:
    retriever = OfflineLexicalRetriever([_chunk("a", "checkpoint")])
    assert retriever.search("unrelated telephone", top_k=3) == []


def test_ai_search_response_parsing() -> None:
    response = {
        "manifest": {
            "columns": [
                {"name": "chunk_id"},
                {"name": "document_id"},
                {"name": "source_name"},
                {"name": "section_heading"},
                {"name": "section_ref"},
                {"name": "content"},
                {"name": "score"},
            ]
        },
        "result": {"data_array": [["c1", "d1", "g.md", "H", "g.md#h", "text", 0.9]]},
    }
    parsed = DatabricksAISearchRetriever._parse(response)
    assert parsed[0].chunk_id == "c1"
    assert parsed[0].score == 0.9


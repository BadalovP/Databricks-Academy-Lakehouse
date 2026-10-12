from __future__ import annotations

from dataclasses import asdict

from urbanflow.rag.models import RagAnswer, SearchResult
from urbanflow.rag.monitoring import event_from_answer


def test_monitoring_event_excludes_raw_text() -> None:
    source = SearchResult("c1", "d1", "g.md", "H", "g.md#h", "sensitive context", 0.9)
    answer = RagAnswer("my raw question", "raw answer", (source,), "model", 12)
    payload = asdict(event_from_answer(answer, "index"))
    serialized = str(payload)
    assert "my raw question" not in serialized
    assert "raw answer" not in serialized
    assert "sensitive context" not in serialized
    assert payload["retrieved_chunk_ids"] == ("c1",)

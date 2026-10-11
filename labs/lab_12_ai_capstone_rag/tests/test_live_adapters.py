"""Behaviour of the live adapters against fakes shaped like the real SDK responses.

These prove the adapters handle the documented response shapes. They do not prove that a real
endpoint, index or model behaves this way - that needs an approved live run.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lab12.models import SearchResult
from lab12.rag import (
    INSUFFICIENT,
    DatabricksChatModel,
    NoRetrievalBaseline,
    message_text,
)
from lab12.retrieval import DatabricksAISearchRetriever

COLUMNS = ["chunk_id", "document_id", "source_name", "section_heading", "section_ref", "content"]


def _response(score_column: str = "score") -> dict:
    return {
        "manifest": {"columns": [{"name": name} for name in [*COLUMNS, score_column]]},
        "result": {"data_array": [["c1", "d1", "g.md", "H", "g.md#h", "text", 0.42]]},
    }


class FakeIndex:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def similarity_search(self, **kwargs):
        self.calls.append(kwargs)
        return _response()


class FakeSearchClient:
    def __init__(self) -> None:
        self.index = FakeIndex()
        self.get_index_calls = 0

    def get_index(self, *, endpoint_name, index_name):
        self.get_index_calls += 1
        return self.index


def test_retriever_defaults_to_hybrid_and_resolves_the_index_once() -> None:
    client = FakeSearchClient()
    retriever = DatabricksAISearchRetriever(client, "ep", "c.s.idx")
    retriever.search("q1", top_k=3)
    retriever.search("q2", top_k=3)
    assert client.get_index_calls == 1
    assert {call["query_type"] for call in client.index.calls} == {"HYBRID"}
    assert client.index.calls[0]["num_results"] == 3


def test_retriever_rejects_unknown_query_type() -> None:
    with pytest.raises(ValueError):
        DatabricksAISearchRetriever(FakeSearchClient(), "ep", "c.s.idx", query_type="FUZZY")


@pytest.mark.parametrize("column", ["score", "similarity_score"])
def test_score_is_read_from_either_column_name(column: str) -> None:
    parsed = DatabricksAISearchRetriever._parse(_response(column))
    assert parsed[0].score == pytest.approx(0.42)
    assert column not in parsed[0].metadata


def test_message_text_keeps_only_text_blocks_from_reasoning_models() -> None:
    content = [
        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "private chain"}]},
        {"type": "text", "text": "Use an isolated checkpoint [1]."},
    ]
    assert message_text(content) == "Use an isolated checkpoint [1]."
    assert "private chain" not in message_text(content)


def test_message_text_handles_plain_strings_objects_and_none() -> None:
    assert message_text("  answer  ") == "answer"
    assert message_text(None) == ""
    assert message_text([SimpleNamespace(type="text", text="from object")]) == "from object"


class FakeChatClient:
    def __init__(self, content) -> None:
        self.kwargs: dict = {}
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))
        self._content = content

    def _create(self, **kwargs):
        self.kwargs = kwargs
        message = SimpleNamespace(content=self._content)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


def test_chat_model_passes_configured_generation_settings() -> None:
    client = FakeChatClient([{"type": "text", "text": "ok"}])
    model = DatabricksChatModel("databricks-gpt-oss-20b", client, temperature=0.2, max_tokens=123)
    assert model.complete(system_prompt="s", user_prompt="u") == "ok"
    assert client.kwargs["model"] == "databricks-gpt-oss-20b"
    assert client.kwargs["temperature"] == 0.2
    assert client.kwargs["max_tokens"] == 123


class EchoModel:
    model_name = "fixture-chat"

    def __init__(self) -> None:
        self.prompts: list[str] = []

    def complete(self, *, system_prompt, user_prompt):
        self.prompts.append(user_prompt)
        return "General guidance without citations."


def test_no_retrieval_baseline_calls_the_model_without_context() -> None:
    model = EchoModel()
    answer = NoRetrievalBaseline(model).answer("How do I recover Orion?")
    assert answer.sources == ()
    assert "CONTEXT" not in model.prompts[0]
    assert answer.answer == "General guidance without citations."
    assert answer.model == "fixture-chat (no retrieval)"


def test_no_retrieval_baseline_sanitizes_errors() -> None:
    class Broken(EchoModel):
        def complete(self, *, system_prompt, user_prompt):
            raise TimeoutError("internal host detail")

    answer = NoRetrievalBaseline(Broken()).answer("q")
    assert answer.error == "TimeoutError"
    assert "internal host detail" not in answer.answer
    assert answer.answer != INSUFFICIENT


def test_search_result_model_is_unchanged() -> None:
    # Guards the column contract the retriever and the index share.
    assert set(SearchResult.__dataclass_fields__) >= set(COLUMNS) | {"score"}
